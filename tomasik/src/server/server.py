from __future__ import annotations

import asyncio
import datetime
import sys
from uuid import UUID

import msgspec
from typing import Any

from tomasik.src.entities.user import User
from tomasik.src.server.database import Database


# Some utilities for writing and reading length-prefix framed messages. Using
# length-prefixed framing makes it easier for the reader to determine the
# boundaries of each message before passing it to msgspec to be decoded.
async def prefixed_send(stream: asyncio.StreamWriter, buffer: bytes) -> None:
    """Write a length-prefixed buffer to the stream"""
    # Encode the message length as a 4 byte big-endian integer.
    prefix = len(buffer).to_bytes(4, "big")

    # Write the prefix and buffer to the stream.
    stream.write(prefix)
    stream.write(buffer)
    await stream.drain()


async def prefixed_recv(stream: asyncio.StreamReader) -> bytes:
    """Read a length-prefixed buffer from the stream"""
    # Read the next 4 byte prefix
    prefix = await stream.readexactly(4)

    # Convert the prefix back into an integer for the next message length
    n = int.from_bytes(prefix, "big")

    # Read in the full message buffer
    return await stream.readexactly(n)


# Define some request types. We set `tag=True` on each type so they can be used
# in a "tagged-union" defining the request types.
class Get(msgspec.Struct, tag=True):
    key: UUID


class Put(msgspec.Struct, tag=True):
    val: User


class Del(msgspec.Struct, tag=True):
    key: UUID


class ListKeys(msgspec.Struct, tag=True):
    keys: set[UUID] = set()

class ListValues(msgspec.Struct, tag=True):
    user: set[User] = set()

class ListUserForGroup(msgspec.Struct, tag=True):
    group: str

class RecentUsers(msgspec.Struct, tag=True):
    date: datetime.datetime


# A union of all valid request types
Request = Get | Put | Del | ListKeys | ListValues | RecentUsers | ListUserForGroup


class Server:
    """An example TCP key-value server using asyncio and msgspec"""

    def __init__(self, host: str = "127.0.0.1", port: int = 8888):
        self.host = host
        self.port = port
        self.__db = Database()
        # A msgpack encoder for encoding responses
        self.encoder = msgspec.msgpack.Encoder()
        # A *typed* msgpack decoder for decoding requests. If a request doesn't
        # match the specified types, a nice error will be raised.
        self.decoder = msgspec.msgpack.Decoder(Request)

    async def handle_connection(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handle the full lifetime of a single connection"""
        print("Connection opened")
        while True:
            try:
                # Receive and decode a request
                buffer = await prefixed_recv(reader)
                req = self.decoder.decode(buffer)

                # Process the request
                resp = await self.handle_request(req)

                # Encode and write the response
                buffer = msgspec.msgpack.encode(resp)
                await prefixed_send(writer, buffer)
            except EOFError:
                print("Connection closed")
                return

    async def handle_request(self, req: Request) -> Any:
        """Handle a single request and return the result (if any)"""
        # We use pattern matching here to branch on the different message types.
        # You could just as well use an if-else statement, but pattern matching
        # works pretty well here.
        match req:
            case Get(key):
                return await self.__db.getUser(key)
            case Put(val):
                return await self.__db.createUser(val)
            case Del(key):
                await self.__db.deleteUser(key)
                return None
            case ListKeys():
                return await self.__db.getKeys()
            case ListValues():
                return await self.__db.getValues()
            case ListUserForGroup(group):
                return await self.__db.getUsersForGroup(group)
            case RecentUsers(date):
                return await self.__db.getRecentUsers(date)

    async def serve_forever(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        await self.__db.connect()
        print(f"Serving on tcp://{self.host}:{self.port}...")
        async with server:
            await server.serve_forever()

    def run(self) -> None:
        """Run the server until ctrl-C"""
        asyncio.run(self.serve_forever())


if __name__ == "__main__":
    try:
        Server().run()
    except:
        print("Server closed")
        sys.exit()
