from __future__ import annotations

import asyncio
import datetime
import sys
import uuid
from uuid import UUID

import msgspec

from tomasik.src.entities.user import User, userPrint
from tomasik.src.server.server import prefixed_send, prefixed_recv, Get, Put, Del, ListKeys, ListValues, \
    RecentUsers, ListUserForGroup


class Client:
    """An example TCP key-value client using asyncio and msgspec."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def create(cls, host: str = "127.0.0.1", port: int = 8888):
        """Create a new client"""
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def close(self) -> None:
        """Close the client."""
        self.writer.close()
        await self.writer.wait_closed()

    async def request(self, req):
        """Send a request and await the response"""
        # Encode and send the request
        buffer = msgspec.msgpack.encode(req)
        await prefixed_send(self.writer, buffer)

        # Receive and decode the response
        buffer = await prefixed_recv(self.reader)
        return msgspec.msgpack.decode(buffer)

    async def get(self, key: UUID) -> User | None:
        """Get a key from the KV store, returning None if not present"""
        userStr = await self.request(Get(key))
        return msgspec.json.decode(userStr, type=User)

    async def put(self, val: User) -> None:
        """Put a key-val pair in the KV store"""
        return await self.request(Put(val))

    async def delete(self, key: UUID) -> None:
        """Delete a key-val pair from the KV store"""
        return await self.request(Del(key))

    async def list_keys(self) -> list[UUID]:
        """List all keys in the KV store"""
        keys = []
        for userID in await self.request(ListKeys()):
            keys += [uuid.UUID(userID)]
        return keys

    async def list_values(self) -> list[User]:
        """List all keys in the KV store"""
        users = []
        for userJson in await self.request(ListValues()):
            users += [msgspec.json.decode(userJson, type=User)]
        return users

    async def list_recently_users(self, date: datetime.datetime) -> list[User]:
        users = []
        for userJson in await self.request(RecentUsers(date)):
            users += [msgspec.json.decode(userJson, type=User)]
        return users

    async def list_group(self, group: str) -> list[User]:
        users = []
        for userJson in await self.request(ListUserForGroup(group)):
            users += [msgspec.json.decode(userJson, type=User)]
        return users


async def main():
    client = await Client.create()
    uuidHaris = uuid.uuid4()
    uuidKobi = uuid.uuid4()

    await client.put(User(uuidHaris, "Haris", datetime.datetime.now(), {"Mathematik"}))
    await client.put(User(uuid.uuid4(), "Steven", datetime.datetime.now(), {"Admin"}, "nachos@gmail.com"))
    await client.put(User(uuid.uuid4(), "Max", datetime.datetime.now(), {"Informatik", "Mathematik"}))
    d = datetime.datetime.now()
    await client.put(User(uuid.uuid4(), "Hogwath", datetime.datetime.now(), {"Informatik", "Mathematik"}))
    await client.put(User(uuidKobi, "Kobi", datetime.datetime.now()))
    await client.put(User(uuid.uuid4(), "VSOP", datetime.datetime.now(), {"Mathematik"}))

    print("-" * 5 + "Create User" + "-" * 5)
    list = await client.list_values()
    for user in list:
        userPrint(user)

    print("-" * 5 + "Checking for Keys" + "-" * 5)
    list = await client.list_keys()
    for id in list:
        print(str(id))
    if not list.__contains__(uuidHaris):
        print("ERROR ID")
        print(str(list[0]) + " " + str(uuidHaris))
        sys.exit()

    print("-" * 5 + "Update User" + "-" * 5)
    newUser = User(uuidHaris, "Haris K", datetime.datetime.now(), {"Mathematik"})
    await client.put(newUser)
    list = await client.list_values()
    for user in list:
        userPrint(user)
    if not list.__contains__(newUser):
        print("ERROR PUT")
        sys.exit()

    print("-" * 5 + "Delete User" + "-" * 5)
    await client.delete(uuidKobi)
    list = await client.list_values()
    for user in list:
        userPrint(user)
    if len(list) == 6:
        print("ERROR DELETE")
        sys.exit()

    print("-" * 5 + "Get all users with Mathematik" + "-" * 5)
    list = await client.list_group("Mathematik")
    for user in list:
        userPrint(user)
    if len(list) != 4:
        print("ERROR LIST GROUP")
        sys.exit()

    print("-" * 5 + "All recent users" + "-" * 5)
    list = await client.list_recently_users(d)
    for user in list:
        userPrint(user)
    if len(list) != 2:
        print("ERROR RECENTLY USERS")
        sys.exit()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
