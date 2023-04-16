import uuid
from datetime import datetime
from uuid import UUID

import aiosqlite
import msgspec.json

from tomasik.src.entities.user import User


class Database:

    def __init__(self):
        self._db = None
        self.id = 0

    async def connect(self):
        self._db = await aiosqlite.connect(r"../../db/db.sqlite")

    # CRUD
    async def createUser(self, user: User):
        if (await self.getUser(user.uuid)) is None:
            insertStatement = """INSERT INTO User(id, data)
                                 VALUES (?, ?)"""
            await self._db.execute(insertStatement, (str(user.uuid), msgspec.json.encode(user)))
            return user
        else:
            updateStatement = """UPDATE User
                                 SET data = ?
                                 WHERE id = ?"""
            await self._db.execute(updateStatement, (msgspec.json.encode(user), str(user.uuid)))
            return user


    async def getUser(self, id: UUID):
        userBytes = None
        selectStatement = """SELECT data FROM User WHERE id = ?"""
        async with self._db.execute(selectStatement, (str(id),)) as cursor:
            async for row in cursor:
                userBytes = row[0]

        await cursor.close()
        return userBytes

    async def deleteUser(self, id: UUID):
        deleteStatement = """DELETE FROM User 
                             WHERE id = ?"""
        await self._db.execute(deleteStatement, (str(id),))

    async def getKeys(self):
        keys = []
        selectStatement = """SELECT id FROM User"""
        async with self._db.execute(selectStatement) as cursor:
            async for row in cursor:
                keys += [uuid.UUID(row[0])]

        await cursor.close()
        return keys

    async def getValues(self):
        values = []
        selectStatement = """SELECT data FROM User"""
        async with self._db.execute(selectStatement) as cursor:
            async for row in cursor:
                values += [row[0]]

        await cursor.close()
        return values

    async def getRecentUsers(self, date: datetime):
        values = []

        selectStatement = """SELECT data FROM User WHERE datetime(json_extract(data, '$.last_access')) < ?"""
        async with self._db.execute(selectStatement, (date, )) as cursor:
            async for row in cursor:
                values += [row[0]]

        await cursor.close()
        return values

    async def getUsersForGroup(self, group):
        values = []
        selectStatement = """SELECT data FROM User 
                             WHERE EXISTS(SELECT 1 FROM json_each(json_extract(data, '$.groups')) WHERE value = ?)"""
        async with self._db.execute(selectStatement, (group, )) as cursor:
            async for row in cursor:
                values += [row[0]]

        await cursor.close()
        return values

    async def printOutDB(self):
        async with self._db.execute("SELECT * FROM User") as cursor:
            async for row in cursor:
                print(row)
        await cursor.close()

    async def close(self):
        await self._db.close()




