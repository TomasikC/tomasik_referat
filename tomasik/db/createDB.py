import asyncio

import aiosqlite


async def createDB():
    db = await aiosqlite.connect(r"db.sqlite")
    sql_create_projects_table = """CREATE TABLE IF NOT EXISTS User(
                                        id text PRIMARY KEY,
                                        json text NOT NULL
                                    );"""
    cursor = await db.execute(sql_create_projects_table)
    await cursor.close()
    await db.close()


f = open("db.sqlite", "x")
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(createDB())


