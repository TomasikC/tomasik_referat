import datetime
from uuid import UUID

import msgspec


class User(msgspec.Struct):
    uuid: UUID
    name: str
    last_access: datetime.datetime
    groups: set[str] = set()
    email: str | None = None


def userPrint(user: User):
    print(str(user.uuid) + ": " + user.name)
