import datetime
from typing import Annotated, Optional
from uuid import UUID

import msgspec

userString = Annotated[
    str, msgspec.Meta(min_length=1, pattern="^[a-zA-Z]+$")
]

mail = Annotated[
    str, msgspec.Meta(pattern="^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+.[a-zA-Z]{2,}$")
]


class User(msgspec.Struct):
    uuid: UUID
    name: userString
    last_access: datetime.datetime
    groups: set[userString] = set()
    email: Optional[mail] = None


def userPrint(user: User):
    print(str(user.uuid) + ": " + user.name)
