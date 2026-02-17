from pydantic import BaseModel
from datetime import datetime
from typing import Literal

class Room(BaseModel):
    id: int
    name: str

class Student(BaseModel):
    id: int
    name: str
    birthday: datetime
    sex: Literal['M', 'F']
    room: int