from enum import Enum
from typing import NamedTuple, Optional, TypeVar, Generic


TaskData = TypeVar('TaskData')


class Status(Enum):
    OK = 'ok'
    ERROR = 'error'


class TaskState(NamedTuple, Generic[TaskData]):
    status: Status
    message: Optional[str]
    data: Optional[TaskData]

    @classmethod
    def error(cls, message: str) -> 'TaskState':
        return cls(Status.ERROR, message, None)

    @classmethod
    def ok(cls, data: TaskData) -> 'TaskState':
        return cls(Status.OK, None, data)
