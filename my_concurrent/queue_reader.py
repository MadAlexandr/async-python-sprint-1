import random
from multiprocessing import JoinableQueue
from typing import Iterable, TypeVar, Generic

from common_types.task_types import TaskState


TData = TypeVar('TData')


class QueueTaskReader(Generic[TData]):
    def __init__(self, input_queue: JoinableQueue):
        self._in_q = input_queue
        self._STOP_SENTINEL = b'STOP_SENTINEL' + random.randbytes(8)

    def read_tasks(self) -> Iterable[TaskState[TData]]:
        self._push_queue()
        while True:
            task = self._in_q.get()
            self._in_q.task_done()
            if task == self._STOP_SENTINEL:
                break

            yield task

    def _push_queue(self):
        self._in_q.put(self._STOP_SENTINEL)
