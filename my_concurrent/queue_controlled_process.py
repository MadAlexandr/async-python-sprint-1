from multiprocessing import Process, JoinableQueue
from typing import Callable, TypeVar, Generic

from common_types.task_types import TaskState, Status


TData = TypeVar('TData')
TResult = TypeVar('TResult')


class QueueControlledProcess(Generic[TData, TResult], Process):
    def __init__(
            self,
            handler: Callable[[TData], TResult],
            input_queue: JoinableQueue,
            output_queue: JoinableQueue
    ):
        super().__init__()
        self._handler = handler
        self._in_q = input_queue
        self._out_q = output_queue
        self._STOP_SENTINEL = 'STOP_SENTINEL'

    def run(self):
        while True:
            task = self._in_q.get()

            if task == self._STOP_SENTINEL:
                self._in_q.task_done()
                break

            if isinstance(task, TaskState):
                if task.status == Status.ERROR:
                    self._out_q.put(task)
                    self._in_q.task_done()
                    continue

                self._handle_task(task.data)
            else:
                self._handle_task(task)

    def _handle_task(self, data: TData):
        try:
            result = self._handler(data)
        except Exception as e:
            self._out_q.put(TaskState[TResult].error(str(e)))
        else:
            self._out_q.put(TaskState[TResult].ok(result))
        finally:
            self._in_q.task_done()

    def stop_queue(self):
        self._in_q.put(self._STOP_SENTINEL)
