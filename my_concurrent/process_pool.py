from multiprocessing import JoinableQueue
from typing import Callable, TypeVar, Generic

from my_concurrent.queue_controlled_process import QueueControlledProcess


TData = TypeVar('TData')
TResult = TypeVar('TResult')


class ProcessPool(Generic[TData, TResult]):
    def __init__(
            self,
            processes: list[QueueControlledProcess[TData, TResult]]
    ):
        self._processes = processes

    @classmethod
    def make_pool(
            cls,
            handler: Callable[[TData], TResult],
            size: int,
            input_queue: JoinableQueue,
            output_queue: JoinableQueue
    ) -> 'ProcessPool':
        return cls([
            QueueControlledProcess[TData, TResult](
                handler,
                input_queue,
                output_queue
            )
            for _ in range(size)
        ])

    def start_all(self):
        for proc in self._processes:
            proc.start()

    def stop_all(self):
        for proc in self._processes:
            proc.stop_queue()
