from concurrent.futures import ThreadPoolExecutor
from multiprocessing import JoinableQueue
from typing import TypeVar, Generic, Iterable, Protocol

from common_types.task_types import TaskState


TSource = TypeVar('TSource')
TData = TypeVar('TData', covariant=True)


class DataFetcher(Protocol[TSource, TData]):
    def get_sources(self) -> Iterable[TSource]:
        ...

    def fetch_source(self, source: TSource) -> TData:
        ...


class ThreadFetcher(Generic[TSource, TData]):
    def __init__(
            self,
            data_fetcher: DataFetcher[TSource, TData],
            fetchers_count: int,
            output_queue: JoinableQueue
    ):
        self.data_fetcher = data_fetcher
        self._fetchers_count = fetchers_count
        self._out_q = output_queue

    def fetch_data(self):
        with ThreadPoolExecutor(max_workers=self._fetchers_count) as pool:
            pool.map(
                self._handle_one_source,
                self.data_fetcher.get_sources()
            )

    def _handle_one_source(self, data_source: TSource):
        try:
            result = self.data_fetcher.fetch_source(data_source)
        except Exception as e:
            self._out_q.put(TaskState[TData].error(str(e)))
        else:
            self._out_q.put(TaskState[TData].ok(result))
