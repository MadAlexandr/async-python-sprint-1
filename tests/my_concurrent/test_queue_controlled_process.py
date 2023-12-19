from multiprocessing import JoinableQueue
from time import sleep
from typing import Callable

from common_types.task_types import TaskState, Status
from my_concurrent.queue_controlled_process import QueueControlledProcess


def simple_converter(data: str) -> int:
    return int(data)


def sleepy_converter(data: str) -> int:
    sleep(0.1)
    return int(data)


def setup(
        handler: Callable
) -> tuple[JoinableQueue, JoinableQueue, QueueControlledProcess]:
    in_q: JoinableQueue = JoinableQueue()
    out_q: JoinableQueue = JoinableQueue()
    qcp = QueueControlledProcess[str, int](
        handler,
        in_q,
        out_q
    )
    qcp.daemon = True
    qcp.start()

    return in_q, out_q, qcp


def stop_process(qcp: QueueControlledProcess):
    qcp.stop_queue()
    while qcp.is_alive():
        sleep(0.001)


def test_handle_raw_data():
    [in_q, out_q, qcp] = setup(simple_converter)

    in_q.put('123')
    in_q.join()

    result = out_q.get()
    assert isinstance(result, TaskState)
    assert result.status == Status.OK
    assert result.message is None
    assert result.data == 123

    stop_process(qcp)


def test_handle_task_state():
    [in_q, out_q, qcp] = setup(simple_converter)

    prev_task = TaskState(
        status=Status.OK,
        message=None,
        data='123'
    )
    in_q.put(prev_task)
    in_q.join()

    result = out_q.get()
    assert isinstance(result, TaskState)
    assert result.data == 123

    stop_process(qcp)


def test_handle_error_task_state():
    [in_q, out_q, qcp] = setup(simple_converter)

    prev_task = TaskState(
        status=Status.ERROR,
        message='Something go wrong',
        data=None
    )
    in_q.put(prev_task)
    in_q.join()

    result = out_q.get()
    assert isinstance(result, TaskState)
    assert result == prev_task

    stop_process(qcp)


def test_chained():
    [in_q, out_q, qcp] = setup(sleepy_converter)

    in_q.put('123')
    in_q.join()

    sleep(0.01)
    result = out_q.get_nowait()
    assert isinstance(result, TaskState)
    assert result.data == 123

    stop_process(qcp)
