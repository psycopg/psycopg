import logging
from time import time
from functools import partial
from contextlib import asynccontextmanager

import pytest

from ..acompat import spawn, gather, asleep

try:
    from psycopg_pool.sched_async import AsyncScheduler
except ImportError:
    # Tests should have been skipped if the package is not available
    pass

pytestmark = [pytest.mark.timing]
if True:  # ASYNC:
    pytestmark.append(pytest.mark.anyio)


@pytest.mark.slow
async def test_sched():
    s = AsyncScheduler()
    results = []

    async def worker(i):
        results.append((i, time()))

    t0 = time()
    await s.enter(0.1, partial(worker, 1))
    await s.enter(0.4, partial(worker, 3))
    await s.enter(0.3, None)
    await s.enter(0.2, partial(worker, 2))
    await s.run()
    assert len(results) == 2
    assert results[0][0] == 1
    assert results[0][1] - t0 == pytest.approx(0.1, 0.1)
    assert results[1][0] == 2
    assert results[1][1] - t0 == pytest.approx(0.2, 0.1)


@pytest.mark.slow
async def test_sched_task():
    s = AsyncScheduler()
    t = spawn(s.run)

    results = []

    async def worker(i):
        results.append((i, time()))

    t0 = time()
    await s.enter(0.1, partial(worker, 1))
    await s.enter(0.4, partial(worker, 3))
    await s.enter(0.3, None)
    await s.enter(0.2, partial(worker, 2))

    await gather(t)
    t1 = time()
    assert t1 - t0 == pytest.approx(0.3, 0.2)

    assert len(results) == 2
    assert results[0][0] == 1
    assert results[0][1] - t0 == pytest.approx(0.1, 0.2)
    assert results[1][0] == 2
    assert results[1][1] - t0 == pytest.approx(0.2, 0.2)


@pytest.mark.slow
async def test_sched_error(caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    s = AsyncScheduler()
    t = spawn(s.run)

    results = []

    async def worker(i):
        results.append((i, time()))

    async def error():
        1 / 0

    t0 = time()
    await s.enter(0.1, partial(worker, 1))
    await s.enter(0.4, None)
    await s.enter(0.3, partial(worker, 2))
    await s.enter(0.2, error)

    await gather(t)
    t1 = time()
    assert t1 - t0 == pytest.approx(0.4, 0.1)

    assert len(results) == 2
    assert results[0][0] == 1
    assert results[0][1] - t0 == pytest.approx(0.1, 0.1)
    assert results[1][0] == 2
    assert results[1][1] - t0 == pytest.approx(0.3, 0.1)

    assert len(caplog.records) == 1
    assert "ZeroDivisionError" in caplog.records[0].message


@pytest.mark.slow
async def test_empty_queue_timeout():
    s = AsyncScheduler()

    async with timed_wait(s) as times:
        s.EMPTY_QUEUE_TIMEOUT = 0.2

        t = spawn(s.run)
        await asleep(0.5)
        await s.enter(0.5, None)
        await gather(t)

    for got, want in zip(times, [0.2, 0.4, 0.5, 1.0]):
        assert got == pytest.approx(want, 0.2), times


@pytest.mark.slow
async def test_first_task_rescheduling():
    s = AsyncScheduler()

    async with timed_wait(s) as times:
        s.EMPTY_QUEUE_TIMEOUT = 0.1

        await s.enter(0.4, noop)
        t = spawn(s.run)
        await s.enter(0.6, None)  # this task doesn't trigger a reschedule
        await asleep(0.1)
        await s.enter(0.1, noop)  # this triggers a reschedule
        await gather(t)

    for got, want in zip(times, [0.1, 0.2, 0.4, 0.6, 0.6]):
        assert got == pytest.approx(want, 0.2), times


@asynccontextmanager
async def timed_wait(s):
    """
    Hack the scheduler's Event.wait() function in order to log waited time.

    The context is a list where the times are accumulated.
    """
    t0 = time()
    times = []

    wait_orig = s._event.wait

    async def wait_logging(timeout=None):
        if True:  # ASYNC
            args = ()
        else:
            args = (timeout,)

        try:
            rv = await wait_orig(*args)
        finally:
            times.append(time() - t0)
        return rv

    setattr(s._event, "wait", wait_logging)

    yield times

    times.append(time() - t0)


async def noop():
    pass
