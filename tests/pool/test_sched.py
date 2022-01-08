import logging
from time import time, sleep
from functools import partial
from threading import Thread

import pytest

try:
    from psycopg_pool.sched import Scheduler
except ImportError:
    # Tests should have been skipped if the package is not available
    pass

pytestmark = [pytest.mark.timing]


@pytest.mark.slow
def test_sched():
    s = Scheduler()
    results = []

    def worker(i):
        results.append((i, time()))

    t0 = time()
    s.enter(0.1, partial(worker, 1))
    s.enter(0.4, partial(worker, 3))
    s.enter(0.3, None)
    s.enter(0.2, partial(worker, 2))
    s.run()
    assert len(results) == 2
    assert results[0][0] == 1
    assert results[0][1] - t0 == pytest.approx(0.1, 0.1)
    assert results[1][0] == 2
    assert results[1][1] - t0 == pytest.approx(0.2, 0.1)


@pytest.mark.slow
def test_sched_thread():
    s = Scheduler()
    t = Thread(target=s.run, daemon=True)
    t.start()

    results = []

    def worker(i):
        results.append((i, time()))

    t0 = time()
    s.enter(0.1, partial(worker, 1))
    s.enter(0.4, partial(worker, 3))
    s.enter(0.3, None)
    s.enter(0.2, partial(worker, 2))

    t.join()
    t1 = time()
    assert t1 - t0 == pytest.approx(0.3, 0.2)

    assert len(results) == 2
    assert results[0][0] == 1
    assert results[0][1] - t0 == pytest.approx(0.1, 0.2)
    assert results[1][0] == 2
    assert results[1][1] - t0 == pytest.approx(0.2, 0.2)


@pytest.mark.slow
def test_sched_error(caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    s = Scheduler()
    t = Thread(target=s.run, daemon=True)
    t.start()

    results = []

    def worker(i):
        results.append((i, time()))

    def error():
        1 / 0

    t0 = time()
    s.enter(0.1, partial(worker, 1))
    s.enter(0.4, None)
    s.enter(0.3, partial(worker, 2))
    s.enter(0.2, error)

    t.join()
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
def test_empty_queue_timeout():
    s = Scheduler()

    t0 = time()
    times = []

    wait_orig = s._event.wait

    def wait_logging(timeout=None):
        rv = wait_orig(timeout)
        times.append(time() - t0)
        return rv

    setattr(s._event, "wait", wait_logging)
    s.EMPTY_QUEUE_TIMEOUT = 0.2

    t = Thread(target=s.run)
    t.start()
    sleep(0.5)
    s.enter(0.5, None)
    t.join()
    times.append(time() - t0)
    for got, want in zip(times, [0.2, 0.4, 0.5, 1.0]):
        assert got == pytest.approx(want, 0.2), times


@pytest.mark.slow
def test_first_task_rescheduling():
    s = Scheduler()

    t0 = time()
    times = []

    wait_orig = s._event.wait

    def wait_logging(timeout=None):
        rv = wait_orig(timeout)
        times.append(time() - t0)
        return rv

    setattr(s._event, "wait", wait_logging)
    s.EMPTY_QUEUE_TIMEOUT = 0.1

    s.enter(0.4, lambda: None)
    t = Thread(target=s.run)
    t.start()
    s.enter(0.6, None)  # this task doesn't trigger a reschedule
    sleep(0.1)
    s.enter(0.1, lambda: None)  # this triggers a reschedule
    t.join()
    times.append(time() - t0)
    for got, want in zip(times, [0.1, 0.2, 0.4, 0.6, 0.6]):
        assert got == pytest.approx(want, 0.2), times
