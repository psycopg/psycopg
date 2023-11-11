import logging
from time import time
from typing import Any, List, Tuple

import pytest

import psycopg

from ..utils import set_autocommit
from ..acompat import AEvent, spawn, gather, asleep, is_alive, skip_async, skip_sync

try:
    import psycopg_pool as pool
except ImportError:
    # Tests should have been skipped if the package is not available
    pass

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


@pytest.fixture(params=[pool.AsyncConnectionPool, pool.AsyncNullConnectionPool])
def pool_cls(request):
    return request.param


async def test_defaults(pool_cls, dsn):
    async with pool_cls(dsn) as p:
        assert p.open
        assert not p.closed
        assert p.timeout == 30
        assert p.max_idle == 10 * 60
        assert p.max_lifetime == 60 * 60
        assert p.num_workers == 3


async def test_connection_class(pool_cls, dsn):
    class MyConn(psycopg.AsyncConnection[Any]):
        pass

    async with pool_cls(dsn, connection_class=MyConn, min_size=min_size(pool_cls)) as p:
        async with p.connection() as conn:
            assert isinstance(conn, MyConn)


async def test_kwargs(pool_cls, dsn):
    async with pool_cls(
        dsn, kwargs={"autocommit": True}, min_size=min_size(pool_cls)
    ) as p:
        async with p.connection() as conn:
            assert conn.autocommit


async def test_context(pool_cls, dsn):
    async with pool_cls(dsn, min_size=min_size(pool_cls)) as p:
        assert not p.closed
    assert p.closed


async def test_create_warning(pool_cls, dsn):
    if True:  # ASYNC
        # warning on explicit open too on async
        with pytest.warns(DeprecationWarning):
            p = pool_cls(dsn, open=True)
            await p.close()

    else:
        # No warning on explicit open for sync pool
        p = pool_cls(dsn, open=True)
        try:
            async with p.connection():
                pass
        finally:
            await p.close()

    # No warning on explicit close
    p = pool_cls(dsn, open=False)
    await p.open()
    try:
        async with p.connection():
            pass
    finally:
        await p.close()

    # No warning on context manager
    async with pool_cls(dsn) as p:
        async with p.connection():
            pass

    # Warning on open not specified
    with pytest.warns(DeprecationWarning):
        p = pool_cls(dsn)
        try:
            async with p.connection():
                pass
        finally:
            await p.close()

    # Warning also if open is called explicitly on already implicitly open
    with pytest.warns(DeprecationWarning):
        p = pool_cls(dsn)
        await p.open()
        try:
            async with p.connection():
                pass
        finally:
            await p.close()


async def test_wait_closed(pool_cls, dsn):
    async with pool_cls(dsn) as p:
        pass

    with pytest.raises(pool.PoolClosed):
        await p.wait()


@pytest.mark.slow
async def test_setup_no_timeout(pool_cls, dsn, proxy):
    with pytest.raises(pool.PoolTimeout):
        async with pool_cls(
            proxy.client_dsn, min_size=min_size(pool_cls), num_workers=1
        ) as p:
            await p.wait(0.2)

    async with pool_cls(
        proxy.client_dsn, min_size=min_size(pool_cls), num_workers=1
    ) as p:
        await asleep(0.5)
        assert not p._pool
        proxy.start()

        async with p.connection() as conn:
            await conn.execute("select 1")


@pytest.mark.slow
async def test_configure_badstate(pool_cls, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def configure(conn):
        await conn.execute("select 1")

    async with pool_cls(dsn, min_size=min_size(pool_cls), configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.slow
async def test_configure_broken(pool_cls, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def configure(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    async with pool_cls(dsn, min_size=min_size(pool_cls), configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_queue(pool_cls, dsn):
    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.2)")
            pid = conn.info.backend_pid
        t1 = time()
        results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    async with pool_cls(dsn, min_size=min_size(pool_cls, 2), max_size=2) as p:
        await p.wait()
        ts = [spawn(worker, args=(i,)) for i in range(6)]
        await gather(*ts)

    times = [item[1] for item in results]
    want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times

    assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
async def test_queue_size(pool_cls, dsn):
    async def worker(t, ev=None):
        try:
            async with p.connection():
                if ev:
                    ev.set()
                await asleep(t)
        except pool.TooManyRequests as e:
            errors.append(e)
        else:
            success.append(True)

    errors: List[Exception] = []
    success: List[bool] = []

    async with pool_cls(
        dsn, min_size=min_size(pool_cls), max_size=1, max_waiting=3
    ) as p:
        await p.wait()
        ev = AEvent()
        spawn(worker, args=(0.3, ev))
        await ev.wait()

        ts = [spawn(worker, args=(0.1,)) for i in range(4)]
        await gather(*ts)

    assert len(success) == 4
    assert len(errors) == 1
    assert isinstance(errors[0], pool.TooManyRequests)
    assert p.name in str(errors[0])
    assert str(p.max_waiting) in str(errors[0])
    assert p.get_stats()["requests_errors"] == 1


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_queue_timeout(pool_cls, dsn):
    async def worker(n):
        t0 = time()
        try:
            async with p.connection() as conn:
                await conn.execute("select pg_sleep(0.2)")
                pid = conn.info.backend_pid
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    async with pool_cls(
        dsn, min_size=min_size(pool_cls, 2), max_size=2, timeout=0.1
    ) as p:
        ts = [spawn(worker, args=(i,)) for i in range(4)]
        await gather(*ts)

    assert len(results) == 2
    assert len(errors) == 2
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.slow
@pytest.mark.timing
async def test_dead_client(pool_cls, dsn):
    async def worker(i, timeout):
        try:
            async with p.connection(timeout=timeout) as conn:
                await conn.execute("select pg_sleep(0.3)")
                results.append(i)
        except pool.PoolTimeout:
            if timeout > 0.2:
                raise

    async with pool_cls(dsn, min_size=min_size(pool_cls, 2), max_size=2) as p:
        results: List[int] = []
        ts = [
            spawn(worker, args=(i, timeout))
            for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4])
        ]
        await gather(*ts)

        await asleep(0.2)
        assert set(results) == set([0, 1, 3, 4])
        if pool_cls is pool.AsyncConnectionPool:
            assert len(p._pool) == 2  # no connection was lost


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_queue_timeout_override(pool_cls, dsn):
    async def worker(n):
        t0 = time()
        timeout = 0.25 if n == 3 else None
        try:
            async with p.connection(timeout=timeout) as conn:
                await conn.execute("select pg_sleep(0.2)")
                pid = conn.info.backend_pid
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    async with pool_cls(
        dsn, min_size=min_size(pool_cls, 2), max_size=2, timeout=0.1
    ) as p:
        ts = [spawn(worker, args=(i,)) for i in range(4)]
        await gather(*ts)

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.crdb_skip("backend pid")
async def test_broken_reconnect(pool_cls, dsn):
    async with pool_cls(dsn, min_size=min_size(pool_cls), max_size=1) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid
            await conn.close()

        async with p.connection() as conn2:
            pid2 = conn2.info.backend_pid

    assert pid1 != pid2


async def test_close_no_tasks(pool_cls, dsn):
    p = pool_cls(dsn)
    assert p._sched_runner and is_alive(p._sched_runner)
    workers = p._workers[:]
    assert workers
    for t in workers:
        assert is_alive(t)

    await p.close()
    assert p._sched_runner is None
    assert not p._workers
    for t in workers:
        assert not is_alive(t)


async def test_putconn_no_pool(pool_cls, aconn_cls, dsn):
    async with pool_cls(dsn, min_size=min_size(pool_cls)) as p:
        conn = await aconn_cls.connect(dsn)
        with pytest.raises(ValueError):
            await p.putconn(conn)

    await conn.close()


async def test_putconn_wrong_pool(pool_cls, dsn):
    async with pool_cls(dsn, min_size=min_size(pool_cls)) as p1:
        async with pool_cls(dsn, min_size=min_size(pool_cls)) as p2:
            conn = await p1.getconn()
            with pytest.raises(ValueError):
                await p2.putconn(conn)


@skip_async
@pytest.mark.slow
async def test_del_stops_threads(pool_cls, dsn):
    p = pool_cls(dsn)
    assert p._sched_runner is not None
    ts = [p._sched_runner] + p._workers
    del p
    await asleep(0.1)
    for t in ts:
        assert not is_alive(t), t


async def test_closed_getconn(pool_cls, dsn):
    p = pool_cls(dsn, min_size=min_size(pool_cls), open=False)
    await p.open()
    assert not p.closed
    async with p.connection():
        pass

    await p.close()
    assert p.closed

    with pytest.raises(pool.PoolClosed):
        async with p.connection():
            pass


async def test_close_connection_on_pool_close(pool_cls, dsn):
    p = pool_cls(dsn, min_size=min_size(pool_cls), open=False)
    await p.open()
    async with p.connection() as conn:
        await p.close()
    assert conn.closed


async def test_closed_queue(pool_cls, dsn):
    async def w1():
        async with p.connection() as conn:
            e1.set()  # Tell w0 that w1 got a connection
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)
            await e2.wait()  # Wait until w0 has tested w2
        success.append("w1")

    async def w2():
        try:
            async with p.connection():
                pass  # unexpected
        except pool.PoolClosed:
            success.append("w2")

    e1 = AEvent()
    e2 = AEvent()

    async with pool_cls(dsn, min_size=min_size(pool_cls), max_size=1) as p:
        await p.wait()
        success: List[str] = []

        t1 = spawn(w1)
        # Wait until w1 has received a connection
        await e1.wait()

        t2 = spawn(w2)
        # Wait until w2 is in the queue
        await ensure_waiting(p)

    # Wait for the workers to finish
    e2.set()
    await gather(t1, t2)
    assert len(success) == 2


async def test_open_explicit(pool_cls, dsn):
    p = pool_cls(dsn, open=False)
    assert p.closed
    with pytest.raises(pool.PoolClosed, match="is not open yet"):
        await p.getconn()

    with pytest.raises(pool.PoolClosed, match="is not open yet"):
        async with p.connection():
            pass

    await p.open()
    try:
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    finally:
        await p.close()

    with pytest.raises(pool.PoolClosed, match="is already closed"):
        await p.getconn()


async def test_open_context(pool_cls, dsn):
    p = pool_cls(dsn, open=False)
    assert p.closed

    async with p:
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    assert p.closed


async def test_open_no_op(pool_cls, dsn):
    p = pool_cls(dsn, open=False)
    await p.open()
    try:
        assert not p.closed
        await p.open()
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    finally:
        await p.close()


async def test_reopen(pool_cls, dsn):
    p = pool_cls(dsn, open=False)
    await p.open()
    async with p.connection() as conn:
        await conn.execute("select 1")
    await p.close()
    assert p._sched_runner is None
    assert not p._workers

    with pytest.raises(psycopg.OperationalError, match="cannot be reused"):
        await p.open()


async def test_jitter(pool_cls):
    rnds = [pool_cls._jitter(30, -0.1, +0.2) for i in range(100)]
    assert 27 <= min(rnds) <= 28
    assert 35 < max(rnds) < 36


@pytest.mark.slow
@pytest.mark.timing
async def test_stats_measures(pool_cls, dsn):
    async def worker(n):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.2)")

    async with pool_cls(dsn, min_size=min_size(pool_cls, 2), max_size=4) as p:
        await p.wait(2.0)

        stats = p.get_stats()
        assert stats["pool_min"] == min_size(pool_cls, 2)
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == min_size(pool_cls, 2)
        assert stats["pool_available"] == min_size(pool_cls, 2)
        assert stats["requests_waiting"] == 0

        ts = [spawn(worker, args=(i,)) for i in range(3)]
        await asleep(0.1)
        stats = p.get_stats()
        await gather(*ts)
        assert stats["pool_min"] == min_size(pool_cls, 2)
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 3
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 0

        await p.wait(2.0)
        ts = [spawn(worker, args=(i,)) for i in range(7)]
        await asleep(0.1)
        stats = p.get_stats()
        await gather(*ts)
        assert stats["pool_min"] == min_size(pool_cls, 2)
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 4
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 3


@pytest.mark.slow
@pytest.mark.timing
async def test_stats_usage(pool_cls, dsn):
    async def worker(n):
        try:
            async with p.connection(timeout=0.3) as conn:
                await conn.execute("select pg_sleep(0.2)")
        except pool.PoolTimeout:
            pass

    async with pool_cls(dsn, min_size=min_size(pool_cls, 3), max_size=3) as p:
        await p.wait(2.0)

        ts = [spawn(worker, args=(i,)) for i in range(7)]
        await gather(*ts)
        stats = p.get_stats()
        assert stats["requests_num"] == 7
        assert stats["requests_queued"] == 4
        assert 850 <= stats["requests_wait_ms"] <= 950
        assert stats["requests_errors"] == 1
        assert 1150 <= stats["usage_ms"] <= 1250
        assert stats.get("returns_bad", 0) == 0

        async with p.connection() as conn:
            await conn.close()
        await p.wait()
        stats = p.pop_stats()
        assert stats["requests_num"] == 8
        assert stats["returns_bad"] == 1
        async with p.connection():
            pass
        assert p.get_stats()["requests_num"] == 1


async def test_debug_deadlock(pool_cls, dsn):
    # https://github.com/psycopg/psycopg/issues/230
    logger = logging.getLogger("psycopg")
    handler = logging.StreamHandler()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        async with pool_cls(dsn, min_size=min_size(pool_cls, 4)) as p:
            await p.wait(timeout=2)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


@pytest.mark.crdb_skip("pg_terminate_backend")
@pytest.mark.parametrize("autocommit", [True, False])
async def test_check_connection(pool_cls, aconn_cls, dsn, autocommit):
    conn = await aconn_cls.connect(dsn)
    await set_autocommit(conn, autocommit)
    await pool_cls.check_connection(conn)
    assert not conn.closed
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE

    async with await aconn_cls.connect(dsn) as conn2:
        await conn2.execute("select pg_terminate_backend(%s)", [conn.info.backend_pid])

    with pytest.raises(psycopg.OperationalError):
        await pool_cls.check_connection(conn)

    assert conn.closed


async def test_check_init(pool_cls, dsn):
    checked = False

    async def check(conn):
        nonlocal checked
        checked = True

    async with pool_cls(dsn, check=check) as p:
        async with p.connection(timeout=1.0) as conn:
            await conn.execute("select 1")

    assert checked


@skip_sync
async def test_cancellation_in_queue(pool_cls, dsn):
    # https://github.com/psycopg/psycopg/issues/509

    nconns = 3

    async with pool_cls(
        dsn, min_size=min_size(pool_cls, nconns), max_size=nconns, timeout=1
    ) as p:
        await p.wait()

        got_conns = []
        ev = AEvent()

        async def worker(i):
            try:
                logging.info("worker %s started", i)
                nonlocal got_conns

                async with p.connection() as conn:
                    logging.info("worker %s got conn", i)
                    cur = await conn.execute("select 1")
                    assert (await cur.fetchone()) == (1,)

                    got_conns.append(conn)
                    if len(got_conns) >= nconns:
                        ev.set()

                    await asleep(5)

            except BaseException as ex:
                logging.info("worker %s stopped: %r", i, ex)
                raise

        # Start tasks taking up all the connections and getting in the queue
        tasks = [spawn(worker, (i,)) for i in range(nconns * 3)]

        # wait until the pool has served all the connections and clients are queued.
        await ev.wait_timeout(3.0)
        for i in range(10):
            if p.get_stats().get("requests_queued", 0):
                break
            else:
                await asleep(0.1)
        else:
            pytest.fail("no client got in the queue")

        [task.cancel() for task in reversed(tasks)]
        await gather(*tasks, return_exceptions=True, timeout=1.0)

        stats = p.get_stats()
        assert stats["pool_available"] == min_size(pool_cls, nconns)
        assert stats.get("requests_waiting", 0) == 0

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)


def min_size(pool_cls, num=1):
    """Return the minimum min_size supported by the pool class."""
    if pool_cls is pool.AsyncConnectionPool:
        return num
    elif pool_cls is pool.AsyncNullConnectionPool:
        return 0
    else:
        assert False, pool_cls


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """

    async def connect_delay(*args, **kwargs):
        t0 = time()
        rv = await connect_orig(*args, **kwargs)
        t1 = time()
        await asleep(max(0, sec - (t1 - t0)))
        return rv

    connect_orig = psycopg.AsyncConnection.connect
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect_delay)


async def ensure_waiting(p, num=1):
    """
    Wait until there are at least *num* clients waiting in the queue.
    """
    while len(p._waiting) < num:
        await asleep(0)
