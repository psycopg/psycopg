import asyncio
import logging
from time import time
from typing import Any, List, Tuple

import pytest
from packaging.version import parse as ver  # noqa: F401  # used in skipif

import psycopg
from psycopg.pq import TransactionStatus
from psycopg._compat import create_task
from .test_pool_async import delay_connection, ensure_waiting

pytestmark = [pytest.mark.asyncio]

try:
    from psycopg_pool import AsyncNullConnectionPool  # noqa: F401
    from psycopg_pool import PoolClosed, PoolTimeout, TooManyRequests
except ImportError:
    pass


async def test_defaults(dsn):
    async with AsyncNullConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 0
        assert p.timeout == 30
        assert p.max_idle == 10 * 60
        assert p.max_lifetime == 60 * 60
        assert p.num_workers == 3


async def test_min_size_max_size(dsn):
    async with AsyncNullConnectionPool(dsn, min_size=0, max_size=2) as p:
        assert p.min_size == 0
        assert p.max_size == 2


@pytest.mark.parametrize("min_size, max_size", [(1, None), (-1, None), (0, -2)])
async def test_bad_size(dsn, min_size, max_size):
    with pytest.raises(ValueError):
        AsyncNullConnectionPool(min_size=min_size, max_size=max_size)


async def test_connection_class(dsn):
    class MyConn(psycopg.AsyncConnection[Any]):
        pass

    async with AsyncNullConnectionPool(dsn, connection_class=MyConn) as p:
        async with p.connection() as conn:
            assert isinstance(conn, MyConn)


async def test_kwargs(dsn):
    async with AsyncNullConnectionPool(dsn, kwargs={"autocommit": True}) as p:
        async with p.connection() as conn:
            assert conn.autocommit


@pytest.mark.crdb_skip("backend pid")
async def test_its_no_pool_at_all(dsn):
    async with AsyncNullConnectionPool(dsn, max_size=2) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid

            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid

        async with p.connection() as conn:
            assert conn.info.backend_pid not in (pid1, pid2)


async def test_context(dsn):
    async with AsyncNullConnectionPool(dsn) as p:
        assert not p.closed
    assert p.closed


@pytest.mark.slow
@pytest.mark.timing
async def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.2)
    with pytest.raises(PoolTimeout):
        async with AsyncNullConnectionPool(dsn, num_workers=1) as p:
            await p.wait(0.1)

    async with AsyncNullConnectionPool(dsn, num_workers=1) as p:
        await p.wait(0.4)


async def test_wait_closed(dsn):
    async with AsyncNullConnectionPool(dsn) as p:
        pass

    with pytest.raises(PoolClosed):
        await p.wait()


@pytest.mark.slow
async def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(PoolTimeout):
        async with AsyncNullConnectionPool(proxy.client_dsn, num_workers=1) as p:
            await p.wait(0.2)

    async with AsyncNullConnectionPool(proxy.client_dsn, num_workers=1) as p:
        await asyncio.sleep(0.5)
        assert not p._pool
        proxy.start()

        async with p.connection() as conn:
            await conn.execute("select 1")


async def test_configure(dsn):
    inits = 0

    async def configure(conn):
        nonlocal inits
        inits += 1
        async with conn.transaction():
            await conn.execute("set default_transaction_read_only to on")

    async with AsyncNullConnectionPool(dsn, configure=configure) as p:
        async with p.connection() as conn:
            assert inits == 1
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"  # type: ignore[index]

        async with p.connection() as conn:
            assert inits == 2
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"  # type: ignore[index]
            await conn.close()

        async with p.connection() as conn:
            assert inits == 3
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"  # type: ignore[index]


@pytest.mark.slow
async def test_configure_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def configure(conn):
        await conn.execute("select 1")

    async with AsyncNullConnectionPool(dsn, configure=configure) as p:
        with pytest.raises(PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.slow
async def test_configure_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def configure(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    async with AsyncNullConnectionPool(dsn, configure=configure) as p:
        with pytest.raises(PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_reset(dsn):
    resets = 0

    async def setup(conn):
        async with conn.transaction():
            await conn.execute("set timezone to '+1:00'")

    async def reset(conn):
        nonlocal resets
        resets += 1
        async with conn.transaction():
            await conn.execute("set timezone to utc")

    pids = []

    async def worker():
        async with p.connection() as conn:
            assert resets == 1
            cur = await conn.execute("show timezone")
            assert (await cur.fetchone()) == ("UTC",)
            pids.append(conn.info.backend_pid)

    async with AsyncNullConnectionPool(dsn, max_size=1, reset=reset) as p:
        async with p.connection() as conn:

            # Queue the worker so it will take the same connection a second time
            # instead of making a new one.
            t = create_task(worker())
            await ensure_waiting(p)

            assert resets == 0
            await conn.execute("set timezone to '+2:00'")
            pids.append(conn.info.backend_pid)

        await asyncio.gather(t)
        await p.wait()

    assert resets == 1
    assert pids[0] == pids[1]


@pytest.mark.crdb_skip("backend pid")
async def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def reset(conn):
        await conn.execute("reset all")

    pids = []

    async def worker():
        async with p.connection() as conn:
            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

    async with AsyncNullConnectionPool(dsn, max_size=1, reset=reset) as p:
        async with p.connection() as conn:

            t = create_task(worker())
            await ensure_waiting(p)

            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

        await asyncio.gather(t)

    assert pids[0] != pids[1]
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def reset(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    pids = []

    async def worker():
        async with p.connection() as conn:
            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

    async with AsyncNullConnectionPool(dsn, max_size=1, reset=reset) as p:
        async with p.connection() as conn:

            t = create_task(worker())
            await ensure_waiting(p)

            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

        await asyncio.gather(t)

    assert pids[0] != pids[1]
    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.slow
@pytest.mark.skipif("ver(psycopg.__version__) < ver('3.0.8')")
async def test_no_queue_timeout(deaf_port):
    async with AsyncNullConnectionPool(
        kwargs={"host": "localhost", "port": deaf_port}
    ) as p:
        with pytest.raises(PoolTimeout):
            async with p.connection(timeout=1):
                pass


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_queue(dsn):
    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.2)")
            pid = conn.info.backend_pid
        t1 = time()
        results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    async with AsyncNullConnectionPool(dsn, max_size=2) as p:
        await p.wait()
        ts = [create_task(worker(i)) for i in range(6)]
        await asyncio.gather(*ts)

    times = [item[1] for item in results]
    want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times

    assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
async def test_queue_size(dsn):
    async def worker(t, ev=None):
        try:
            async with p.connection():
                if ev:
                    ev.set()
                await asyncio.sleep(t)
        except TooManyRequests as e:
            errors.append(e)
        else:
            success.append(True)

    errors: List[Exception] = []
    success: List[bool] = []

    async with AsyncNullConnectionPool(dsn, max_size=1, max_waiting=3) as p:
        await p.wait()
        ev = asyncio.Event()
        create_task(worker(0.3, ev))
        await ev.wait()

        ts = [create_task(worker(0.1)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(success) == 4
    assert len(errors) == 1
    assert isinstance(errors[0], TooManyRequests)
    assert p.name in str(errors[0])
    assert str(p.max_waiting) in str(errors[0])
    assert p.get_stats()["requests_errors"] == 1


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_queue_timeout(dsn):
    async def worker(n):
        t0 = time()
        try:
            async with p.connection() as conn:
                await conn.execute("select pg_sleep(0.2)")
                pid = conn.info.backend_pid
        except PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    async with AsyncNullConnectionPool(dsn, max_size=2, timeout=0.1) as p:
        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(results) == 2
    assert len(errors) == 2
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.slow
@pytest.mark.timing
async def test_dead_client(dsn):
    async def worker(i, timeout):
        try:
            async with p.connection(timeout=timeout) as conn:
                await conn.execute("select pg_sleep(0.3)")
                results.append(i)
        except PoolTimeout:
            if timeout > 0.2:
                raise

    async with AsyncNullConnectionPool(dsn, max_size=2) as p:
        results: List[int] = []
        ts = [
            create_task(worker(i, timeout))
            for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4])
        ]
        await asyncio.gather(*ts)

        await asyncio.sleep(0.2)
        assert set(results) == set([0, 1, 3, 4])


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_queue_timeout_override(dsn):
    async def worker(n):
        t0 = time()
        timeout = 0.25 if n == 3 else None
        try:
            async with p.connection(timeout=timeout) as conn:
                await conn.execute("select pg_sleep(0.2)")
                pid = conn.info.backend_pid
        except PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    async with AsyncNullConnectionPool(dsn, max_size=2, timeout=0.1) as p:
        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.crdb_skip("backend pid")
async def test_broken_reconnect(dsn):
    async with AsyncNullConnectionPool(dsn, max_size=1) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid
            await conn.close()

        async with p.connection() as conn2:
            pid2 = conn2.info.backend_pid

    assert pid1 != pid2


@pytest.mark.crdb_skip("backend pid")
async def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE
            cur = await conn.execute(
                "select 1 from pg_class where relname = 'test_intrans_rollback'"
            )
            assert not await cur.fetchone()

    async with AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()

        # Queue the worker so it will take the connection a second time instead
        # of making a new one.
        t = create_task(worker())
        await ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        await conn.execute("create table test_intrans_rollback ()")
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        await p.putconn(conn)
        await asyncio.gather(t)

    assert pids[0] == pids[1]
    assert len(caplog.records) == 1
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_inerror_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    async with AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()

        t = create_task(worker())
        await ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        with pytest.raises(psycopg.ProgrammingError):
            await conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)
        await asyncio.gather(t)

    assert pids[0] == pids[1]
    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
@pytest.mark.crdb_skip("copy")
async def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    async with AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()

        t = create_task(worker())
        await ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        conn.pgconn.exec_(b"copy (select * from generate_series(1, 10)) to stdout")
        assert conn.info.transaction_status == TransactionStatus.ACTIVE
        await p.putconn(conn)
        await asyncio.gather(t)

    assert pids[0] != pids[1]
    assert len(caplog.records) == 2
    assert "ACTIVE" in caplog.records[0].message
    assert "BAD" in caplog.records[1].message


@pytest.mark.crdb_skip("backend pid")
async def test_fail_rollback_close(dsn, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    async with AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()
        t = create_task(worker())
        await ensure_waiting(p)

        async def bad_rollback():
            conn.pgconn.finish()
            await orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        pids.append(conn.info.backend_pid)
        with pytest.raises(psycopg.ProgrammingError):
            await conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)
        await asyncio.gather(t)

    assert pids[0] != pids[1]
    assert len(caplog.records) == 3
    assert "INERROR" in caplog.records[0].message
    assert "OperationalError" in caplog.records[1].message
    assert "BAD" in caplog.records[2].message


async def test_close_no_tasks(dsn):
    p = AsyncNullConnectionPool(dsn)
    assert p._sched_runner and not p._sched_runner.done()
    assert p._workers
    workers = p._workers[:]
    for t in workers:
        assert not t.done()

    await p.close()
    assert p._sched_runner is None
    assert not p._workers
    for t in workers:
        assert t.done()


async def test_putconn_no_pool(aconn_cls, dsn):
    async with AsyncNullConnectionPool(dsn) as p:
        conn = await aconn_cls.connect(dsn)
        with pytest.raises(ValueError):
            await p.putconn(conn)

    await conn.close()


async def test_putconn_wrong_pool(dsn):
    async with AsyncNullConnectionPool(dsn) as p1:
        async with AsyncNullConnectionPool(dsn) as p2:
            conn = await p1.getconn()
            with pytest.raises(ValueError):
                await p2.putconn(conn)


async def test_closed_getconn(dsn):
    p = AsyncNullConnectionPool(dsn)
    assert not p.closed
    async with p.connection():
        pass

    await p.close()
    assert p.closed

    with pytest.raises(PoolClosed):
        async with p.connection():
            pass


async def test_closed_putconn(dsn):
    p = AsyncNullConnectionPool(dsn)

    async with p.connection() as conn:
        pass
    assert conn.closed

    async with p.connection() as conn:
        await p.close()
    assert conn.closed


async def test_closed_queue(dsn):
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
        except PoolClosed:
            success.append("w2")

    e1 = asyncio.Event()
    e2 = asyncio.Event()

    p = AsyncNullConnectionPool(dsn, max_size=1)
    await p.wait()
    success: List[str] = []

    t1 = create_task(w1())
    # Wait until w1 has received a connection
    await e1.wait()

    t2 = create_task(w2())
    # Wait until w2 is in the queue
    await ensure_waiting(p)
    await p.close()

    # Wait for the workers to finish
    e2.set()
    await asyncio.gather(t1, t2)
    assert len(success) == 2


async def test_open_explicit(dsn):
    p = AsyncNullConnectionPool(dsn, open=False)
    assert p.closed
    with pytest.raises(PoolClosed):
        await p.getconn()

    with pytest.raises(PoolClosed, match="is not open yet"):
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

    with pytest.raises(PoolClosed, match="is already closed"):
        await p.getconn()


async def test_open_context(dsn):
    p = AsyncNullConnectionPool(dsn, open=False)
    assert p.closed

    async with p:
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    assert p.closed


async def test_open_no_op(dsn):
    p = AsyncNullConnectionPool(dsn)
    try:
        assert not p.closed
        await p.open()
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    finally:
        await p.close()


async def test_reopen(dsn):
    p = AsyncNullConnectionPool(dsn)
    async with p.connection() as conn:
        await conn.execute("select 1")
    await p.close()
    assert p._sched_runner is None

    with pytest.raises(psycopg.OperationalError, match="cannot be reused"):
        await p.open()


@pytest.mark.parametrize("min_size, max_size", [(1, None), (-1, None), (0, -2)])
async def test_bad_resize(dsn, min_size, max_size):
    async with AsyncNullConnectionPool() as p:
        with pytest.raises(ValueError):
            await p.resize(min_size=min_size, max_size=max_size)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_max_lifetime(dsn):
    pids: List[int] = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            await asyncio.sleep(0.1)

    async with AsyncNullConnectionPool(dsn, max_size=1, max_lifetime=0.2) as p:
        ts = [create_task(worker()) for i in range(5)]
        await asyncio.gather(*ts)

    assert pids[0] == pids[1] != pids[4], pids


async def test_check(dsn):
    # no.op
    async with AsyncNullConnectionPool(dsn) as p:
        await p.check()


@pytest.mark.slow
@pytest.mark.timing
async def test_stats_measures(dsn):
    async def worker(n):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.2)")

    async with AsyncNullConnectionPool(dsn, max_size=4) as p:
        await p.wait(2.0)

        stats = p.get_stats()
        assert stats["pool_min"] == 0
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 0
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 0

        ts = [create_task(worker(i)) for i in range(3)]
        await asyncio.sleep(0.1)
        stats = p.get_stats()
        await asyncio.gather(*ts)
        assert stats["pool_min"] == 0
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 3
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 0

        await p.wait(2.0)
        ts = [create_task(worker(i)) for i in range(7)]
        await asyncio.sleep(0.1)
        stats = p.get_stats()
        await asyncio.gather(*ts)
        assert stats["pool_min"] == 0
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 4
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 3


@pytest.mark.slow
@pytest.mark.timing
async def test_stats_usage(dsn):
    async def worker(n):
        try:
            async with p.connection(timeout=0.3) as conn:
                await conn.execute("select pg_sleep(0.2)")
        except PoolTimeout:
            pass

    async with AsyncNullConnectionPool(dsn, max_size=3) as p:
        await p.wait(2.0)

        ts = [create_task(worker(i)) for i in range(7)]
        await asyncio.gather(*ts)
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


@pytest.mark.slow
async def test_stats_connect(dsn, proxy, monkeypatch):
    proxy.start()
    delay_connection(monkeypatch, 0.2)
    async with AsyncNullConnectionPool(proxy.client_dsn, max_size=3) as p:
        await p.wait()
        stats = p.get_stats()
        assert stats["connections_num"] == 1
        assert stats.get("connections_errors", 0) == 0
        assert stats.get("connections_lost", 0) == 0
        assert 200 <= stats["connections_ms"] < 300
