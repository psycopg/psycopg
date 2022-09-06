import asyncio
import logging
from time import time
from typing import Any, List, Tuple

import pytest

import psycopg
from psycopg.pq import TransactionStatus
from psycopg._compat import create_task, Counter

try:
    import psycopg_pool as pool
except ImportError:
    # Tests should have been skipped if the package is not available
    pass

pytestmark = [pytest.mark.asyncio]


async def test_defaults(dsn):
    async with pool.AsyncConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 4
        assert p.timeout == 30
        assert p.max_idle == 10 * 60
        assert p.max_lifetime == 60 * 60
        assert p.num_workers == 3


@pytest.mark.parametrize("min_size, max_size", [(2, None), (0, 2), (2, 4)])
async def test_min_size_max_size(dsn, min_size, max_size):
    async with pool.AsyncConnectionPool(dsn, min_size=min_size, max_size=max_size) as p:
        assert p.min_size == min_size
        assert p.max_size == max_size if max_size is not None else min_size


@pytest.mark.parametrize("min_size, max_size", [(0, 0), (0, None), (-1, None), (4, 2)])
async def test_bad_size(dsn, min_size, max_size):
    with pytest.raises(ValueError):
        pool.AsyncConnectionPool(min_size=min_size, max_size=max_size)


async def test_connection_class(dsn):
    class MyConn(psycopg.AsyncConnection[Any]):
        pass

    async with pool.AsyncConnectionPool(dsn, connection_class=MyConn, min_size=1) as p:
        async with p.connection() as conn:
            assert isinstance(conn, MyConn)


async def test_kwargs(dsn):
    async with pool.AsyncConnectionPool(
        dsn, kwargs={"autocommit": True}, min_size=1
    ) as p:
        async with p.connection() as conn:
            assert conn.autocommit


@pytest.mark.crdb_skip("backend pid")
async def test_its_really_a_pool(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=2) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid

            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid

        async with p.connection() as conn:
            assert conn.info.backend_pid in (pid1, pid2)


async def test_context(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        assert not p.closed
    assert p.closed


@pytest.mark.crdb_skip("backend pid")
async def test_connection_not_lost(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        with pytest.raises(ZeroDivisionError):
            async with p.connection() as conn:
                pid = conn.info.backend_pid
                1 / 0

        async with p.connection() as conn2:
            assert conn2.info.backend_pid == pid


@pytest.mark.slow
@pytest.mark.timing
async def test_concurrent_filling(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)

    async def add_time(self, conn):
        times.append(time() - t0)
        await add_orig(self, conn)

    add_orig = pool.AsyncConnectionPool._add_to_pool
    monkeypatch.setattr(pool.AsyncConnectionPool, "_add_to_pool", add_time)

    times: List[float] = []
    t0 = time()

    async with pool.AsyncConnectionPool(dsn, min_size=5, num_workers=2) as p:
        await p.wait(1.0)
        want_times = [0.1, 0.1, 0.2, 0.2, 0.3]
        assert len(times) == len(want_times)
        for got, want in zip(times, want_times):
            assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
@pytest.mark.timing
async def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        async with pool.AsyncConnectionPool(dsn, min_size=4, num_workers=1) as p:
            await p.wait(0.3)

    async with pool.AsyncConnectionPool(dsn, min_size=4, num_workers=1) as p:
        await p.wait(0.5)

    async with pool.AsyncConnectionPool(dsn, min_size=4, num_workers=2) as p:
        await p.wait(0.3)
        await p.wait(0.0001)  # idempotent


async def test_wait_closed(dsn):
    async with pool.AsyncConnectionPool(dsn) as p:
        pass

    with pytest.raises(pool.PoolClosed):
        await p.wait()


@pytest.mark.slow
async def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(pool.PoolTimeout):
        async with pool.AsyncConnectionPool(
            proxy.client_dsn, min_size=1, num_workers=1
        ) as p:
            await p.wait(0.2)

    async with pool.AsyncConnectionPool(
        proxy.client_dsn, min_size=1, num_workers=1
    ) as p:
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

    async with pool.AsyncConnectionPool(dsn, min_size=1, configure=configure) as p:
        await p.wait(timeout=1.0)
        async with p.connection() as conn:
            assert inits == 1
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"  # type: ignore[index]

        async with p.connection() as conn:
            assert inits == 1
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"  # type: ignore[index]
            await conn.close()

        async with p.connection() as conn:
            assert inits == 2
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"  # type: ignore[index]


@pytest.mark.slow
async def test_configure_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def configure(conn):
        await conn.execute("select 1")

    async with pool.AsyncConnectionPool(dsn, min_size=1, configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.slow
async def test_configure_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def configure(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    async with pool.AsyncConnectionPool(dsn, min_size=1, configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "WAT" in caplog.records[0].message


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

    async with pool.AsyncConnectionPool(dsn, min_size=1, reset=reset) as p:
        async with p.connection() as conn:
            assert resets == 0
            await conn.execute("set timezone to '+2:00'")

        await p.wait()
        assert resets == 1

        async with p.connection() as conn:
            cur = await conn.execute("show timezone")
            assert (await cur.fetchone()) == ("UTC",)

        await p.wait()
        assert resets == 2


@pytest.mark.crdb_skip("backend pid")
async def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def reset(conn):
        await conn.execute("reset all")

    async with pool.AsyncConnectionPool(dsn, min_size=1, reset=reset) as p:
        async with p.connection() as conn:
            await conn.execute("select 1")
            pid1 = conn.info.backend_pid

        async with p.connection() as conn:
            await conn.execute("select 1")
            pid2 = conn.info.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def reset(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    async with pool.AsyncConnectionPool(dsn, min_size=1, reset=reset) as p:
        async with p.connection() as conn:
            await conn.execute("select 1")
            pid1 = conn.info.backend_pid

        async with p.connection() as conn:
            await conn.execute("select 1")
            pid2 = conn.info.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "WAT" in caplog.records[0].message


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
    async with pool.AsyncConnectionPool(dsn, min_size=2) as p:
        await p.wait()
        ts = [create_task(worker(i)) for i in range(6)]
        await asyncio.gather(*ts)

    times = [item[1] for item in results]
    want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.1), times

    assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
async def test_queue_size(dsn):
    async def worker(t, ev=None):
        try:
            async with p.connection():
                if ev:
                    ev.set()
                await asyncio.sleep(t)
        except pool.TooManyRequests as e:
            errors.append(e)
        else:
            success.append(True)

    errors: List[Exception] = []
    success: List[bool] = []

    async with pool.AsyncConnectionPool(dsn, min_size=1, max_waiting=3) as p:
        await p.wait()
        ev = asyncio.Event()
        create_task(worker(0.3, ev))
        await ev.wait()

        ts = [create_task(worker(0.1)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(success) == 4
    assert len(errors) == 1
    assert isinstance(errors[0], pool.TooManyRequests)
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
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    async with pool.AsyncConnectionPool(dsn, min_size=2, timeout=0.1) as p:
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
        except pool.PoolTimeout:
            if timeout > 0.2:
                raise

    async with pool.AsyncConnectionPool(dsn, min_size=2) as p:
        results: List[int] = []
        ts = [
            create_task(worker(i, timeout))
            for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4])
        ]
        await asyncio.gather(*ts)

        await asyncio.sleep(0.2)
        assert set(results) == set([0, 1, 3, 4])
        assert len(p._pool) == 2  # no connection was lost


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
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    async with pool.AsyncConnectionPool(dsn, min_size=2, timeout=0.1) as p:
        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.crdb_skip("backend pid")
async def test_broken_reconnect(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid
            await conn.close()

        async with p.connection() as conn2:
            pid2 = conn2.info.backend_pid

    assert pid1 != pid2


@pytest.mark.crdb_skip("backend pid")
async def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        conn = await p.getconn()
        pid = conn.info.backend_pid
        await conn.execute("create table test_intrans_rollback ()")
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.info.backend_pid == pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE
            cur = await conn2.execute(
                "select 1 from pg_class where relname = 'test_intrans_rollback'"
            )
            assert not await cur.fetchone()

    assert len(caplog.records) == 1
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_inerror_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        conn = await p.getconn()
        pid = conn.info.backend_pid
        with pytest.raises(psycopg.ProgrammingError):
            await conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.info.backend_pid == pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
@pytest.mark.crdb_skip("copy")
async def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        conn = await p.getconn()
        pid = conn.info.backend_pid
        conn.pgconn.exec_(b"copy (select * from generate_series(1, 10)) to stdout")
        assert conn.info.transaction_status == TransactionStatus.ACTIVE
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.info.backend_pid != pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 2
    assert "ACTIVE" in caplog.records[0].message
    assert "BAD" in caplog.records[1].message


@pytest.mark.crdb_skip("backend pid")
async def test_fail_rollback_close(dsn, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        conn = await p.getconn()

        async def bad_rollback():
            conn.pgconn.finish()
            await orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        pid = conn.info.backend_pid
        with pytest.raises(psycopg.ProgrammingError):
            await conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.info.backend_pid != pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 3
    assert "INERROR" in caplog.records[0].message
    assert "OperationalError" in caplog.records[1].message
    assert "BAD" in caplog.records[2].message


async def test_close_no_tasks(dsn):
    p = pool.AsyncConnectionPool(dsn)
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
    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        conn = await aconn_cls.connect(dsn)
        with pytest.raises(ValueError):
            await p.putconn(conn)

    await conn.close()


async def test_putconn_wrong_pool(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1) as p1:
        async with pool.AsyncConnectionPool(dsn, min_size=1) as p2:
            conn = await p1.getconn()
            with pytest.raises(ValueError):
                await p2.putconn(conn)


async def test_closed_getconn(dsn):
    p = pool.AsyncConnectionPool(dsn, min_size=1)
    assert not p.closed
    async with p.connection():
        pass

    await p.close()
    assert p.closed

    with pytest.raises(pool.PoolClosed):
        async with p.connection():
            pass


async def test_closed_putconn(dsn):
    p = pool.AsyncConnectionPool(dsn, min_size=1)

    async with p.connection() as conn:
        pass
    assert not conn.closed

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
        except pool.PoolClosed:
            success.append("w2")

    e1 = asyncio.Event()
    e2 = asyncio.Event()

    p = pool.AsyncConnectionPool(dsn, min_size=1)
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
    p = pool.AsyncConnectionPool(dsn, open=False)
    assert p.closed
    with pytest.raises(pool.PoolClosed):
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


async def test_open_context(dsn):
    p = pool.AsyncConnectionPool(dsn, open=False)
    assert p.closed

    async with p:
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    assert p.closed


async def test_open_no_op(dsn):
    p = pool.AsyncConnectionPool(dsn)
    try:
        assert not p.closed
        await p.open()
        assert not p.closed

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)

    finally:
        await p.close()


@pytest.mark.slow
@pytest.mark.timing
async def test_open_wait(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        p = pool.AsyncConnectionPool(dsn, min_size=4, num_workers=1, open=False)
        try:
            await p.open(wait=True, timeout=0.3)
        finally:
            await p.close()

    p = pool.AsyncConnectionPool(dsn, min_size=4, num_workers=1, open=False)
    try:
        await p.open(wait=True, timeout=0.5)
    finally:
        await p.close()


@pytest.mark.slow
@pytest.mark.timing
async def test_open_as_wait(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        async with pool.AsyncConnectionPool(dsn, min_size=4, num_workers=1) as p:
            await p.open(wait=True, timeout=0.3)

    async with pool.AsyncConnectionPool(dsn, min_size=4, num_workers=1) as p:
        await p.open(wait=True, timeout=0.5)


async def test_reopen(dsn):
    p = pool.AsyncConnectionPool(dsn)
    async with p.connection() as conn:
        await conn.execute("select 1")
    await p.close()
    assert p._sched_runner is None

    with pytest.raises(psycopg.OperationalError, match="cannot be reused"):
        await p.open()


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.parametrize(
    "min_size, want_times",
    [
        (2, [0.25, 0.25, 0.35, 0.45, 0.50, 0.50, 0.60, 0.70]),
        (0, [0.35, 0.45, 0.55, 0.60, 0.65, 0.70, 0.80, 0.85]),
    ],
)
async def test_grow(dsn, monkeypatch, min_size, want_times):
    delay_connection(monkeypatch, 0.1)

    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            await conn.execute("select 1 from pg_sleep(0.25)")
        t1 = time()
        results.append((n, t1 - t0))

    async with pool.AsyncConnectionPool(
        dsn, min_size=min_size, max_size=4, num_workers=3
    ) as p:
        await p.wait(1.0)
        ts = []
        results: List[Tuple[int, float]] = []

        ts = [create_task(worker(i)) for i in range(len(want_times))]
        await asyncio.gather(*ts)

    times = [item[1] for item in results]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
@pytest.mark.timing
async def test_shrink(dsn, monkeypatch):

    from psycopg_pool.pool_async import ShrinkPool

    results: List[Tuple[int, int]] = []

    async def run_hacked(self, pool):
        n0 = pool._nconns
        await orig_run(self, pool)
        n1 = pool._nconns
        results.append((n0, n1))

    orig_run = ShrinkPool._run
    monkeypatch.setattr(ShrinkPool, "_run", run_hacked)

    async def worker(n):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.1)")

    async with pool.AsyncConnectionPool(dsn, min_size=2, max_size=4, max_idle=0.2) as p:
        await p.wait(5.0)
        assert p.max_idle == 0.2

        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

        await asyncio.sleep(1)

    assert results == [(4, 4), (4, 3), (3, 2), (2, 2), (2, 2)]


@pytest.mark.slow
async def test_reconnect(proxy, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    assert pool.base.ConnectionAttempt.INITIAL_DELAY == 1.0
    assert pool.base.ConnectionAttempt.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.base.ConnectionAttempt, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.base.ConnectionAttempt, "DELAY_JITTER", 0.0)

    caplog.clear()
    proxy.start()
    async with pool.AsyncConnectionPool(proxy.client_dsn, min_size=1) as p:
        await p.wait(2.0)
        proxy.stop()

        with pytest.raises(psycopg.OperationalError):
            async with p.connection() as conn:
                await conn.execute("select 1")

        await asyncio.sleep(1.0)
        proxy.start()
        await p.wait()

        async with p.connection() as conn:
            await conn.execute("select 1")

    assert "BAD" in caplog.messages[0]
    times = [rec.created for rec in caplog.records]
    assert times[1] - times[0] < 0.05
    deltas = [times[i + 1] - times[i] for i in range(1, len(times) - 1)]
    assert len(deltas) == 3
    want = 0.1
    for delta in deltas:
        assert delta == pytest.approx(want, 0.05), deltas
        want *= 2


@pytest.mark.slow
@pytest.mark.timing
async def test_reconnect_failure(proxy):
    proxy.start()

    t1 = None

    def failed(pool):
        assert pool.name == "this-one"
        nonlocal t1
        t1 = time()

    async with pool.AsyncConnectionPool(
        proxy.client_dsn,
        name="this-one",
        min_size=1,
        reconnect_timeout=1.0,
        reconnect_failed=failed,
    ) as p:
        await p.wait(2.0)
        proxy.stop()

        with pytest.raises(psycopg.OperationalError):
            async with p.connection() as conn:
                await conn.execute("select 1")

        t0 = time()
        await asyncio.sleep(1.5)
        assert t1
        assert t1 - t0 == pytest.approx(1.0, 0.1)
        assert p._nconns == 0

        proxy.start()
        t0 = time()
        async with p.connection() as conn:
            await conn.execute("select 1")
        t1 = time()
        assert t1 - t0 < 0.2


@pytest.mark.slow
async def test_reconnect_after_grow_failed(proxy):
    # Retry reconnection after a failed connection attempt has put the pool
    # in grow mode. See issue #370.
    proxy.stop()

    ev = asyncio.Event()

    def failed(pool):
        ev.set()

    async with pool.AsyncConnectionPool(
        proxy.client_dsn, min_size=4, reconnect_timeout=1.0, reconnect_failed=failed
    ) as p:
        await asyncio.wait_for(ev.wait(), 2.0)

        with pytest.raises(pool.PoolTimeout):
            async with p.connection(timeout=0.5) as conn:
                pass

        ev.clear()
        await asyncio.wait_for(ev.wait(), 2.0)

        proxy.start()

        async with p.connection(timeout=2) as conn:
            await conn.execute("select 1")

        await p.wait(timeout=3.0)
        assert len(p._pool) == p.min_size == 4


@pytest.mark.slow
async def test_uniform_use(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=4) as p:
        counts = Counter[int]()
        for i in range(8):
            async with p.connection() as conn:
                await asyncio.sleep(0.1)
                counts[id(conn)] += 1

    assert len(counts) == 4
    assert set(counts.values()) == set([2])


@pytest.mark.slow
@pytest.mark.timing
async def test_resize(dsn):
    async def sampler():
        await asyncio.sleep(0.05)  # ensure sampling happens after shrink check
        while True:
            await asyncio.sleep(0.2)
            if p.closed:
                break
            size.append(len(p._pool))

    async def client(t):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(%s)", [t])

    size: List[int] = []

    async with pool.AsyncConnectionPool(dsn, min_size=2, max_idle=0.2) as p:
        s = create_task(sampler())

        await asyncio.sleep(0.3)

        c = create_task(client(0.4))

        await asyncio.sleep(0.2)
        await p.resize(4)
        assert p.min_size == 4
        assert p.max_size == 4

        await asyncio.sleep(0.4)
        await p.resize(2)
        assert p.min_size == 2
        assert p.max_size == 2

        await asyncio.sleep(0.6)

    await asyncio.gather(s, c)
    assert size == [2, 1, 3, 4, 3, 2, 2]


@pytest.mark.parametrize("min_size, max_size", [(0, 0), (-1, None), (4, 2)])
async def test_bad_resize(dsn, min_size, max_size):
    async with pool.AsyncConnectionPool() as p:
        with pytest.raises(ValueError):
            await p.resize(min_size=min_size, max_size=max_size)


async def test_jitter():
    rnds = [pool.AsyncConnectionPool._jitter(30, -0.1, +0.2) for i in range(100)]
    assert 27 <= min(rnds) <= 28
    assert 35 < max(rnds) < 36


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_max_lifetime(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1, max_lifetime=0.2) as p:
        await asyncio.sleep(0.1)
        pids = []
        for i in range(5):
            async with p.connection() as conn:
                pids.append(conn.info.backend_pid)
            await asyncio.sleep(0.2)

    assert pids[0] == pids[1] != pids[4], pids


@pytest.mark.crdb_skip("backend pid")
async def test_check(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    async with pool.AsyncConnectionPool(dsn, min_size=4) as p:
        await p.wait(1.0)
        async with p.connection() as conn:
            pid = conn.info.backend_pid

        await p.wait(1.0)
        pids = set(conn.info.backend_pid for conn in p._pool)
        assert pid in pids
        await conn.close()

        assert len(caplog.records) == 0
        await p.check()
        assert len(caplog.records) == 1
        await p.wait(1.0)
        pids2 = set(conn.info.backend_pid for conn in p._pool)
        assert len(pids & pids2) == 3
        assert pid not in pids2


async def test_check_idle(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=2) as p:
        await p.wait(1.0)
        await p.check()
        async with p.connection() as conn:
            assert conn.info.transaction_status == TransactionStatus.IDLE


@pytest.mark.slow
@pytest.mark.timing
async def test_stats_measures(dsn):
    async def worker(n):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.2)")

    async with pool.AsyncConnectionPool(dsn, min_size=2, max_size=4) as p:
        await p.wait(2.0)

        stats = p.get_stats()
        assert stats["pool_min"] == 2
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 2
        assert stats["pool_available"] == 2
        assert stats["requests_waiting"] == 0

        ts = [create_task(worker(i)) for i in range(3)]
        await asyncio.sleep(0.1)
        stats = p.get_stats()
        await asyncio.gather(*ts)
        assert stats["pool_min"] == 2
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 3
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 0

        await p.wait(2.0)
        ts = [create_task(worker(i)) for i in range(7)]
        await asyncio.sleep(0.1)
        stats = p.get_stats()
        await asyncio.gather(*ts)
        assert stats["pool_min"] == 2
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
        except pool.PoolTimeout:
            pass

    async with pool.AsyncConnectionPool(dsn, min_size=3) as p:
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
    async with pool.AsyncConnectionPool(proxy.client_dsn, min_size=3) as p:
        await p.wait()
        stats = p.get_stats()
        assert stats["connections_num"] == 3
        assert stats.get("connections_errors", 0) == 0
        assert stats.get("connections_lost", 0) == 0
        assert 580 <= stats["connections_ms"] < 1200

        proxy.stop()
        await p.check()
        await asyncio.sleep(0.1)
        stats = p.get_stats()
        assert stats["connections_num"] > 3
        assert stats["connections_errors"] > 0
        assert stats["connections_lost"] == 3


@pytest.mark.slow
async def test_spike(dsn, monkeypatch):
    # Inspired to https://github.com/brettwooldridge/HikariCP/blob/dev/
    # documents/Welcome-To-The-Jungle.md
    delay_connection(monkeypatch, 0.15)

    async def worker():
        async with p.connection():
            await asyncio.sleep(0.002)

    async with pool.AsyncConnectionPool(dsn, min_size=5, max_size=10) as p:
        await p.wait()

        ts = [create_task(worker()) for i in range(50)]
        await asyncio.gather(*ts)
        await p.wait()

        assert len(p._pool) < 7


async def test_debug_deadlock(dsn):
    # https://github.com/psycopg/psycopg/issues/230
    logger = logging.getLogger("psycopg")
    handler = logging.StreamHandler()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        async with pool.AsyncConnectionPool(dsn, min_size=4, open=True) as p:
            await p.wait(timeout=2)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """

    async def connect_delay(*args, **kwargs):
        t0 = time()
        rv = await connect_orig(*args, **kwargs)
        t1 = time()
        await asyncio.sleep(max(0, sec - (t1 - t0)))
        return rv

    connect_orig = psycopg.AsyncConnection.connect
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect_delay)


async def ensure_waiting(p, num=1):
    while len(p._waiting) < num:
        await asyncio.sleep(0)
