import logging
import weakref
from time import time
from typing import Any, Dict, List, Tuple

import pytest

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import class_row, Row, TupleRow

from ..utils import assert_type, Counter, set_autocommit
from ..acompat import AEvent, spawn, gather, asleep, skip_sync
from .test_pool_common_async import delay_connection

try:
    import psycopg_pool as pool
except ImportError:
    # Tests should have been skipped if the package is not available
    pass

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


async def test_default_sizes(dsn):
    async with pool.AsyncConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 4


@pytest.mark.parametrize("min_size, max_size", [(2, None), (0, 2), (2, 4)])
async def test_min_size_max_size(dsn, min_size, max_size):
    async with pool.AsyncConnectionPool(dsn, min_size=min_size, max_size=max_size) as p:
        assert p.min_size == min_size
        assert p.max_size == max_size if max_size is not None else min_size


@pytest.mark.parametrize("min_size, max_size", [(0, 0), (0, None), (-1, None), (4, 2)])
async def test_bad_size(dsn, min_size, max_size):
    with pytest.raises(ValueError):
        pool.AsyncConnectionPool(min_size=min_size, max_size=max_size)


class MyRow(Dict[str, Any]):
    ...


async def test_generic_connection_type(dsn):
    async def configure(conn: psycopg.AsyncConnection[Any]) -> None:
        await set_autocommit(conn, True)

    class MyConnection(psycopg.AsyncConnection[Row]):
        pass

    async with pool.AsyncConnectionPool(
        dsn,
        connection_class=MyConnection[MyRow],
        kwargs=dict(row_factory=class_row(MyRow)),
        configure=configure,
    ) as p1:
        async with p1.connection() as conn1:
            cur1 = await conn1.execute("select 1 as x")
            (row1,) = await cur1.fetchall()
    assert_type(p1, pool.AsyncConnectionPool[MyConnection[MyRow]])
    assert_type(conn1, MyConnection[MyRow])
    assert_type(row1, MyRow)
    assert conn1.autocommit
    assert row1 == {"x": 1}

    async with pool.AsyncConnectionPool(
        dsn, connection_class=MyConnection[TupleRow]
    ) as p2:
        async with p2.connection() as conn2:
            cur2 = await conn2.execute("select 2 as y")
            (row2,) = await cur2.fetchall()
    assert_type(p2, pool.AsyncConnectionPool[MyConnection[TupleRow]])
    assert_type(conn2, MyConnection[TupleRow])
    assert_type(row2, TupleRow)
    assert row2 == (2,)


async def test_non_generic_connection_type(dsn):
    async def configure(conn: psycopg.AsyncConnection[Any]) -> None:
        await set_autocommit(conn, True)

    class MyConnection(psycopg.AsyncConnection[MyRow]):
        def __init__(self, *args: Any, **kwargs: Any):
            kwargs["row_factory"] = class_row(MyRow)
            super().__init__(*args, **kwargs)

    async with pool.AsyncConnectionPool(
        dsn,
        connection_class=MyConnection,
        configure=configure,
    ) as p1:
        async with p1.connection() as conn1:
            cur1 = await conn1.execute("select 1 as x")
            (row1,) = await cur1.fetchall()
    assert_type(p1, pool.AsyncConnectionPool[MyConnection])
    assert_type(conn1, MyConnection)
    assert_type(row1, MyRow)
    assert conn1.autocommit
    assert row1 == {"x": 1}


@pytest.mark.crdb_skip("backend pid")
async def test_its_really_a_pool(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=2) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid

            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid

        async with p.connection() as conn:
            assert conn.info.backend_pid in (pid1, pid2)


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


async def test_del_no_warning(dsn, recwarn):
    p = pool.AsyncConnectionPool(dsn, min_size=2, open=False)
    await p.open()
    async with p.connection() as conn:
        await conn.execute("select 1")

    await p.wait()
    ref = weakref.ref(p)
    del p
    assert not ref()
    assert not recwarn, [str(w.message) for w in recwarn.list]


async def test_closed_putconn(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1) as p:
        async with p.connection() as conn:
            pass
        assert not conn.closed


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
        results: List[Tuple[int, float]] = []
        ts = [spawn(worker, args=(i,)) for i in range(len(want_times))]
        await gather(*ts)

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

        ts = [spawn(worker, args=(i,)) for i in range(4)]
        await gather(*ts)

        await asleep(1)

    assert results == [(4, 4), (4, 3), (3, 2), (2, 2), (2, 2)]


@pytest.mark.slow
@pytest.mark.timing
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

        await asleep(1.0)
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
@pytest.mark.parametrize("async_cb", [pytest.param(True, marks=skip_sync), False])
async def test_reconnect_failure(proxy, async_cb):
    proxy.start()

    t1 = None

    if async_cb:

        async def failed(pool):
            assert pool.name == "this-one"
            nonlocal t1
            t1 = time()

    else:

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
        await asleep(1.5)
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

    ev = AEvent()

    def failed(pool):
        ev.set()

    async with pool.AsyncConnectionPool(
        proxy.client_dsn, min_size=4, reconnect_timeout=1.0, reconnect_failed=failed
    ) as p:
        await ev.wait_timeout(2.0)

        with pytest.raises(pool.PoolTimeout):
            async with p.connection(timeout=0.5) as conn:
                pass

        ev.clear()
        await ev.wait_timeout(2.0)

        proxy.start()

        async with p.connection(timeout=2) as conn:
            await conn.execute("select 1")

        await p.wait(timeout=3.0)
        assert len(p._pool) == p.min_size == 4


@pytest.mark.slow
async def test_refill_on_check(proxy):
    proxy.start()
    ev = AEvent()

    def failed(pool):
        ev.set()

    async with pool.AsyncConnectionPool(
        proxy.client_dsn, min_size=4, reconnect_timeout=1.0, reconnect_failed=failed
    ) as p:
        # The pool is full
        await p.wait(timeout=2)

        # Break all the connection
        proxy.stop()

        # Checking the pool will empty it
        await p.check()
        await ev.wait_timeout(2.0)
        assert len(p._pool) == 0

        # Allow to connect again
        proxy.start()

        # Make sure that check has refilled the pool
        await p.check()
        await p.wait(timeout=2)
        assert len(p._pool) == 4


@pytest.mark.slow
async def test_uniform_use(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=4) as p:
        counts = Counter[int]()
        for i in range(8):
            async with p.connection() as conn:
                await asleep(0.1)
                counts[id(conn)] += 1

    assert len(counts) == 4
    assert set(counts.values()) == set([2])


@pytest.mark.slow
@pytest.mark.timing
async def test_resize(dsn):
    async def sampler():
        await asleep(0.05)  # ensure sampling happens after shrink check
        while True:
            await asleep(0.2)
            if p.closed:
                break
            size.append(len(p._pool))

    async def client(t):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(%s)", [t])

    size: List[int] = []

    async with pool.AsyncConnectionPool(dsn, min_size=2, max_idle=0.2) as p:
        s = spawn(sampler)

        await asleep(0.3)

        c = spawn(client, args=(0.4,))

        await asleep(0.2)
        await p.resize(4)
        assert p.min_size == 4
        assert p.max_size == 4

        await asleep(0.4)
        await p.resize(2)
        assert p.min_size == 2
        assert p.max_size == 2

        await asleep(0.6)

    await gather(s, c)
    assert size == [2, 1, 3, 4, 3, 2, 2]


@pytest.mark.parametrize("min_size, max_size", [(0, 0), (-1, None), (4, 2)])
async def test_bad_resize(dsn, min_size, max_size):
    async with pool.AsyncConnectionPool() as p:
        with pytest.raises(ValueError):
            await p.resize(min_size=min_size, max_size=max_size)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_max_lifetime(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1, max_lifetime=0.2) as p:
        await asleep(0.1)
        pids = []
        for i in range(5):
            async with p.connection() as conn:
                pids.append(conn.info.backend_pid)
            await asleep(0.2)

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


@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_connect_no_check(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=2) as p:
        await p.wait(1.0)
        async with p.connection() as conn:
            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid
            await conn.execute("select pg_terminate_backend(%s)", [pid2])

        with pytest.raises(psycopg.OperationalError):
            async with p.connection() as conn:
                await conn.execute("select 1")
                async with p.connection() as conn2:
                    await conn2.execute("select 2")


@pytest.mark.crdb_skip("pg_terminate_backend")
@pytest.mark.parametrize("autocommit", [True, False])
async def test_connect_check(dsn, caplog, autocommit):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async with pool.AsyncConnectionPool(
        dsn,
        min_size=2,
        kwargs={"autocommit": autocommit},
        check=pool.AsyncConnectionPool.check_connection,
    ) as p:
        await p.wait(1.0)
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid
            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid
            await conn.execute("select pg_terminate_backend(%s)", [pid2])

        async with p.connection() as conn:
            assert conn.info.transaction_status == TransactionStatus.IDLE
            await conn.execute("select 1")
            async with p.connection() as conn2:
                assert conn2.info.transaction_status == TransactionStatus.IDLE
                await conn2.execute("select 2")

                pids = {c.info.backend_pid for c in [conn, conn2]}

    assert pid1 in pids
    assert pid2 not in pids
    assert not caplog.records


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_getconn_check(dsn, caplog, autocommit):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async with pool.AsyncConnectionPool(
        dsn,
        kwargs={"autocommit": autocommit},
        min_size=2,
        check=pool.AsyncConnectionPool.check_connection,
    ) as p:
        await p.wait(1.0)
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid
            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid
            await conn.execute("select pg_terminate_backend(%s)", [pid2])

        conn = await p.getconn()
        try:
            assert conn.info.transaction_status == TransactionStatus.IDLE
            await conn.execute("select 1")
            await conn.rollback()
            conn2 = await p.getconn()
            try:
                assert conn2.info.transaction_status == TransactionStatus.IDLE
                await conn2.execute("select 1")
                await conn2.rollback()
                pids = {c.info.backend_pid for c in [conn, conn2]}
            finally:
                await p.putconn(conn2)
        finally:
            await p.putconn(conn)

    assert pid1 in pids
    assert pid2 not in pids
    assert not caplog.records


@pytest.mark.slow
async def test_connect_check_timeout(dsn, proxy):
    proxy.start()
    async with pool.AsyncConnectionPool(
        proxy.client_dsn,
        min_size=1,
        timeout=1.0,
        check=pool.AsyncConnectionPool.check_connection,
    ) as p:
        await p.wait()

        proxy.stop()
        t0 = time()
        with pytest.raises(pool.PoolTimeout):
            async with p.connection():
                pass
        assert 1.0 <= (time() - t0) <= 1.1

        proxy.start()
        async with p.connection(timeout=10) as conn:
            await conn.execute("select 1")


@pytest.mark.slow
async def test_check_max_lifetime(dsn):
    async with pool.AsyncConnectionPool(dsn, min_size=1, max_lifetime=0.2) as p:
        async with p.connection() as conn:
            pid = conn.info.backend_pid
        async with p.connection() as conn:
            assert conn.info.backend_pid == pid
        await asleep(0.3)
        await p.check()
        async with p.connection() as conn:
            assert conn.info.backend_pid != pid


@pytest.mark.slow
async def test_stats_connect(proxy, monkeypatch):
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
        await asleep(0.1)
        stats = p.get_stats()
        assert stats["connections_num"] > 3
        assert stats["connections_errors"] > 0
        assert stats["connections_lost"] == 3


@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_stats_check(dsn):
    async with pool.AsyncConnectionPool(
        dsn, min_size=1, check=pool.AsyncConnectionPool.check_connection
    ) as p:
        await p.wait()
        async with p.connection() as conn:
            pid = conn.info.backend_pid

        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            await conn.execute("select pg_terminate_backend(%s)", [pid])

        async with p.connection() as conn:
            assert conn.info.backend_pid != pid

        stats = p.get_stats()
        assert stats["connections_lost"] == 1


@pytest.mark.slow
async def test_spike(dsn, monkeypatch):
    # Inspired to https://github.com/brettwooldridge/HikariCP/blob/dev/
    # documents/Welcome-To-The-Jungle.md
    delay_connection(monkeypatch, 0.15)

    async def worker():
        async with p.connection():
            await asleep(0.002)

    async with pool.AsyncConnectionPool(dsn, min_size=5, max_size=10) as p:
        await p.wait()

        ts = [spawn(worker) for i in range(50)]
        await gather(*ts)
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
        async with pool.AsyncConnectionPool(dsn, min_size=4) as p:
            await p.wait(timeout=2)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


@skip_sync
async def test_cancellation_in_queue(dsn):
    # https://github.com/psycopg/psycopg/issues/509

    nconns = 3

    async with pool.AsyncConnectionPool(dsn, min_size=nconns, timeout=1) as p:
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
        assert stats["pool_available"] == 3
        assert stats.get("requests_waiting", 0) == 0

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)
