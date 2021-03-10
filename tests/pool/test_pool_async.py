import sys
import asyncio
import logging
from time import time
from collections import Counter

import pytest

import psycopg3
from psycopg3 import pool
from psycopg3.pq import TransactionStatus
from psycopg3.utils.compat import create_task

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        sys.version_info < (3, 7),
        reason="async pool not supported before Python 3.7",
    ),
]


async def test_defaults(dsn):
    async with pool.AsyncConnectionPool(dsn) as p:
        assert p.minconn == p.maxconn == 4
        assert p.timeout == 30
        assert p.max_idle == 10 * 60
        assert p.max_lifetime == 60 * 60
        assert p.num_workers == 3


async def test_minconn_maxconn(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=2) as p:
        assert p.minconn == p.maxconn == 2

    async with pool.AsyncConnectionPool(dsn, minconn=2, maxconn=4) as p:
        assert p.minconn == 2
        assert p.maxconn == 4

    with pytest.raises(ValueError):
        pool.AsyncConnectionPool(dsn, minconn=4, maxconn=2)


async def test_connection_class(dsn):
    class MyConn(psycopg3.AsyncConnection):
        pass

    async with pool.AsyncConnectionPool(
        dsn, connection_class=MyConn, minconn=1
    ) as p:
        async with p.connection() as conn:
            assert isinstance(conn, MyConn)


async def test_kwargs(dsn):
    async with pool.AsyncConnectionPool(
        dsn, kwargs={"autocommit": True}, minconn=1
    ) as p:
        async with p.connection() as conn:
            assert conn.autocommit


async def test_its_really_a_pool(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=2) as p:
        async with p.connection() as conn:
            cur = await conn.execute("select pg_backend_pid()")
            (pid1,) = await cur.fetchone()

            async with p.connection() as conn2:
                cur = await conn2.execute("select pg_backend_pid()")
                (pid2,) = await cur.fetchone()

        async with p.connection() as conn:
            assert conn.pgconn.backend_pid in (pid1, pid2)


async def test_context(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        assert not p.closed
    assert p.closed


async def test_connection_not_lost(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        with pytest.raises(ZeroDivisionError):
            async with p.connection() as conn:
                pid = conn.pgconn.backend_pid
                1 / 0

        async with p.connection() as conn2:
            assert conn2.pgconn.backend_pid == pid


@pytest.mark.slow
async def test_concurrent_filling(dsn, monkeypatch, retries):
    delay_connection(monkeypatch, 0.1)

    async def add_time(self, conn):
        times.append(time() - t0)
        await add_orig(self, conn)

    add_orig = pool.AsyncConnectionPool._add_to_pool
    monkeypatch.setattr(pool.AsyncConnectionPool, "_add_to_pool", add_time)

    async for retry in retries:
        with retry:
            times = []
            t0 = time()

            async with pool.AsyncConnectionPool(
                dsn, minconn=5, num_workers=2
            ) as p:
                await p.wait(1.0)
                want_times = [0.1, 0.1, 0.2, 0.2, 0.3]
                assert len(times) == len(want_times)
                for got, want in zip(times, want_times):
                    assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
async def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        async with pool.AsyncConnectionPool(
            dsn, minconn=4, num_workers=1
        ) as p:
            await p.wait(0.3)

    async with pool.AsyncConnectionPool(dsn, minconn=4, num_workers=1) as p:
        await p.wait(0.5)

    async with pool.AsyncConnectionPool(dsn, minconn=4, num_workers=2) as p:
        await p.wait(0.3)
        await p.wait(0.0001)  # idempotent


@pytest.mark.slow
async def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(pool.PoolTimeout):
        async with pool.AsyncConnectionPool(
            proxy.client_dsn, minconn=1, num_workers=1
        ) as p:
            await p.wait(0.2)

    async with pool.AsyncConnectionPool(
        proxy.client_dsn, minconn=1, num_workers=1
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

    async with pool.AsyncConnectionPool(minconn=1, configure=configure) as p:
        await p.wait(timeout=1.0)
        async with p.connection() as conn:
            assert inits == 1
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"

        async with p.connection() as conn:
            assert inits == 1
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"
            await conn.close()

        async with p.connection() as conn:
            assert inits == 2
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone())[0] == "on"


@pytest.mark.slow
async def test_configure_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")

    async def configure(conn):
        await conn.execute("select 1")

    async with pool.AsyncConnectionPool(minconn=1, configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            await p.wait(timeout=0.5)

    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.slow
async def test_configure_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")

    async def configure(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    async with pool.AsyncConnectionPool(minconn=1, configure=configure) as p:
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

    async with pool.AsyncConnectionPool(minconn=1, reset=reset) as p:
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


async def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")

    async def reset(conn):
        await conn.execute("reset all")

    async with pool.AsyncConnectionPool(minconn=1, reset=reset) as p:
        async with p.connection() as conn:
            await conn.execute("select 1")
            pid1 = conn.pgconn.backend_pid

        async with p.connection() as conn:
            await conn.execute("select 1")
            pid2 = conn.pgconn.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


async def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")

    async def reset(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    async with pool.AsyncConnectionPool(minconn=1, reset=reset) as p:
        async with p.connection() as conn:
            await conn.execute("select 1")
            pid1 = conn.pgconn.backend_pid

        async with p.connection() as conn:
            await conn.execute("select 1")
            pid2 = conn.pgconn.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.slow
async def test_queue(dsn, retries):
    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            cur = await conn.execute(
                "select pg_backend_pid() from pg_sleep(0.2)"
            )
            (pid,) = await cur.fetchone()
        t1 = time()
        results.append((n, t1 - t0, pid))

    async for retry in retries:
        with retry:
            results = []
            async with pool.AsyncConnectionPool(dsn, minconn=2) as p:
                ts = [create_task(worker(i)) for i in range(6)]
                await asyncio.gather(*ts)

            times = [item[1] for item in results]
            want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
            for got, want in zip(times, want_times):
                assert got == pytest.approx(want, 0.1), times

            assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
async def test_queue_timeout(dsn):
    async def worker(n):
        t0 = time()
        try:
            async with p.connection() as conn:
                cur = await conn.execute(
                    "select pg_backend_pid() from pg_sleep(0.2)"
                )
                (pid,) = await cur.fetchone()
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results = []
    errors = []

    async with pool.AsyncConnectionPool(dsn, minconn=2, timeout=0.1) as p:
        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(results) == 2
    assert len(errors) == 2
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.slow
async def test_dead_client(dsn):
    async def worker(i, timeout):
        try:
            async with p.connection(timeout=timeout) as conn:
                await conn.execute("select pg_sleep(0.3)")
                results.append(i)
        except pool.PoolTimeout:
            if timeout > 0.2:
                raise

    async with pool.AsyncConnectionPool(dsn, minconn=2) as p:
        results = []
        ts = [
            create_task(worker(i, timeout))
            for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4])
        ]
        await asyncio.gather(*ts)

        await asyncio.sleep(0.2)
        assert set(results) == set([0, 1, 3, 4])
        assert len(p._pool) == 2  # no connection was lost


@pytest.mark.slow
async def test_queue_timeout_override(dsn):
    async def worker(n):
        t0 = time()
        timeout = 0.25 if n == 3 else None
        try:
            async with p.connection(timeout=timeout) as conn:
                cur = await conn.execute(
                    "select pg_backend_pid() from pg_sleep(0.2)"
                )
                (pid,) = await cur.fetchone()
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results = []
    errors = []

    async with pool.AsyncConnectionPool(dsn, minconn=2, timeout=0.1) as p:
        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


async def test_broken_reconnect(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        async with p.connection() as conn:
            cur = await conn.execute("select pg_backend_pid()")
            (pid1,) = await cur.fetchone()
            await conn.close()

        async with p.connection() as conn2:
            cur = await conn2.execute("select pg_backend_pid()")
            (pid2,) = await cur.fetchone()

    assert pid1 != pid2


async def test_intrans_rollback(dsn, caplog):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        conn = await p.getconn()
        pid = conn.pgconn.backend_pid
        await conn.execute("create table test_intrans_rollback ()")
        assert conn.pgconn.transaction_status == TransactionStatus.INTRANS
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.pgconn.backend_pid == pid
            assert conn2.pgconn.transaction_status == TransactionStatus.IDLE
            cur = await conn.execute(
                "select 1 from pg_class where relname = 'test_intrans_rollback'"
            )
            assert not await cur.fetchone()

    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 1
    assert "INTRANS" in recs[0].message


async def test_inerror_rollback(dsn, caplog):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        conn = await p.getconn()
        pid = conn.pgconn.backend_pid
        with pytest.raises(psycopg3.ProgrammingError):
            await conn.execute("wat")
        assert conn.pgconn.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.pgconn.backend_pid == pid
            assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 1
    assert "INERROR" in recs[0].message


async def test_active_close(dsn, caplog):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        conn = await p.getconn()
        pid = conn.pgconn.backend_pid
        cur = conn.cursor()
        async with cur.copy(
            "copy (select * from generate_series(1, 10)) to stdout"
        ):
            pass
        assert conn.pgconn.transaction_status == TransactionStatus.ACTIVE
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.pgconn.backend_pid != pid
            assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 2
    assert "ACTIVE" in recs[0].message
    assert "BAD" in recs[1].message


async def test_fail_rollback_close(dsn, caplog, monkeypatch):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        conn = await p.getconn()

        async def bad_rollback():
            conn.pgconn.finish()
            await orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        pid = conn.pgconn.backend_pid
        with pytest.raises(psycopg3.ProgrammingError):
            await conn.execute("wat")
        assert conn.pgconn.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)

        async with p.connection() as conn2:
            assert conn2.pgconn.backend_pid != pid
            assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 3
    assert "INERROR" in recs[0].message
    assert "OperationalError" in recs[1].message
    assert "BAD" in recs[2].message


async def test_close_no_tasks(dsn):
    p = pool.AsyncConnectionPool(dsn)
    assert not p._sched_runner.done()
    for t in p._workers:
        assert not t.done()

    await p.close()
    assert p._sched_runner.done()
    for t in p._workers:
        assert t.done()


async def test_putconn_no_pool(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        conn = psycopg3.connect(dsn)
        with pytest.raises(ValueError):
            await p.putconn(conn)


async def test_putconn_wrong_pool(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p1:
        async with pool.AsyncConnectionPool(dsn, minconn=1) as p2:
            conn = await p1.getconn()
            with pytest.raises(ValueError):
                await p2.putconn(conn)


async def test_closed_getconn(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
    assert not p.closed
    async with p.connection():
        pass

    await p.close()
    assert p.closed

    with pytest.raises(pool.PoolClosed):
        async with p.connection():
            pass


async def test_closed_putconn(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=1)

    async with p.connection() as conn:
        pass
    assert not conn.closed

    async with p.connection() as conn:
        await p.close()
    assert conn.closed


@pytest.mark.slow
async def test_closed_queue(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
    success = []

    async def w1():
        async with p.connection() as conn:
            res = await conn.execute("select 1 from pg_sleep(0.2)")
            assert await res.fetchone() == (1,)
        success.append("w1")

    async def w2():
        with pytest.raises(pool.PoolClosed):
            async with p.connection():
                pass
        success.append("w2")

    t1 = create_task(w1())
    await asyncio.sleep(0.1)
    t2 = create_task(w2())
    await p.close()
    await asyncio.gather(t1, t2)
    assert len(success) == 2


@pytest.mark.slow
async def test_grow(dsn, monkeypatch, retries):
    delay_connection(monkeypatch, 0.1)

    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            await conn.execute("select 1 from pg_sleep(0.2)")
        t1 = time()
        results.append((n, t1 - t0))

    async for retry in retries:
        with retry:
            async with pool.AsyncConnectionPool(
                dsn, minconn=2, maxconn=4, num_workers=3
            ) as p:
                await p.wait(1.0)
                ts = []
                results = []

                ts = [create_task(worker(i)) for i in range(6)]
                await asyncio.gather(*ts)

            want_times = [0.2, 0.2, 0.3, 0.3, 0.4, 0.4]
            times = [item[1] for item in results]
            for got, want in zip(times, want_times):
                assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
async def test_shrink(dsn, monkeypatch):

    from psycopg3.pool.async_pool import ShrinkPool

    results = []

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

    async with pool.AsyncConnectionPool(
        dsn, minconn=2, maxconn=4, max_idle=0.2
    ) as p:
        await p.wait(5.0)
        assert p.max_idle == 0.2

        ts = [create_task(worker(i)) for i in range(4)]
        await asyncio.gather(*ts)

        await asyncio.sleep(1)

    assert results == [(4, 4), (4, 3), (3, 2), (2, 2), (2, 2)]


@pytest.mark.slow
async def test_reconnect(proxy, caplog, monkeypatch):
    assert pool.base.ConnectionAttempt.INITIAL_DELAY == 1.0
    assert pool.base.ConnectionAttempt.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.base.ConnectionAttempt, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.base.ConnectionAttempt, "DELAY_JITTER", 0.0)

    proxy.start()
    async with pool.AsyncConnectionPool(proxy.client_dsn, minconn=1) as p:
        await p.wait(2.0)
        proxy.stop()

        with pytest.raises(psycopg3.OperationalError):
            async with p.connection() as conn:
                await conn.execute("select 1")

        await asyncio.sleep(1.0)
        proxy.start()
        await p.wait()

        async with p.connection() as conn:
            await conn.execute("select 1")

    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert "BAD" in recs[0].message
    times = [rec.created for rec in recs]
    assert times[1] - times[0] < 0.05
    deltas = [times[i + 1] - times[i] for i in range(1, len(times) - 1)]
    assert len(deltas) == 3
    want = 0.1
    for delta in deltas:
        assert delta == pytest.approx(want, 0.05), deltas
        want *= 2


@pytest.mark.slow
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
        minconn=1,
        reconnect_timeout=1.0,
        reconnect_failed=failed,
    ) as p:
        await p.wait(2.0)
        proxy.stop()

        with pytest.raises(psycopg3.OperationalError):
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
async def test_uniform_use(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=4) as p:
        counts = Counter()
        for i in range(8):
            async with p.connection() as conn:
                await asyncio.sleep(0.1)
                counts[id(conn)] += 1

    assert len(counts) == 4
    assert set(counts.values()) == set([2])


@pytest.mark.slow
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

    size = []

    async with pool.AsyncConnectionPool(dsn, minconn=2, max_idle=0.2) as p:
        s = create_task(sampler())

        await asyncio.sleep(0.3)

        c = create_task(client(0.4))

        await asyncio.sleep(0.2)
        await p.resize(4)
        assert p.minconn == 4
        assert p.maxconn == 4

        await asyncio.sleep(0.4)
        await p.resize(2)
        assert p.minconn == 2
        assert p.maxconn == 2

        await asyncio.sleep(0.6)

    await asyncio.gather(s, c)
    assert size == [2, 1, 3, 4, 3, 2, 2]


def test_jitter():
    rnds = [
        pool.AsyncConnectionPool._jitter(30, -0.1, +0.2) for i in range(100)
    ]
    rnds.sort()
    assert 27 <= min(rnds) <= 28
    assert 35 < max(rnds) < 36


@pytest.mark.slow
async def test_max_lifetime(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1, max_lifetime=0.2) as p:
        await asyncio.sleep(0.1)
        pids = []
        for i in range(5):
            async with p.connection() as conn:
                pids.append(conn.pgconn.backend_pid)
            await asyncio.sleep(0.2)

    assert pids[0] == pids[1] != pids[4], pids


async def test_check(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")
    async with pool.AsyncConnectionPool(dsn, minconn=4) as p:
        await p.wait(1.0)
        async with p.connection() as conn:
            pid = conn.pgconn.backend_pid

        await p.wait(1.0)
        pids = set(conn.pgconn.backend_pid for conn in p._pool)
        assert pid in pids
        await conn.close()

        assert len(caplog.records) == 0
        await p.check()
        assert len(caplog.records) == 1
        await p.wait(1.0)
        pids2 = set(conn.pgconn.backend_pid for conn in p._pool)
        assert len(pids & pids2) == 3
        assert pid not in pids2


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """

    async def connect_delay(*args, **kwargs):
        t0 = time()
        rv = await connect_orig(*args, **kwargs)
        t1 = time()
        await asyncio.sleep(sec - (t1 - t0))
        return rv

    connect_orig = psycopg3.AsyncConnection.connect
    monkeypatch.setattr(psycopg3.AsyncConnection, "connect", connect_delay)
