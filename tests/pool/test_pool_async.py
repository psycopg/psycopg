import sys
import asyncio
import logging
import weakref
from time import time
from collections import Counter

import pytest

import psycopg3
from psycopg3 import pool
from psycopg3.pq import TransactionStatus

create_task = (
    asyncio.create_task
    if sys.version_info >= (3, 7)
    else asyncio.ensure_future
)

pytestmark = pytest.mark.asyncio


async def test_defaults(dsn):
    p = pool.AsyncConnectionPool(dsn)
    assert p.minconn == p.maxconn == 4
    assert p.timeout == 30
    assert p.max_idle == 600
    assert p.num_workers == 3
    await p.close()


async def test_minconn_maxconn(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2)
    assert p.minconn == p.maxconn == 2
    await p.close()

    p = pool.AsyncConnectionPool(dsn, minconn=2, maxconn=4)
    assert p.minconn == 2
    assert p.maxconn == 4
    await p.close()

    with pytest.raises(ValueError):
        pool.AsyncConnectionPool(dsn, minconn=4, maxconn=2)


async def test_kwargs(dsn):
    p = pool.AsyncConnectionPool(dsn, kwargs={"autocommit": True}, minconn=1)
    async with p.connection() as conn:
        assert conn.autocommit

    await p.close()


async def test_its_really_a_pool(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2)
    async with p.connection() as conn:
        cur = await conn.execute("select pg_backend_pid()")
        (pid1,) = await cur.fetchone()

        async with p.connection() as conn2:
            cur = await conn2.execute("select pg_backend_pid()")
            (pid2,) = await cur.fetchone()

    async with p.connection() as conn:
        assert conn.pgconn.backend_pid in (pid1, pid2)

    await p.close()


async def test_context(dsn):
    async with pool.AsyncConnectionPool(dsn, minconn=1) as p:
        assert not p.closed
    assert p.closed


async def test_connection_not_lost(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
    with pytest.raises(ZeroDivisionError):
        async with p.connection() as conn:
            pid = conn.pgconn.backend_pid
            1 / 0

    async with p.connection() as conn2:
        assert conn2.pgconn.backend_pid == pid

    await p.close()


@pytest.mark.slow
async def test_concurrent_filling(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    t0 = time()
    times = []

    add_orig = pool.AsyncConnectionPool._add_to_pool

    async def add_time(self, conn):
        times.append(time() - t0)
        await add_orig(self, conn)

    monkeypatch.setattr(pool.AsyncConnectionPool, "_add_to_pool", add_time)

    p = pool.AsyncConnectionPool(dsn, minconn=5, num_workers=2)
    await p.wait_ready(5.0)
    want_times = [0.1, 0.1, 0.2, 0.2, 0.3]
    assert len(times) == len(want_times)
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times
    await p.close()


@pytest.mark.slow
async def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        p = pool.AsyncConnectionPool(dsn, minconn=4, num_workers=1)
        await p.wait_ready(0.3)

    p = pool.AsyncConnectionPool(dsn, minconn=4, num_workers=1)
    await p.wait_ready(0.5)
    await p.close()
    p = pool.AsyncConnectionPool(dsn, minconn=4, num_workers=2)
    await p.wait_ready(0.3)
    await p.wait_ready(0.0001)  # idempotent
    await p.close()


@pytest.mark.slow
async def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(pool.PoolTimeout):
        p = pool.AsyncConnectionPool(
            proxy.client_dsn, minconn=1, num_workers=1
        )
        await p.wait_ready(0.2)

    p = pool.AsyncConnectionPool(proxy.client_dsn, minconn=1, num_workers=1)
    await asyncio.sleep(0.5)
    assert not p._pool
    proxy.start()

    async with p.connection() as conn:
        await conn.execute("select 1")

    await p.close()


@pytest.mark.slow
async def test_queue(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2)
    results = []

    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            cur = await conn.execute(
                "select pg_backend_pid() from pg_sleep(0.2)"
            )
            (pid,) = await cur.fetchone()
        t1 = time()
        results.append((n, t1 - t0, pid))

    ts = [create_task(worker(i)) for i in range(6)]
    await asyncio.gather(*ts)
    await p.close()

    times = [item[1] for item in results]
    want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times

    assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
async def test_queue_timeout(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2, timeout=0.1)
    results = []
    errors = []

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

    ts = [create_task(worker(i)) for i in range(4)]
    await asyncio.gather(*ts)

    assert len(results) == 2
    assert len(errors) == 2
    for e in errors:
        assert 0.1 < e[1] < 0.15

    await p.close()


@pytest.mark.slow
async def test_dead_client(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2)

    results = []

    async def worker(i, timeout):
        try:
            async with p.connection(timeout=timeout) as conn:
                await conn.execute("select pg_sleep(0.3)")
                results.append(i)
        except pool.PoolTimeout:
            if timeout > 0.2:
                raise

    ts = [
        create_task(worker(i, timeout))
        for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4])
    ]
    await asyncio.gather(*ts)

    await asyncio.sleep(0.2)
    assert set(results) == set([0, 1, 3, 4])
    assert len(p._pool) == 2  # no connection was lost
    await p.close()


@pytest.mark.slow
async def test_queue_timeout_override(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2, timeout=0.1)
    results = []
    errors = []

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

    ts = [create_task(worker(i)) for i in range(4)]
    await asyncio.gather(*ts)
    await p.close()

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


async def test_broken_reconnect(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
    with pytest.raises(psycopg3.OperationalError):
        async with p.connection() as conn:
            cur = await conn.execute("select pg_backend_pid()")
            (pid1,) = await cur.fetchone()
            await conn.close()

    async with p.connection() as conn2:
        cur = await conn2.execute("select pg_backend_pid()")
        (pid2,) = await cur.fetchone()

    await p.close()
    assert pid1 != pid2


async def test_intrans_rollback(dsn, caplog):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
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

    await p.close()
    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 1
    assert "INTRANS" in recs[0].message


async def test_inerror_rollback(dsn, caplog):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
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

    await p.close()


async def test_active_close(dsn, caplog):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
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

    await p.close()
    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 2
    assert "ACTIVE" in recs[0].message
    assert "BAD" in recs[1].message


async def test_fail_rollback_close(dsn, caplog, monkeypatch):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
    conn = await p.getconn()

    # Make the rollback fail
    orig_rollback = conn.rollback

    async def bad_rollback():
        conn.pgconn.finish()
        await orig_rollback()

    monkeypatch.setattr(conn, "rollback", bad_rollback)

    pid = conn.pgconn.backend_pid
    with pytest.raises(psycopg3.ProgrammingError):
        await conn.execute("wat")
    assert conn.pgconn.transaction_status == TransactionStatus.INERROR
    await p.putconn(conn)

    async with p.connection() as conn2:
        assert conn2.pgconn.backend_pid != pid
        assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    await p.close()

    recs = [
        r
        for r in caplog.records
        if r.name.startswith("psycopg3") and r.levelno >= logging.WARNING
    ]
    assert len(recs) == 3
    assert "INERROR" in recs[0].message
    assert "OperationalError" in recs[1].message
    assert "BAD" in recs[2].message


async def test_close_no_threads(dsn):
    p = pool.AsyncConnectionPool(dsn)
    assert p._sched_runner.is_alive()
    for t in p._workers:
        assert t.is_alive()

    await p.close()
    assert not p._sched_runner.is_alive()
    for t in p._workers:
        assert not t.is_alive()


async def test_putconn_no_pool(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=1)
    conn = psycopg3.connect(dsn)
    with pytest.raises(ValueError):
        await p.putconn(conn)
    await p.close()


async def test_putconn_wrong_pool(dsn):
    p1 = pool.AsyncConnectionPool(dsn, minconn=1)
    p2 = pool.AsyncConnectionPool(dsn, minconn=1)
    conn = await p1.getconn()
    with pytest.raises(ValueError):
        await p2.putconn(conn)
    await p1.close()
    await p2.close()


async def test_del_no_warning(dsn, recwarn):
    p = pool.AsyncConnectionPool(dsn, minconn=2)
    async with p.connection() as conn:
        await conn.execute("select 1")

    await p.wait_ready()
    ref = weakref.ref(p)
    del p
    await asyncio.sleep(0.1)  # TODO: I wish it wasn't needed
    assert not ref()
    assert not recwarn


@pytest.mark.slow
async def test_del_stop_threads(dsn):
    p = pool.AsyncConnectionPool(dsn)
    ts = [p._sched_runner] + p._workers
    del p
    await asyncio.sleep(0.1)
    for t in ts:
        assert not t.is_alive()


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
async def test_grow(dsn, monkeypatch):
    p = pool.AsyncConnectionPool(dsn, minconn=2, maxconn=4, num_workers=3)
    await p.wait_ready(5.0)
    delay_connection(monkeypatch, 0.1)
    ts = []
    results = []

    async def worker(n):
        t0 = time()
        async with p.connection() as conn:
            await conn.execute("select 1 from pg_sleep(0.2)")
        t1 = time()
        results.append((n, t1 - t0))

    ts = [create_task(worker(i)) for i in range(6)]
    await asyncio.gather(*ts)
    await p.close()

    want_times = [0.2, 0.2, 0.3, 0.3, 0.4, 0.4]
    times = [item[1] for item in results]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times


@pytest.mark.slow
async def test_shrink(dsn, monkeypatch):

    from psycopg3.pool.tasks import ShrinkPool

    orig_run = ShrinkPool._run_async
    results = []

    async def run_async_hacked(self, pool):
        n0 = pool._nconns
        await orig_run(self, pool)
        n1 = pool._nconns
        results.append((n0, n1))

    monkeypatch.setattr(ShrinkPool, "_run_async", run_async_hacked)

    p = pool.AsyncConnectionPool(dsn, minconn=2, maxconn=4, max_idle=0.2)
    await p.wait_ready(5.0)
    assert p.max_idle == 0.2

    async def worker(n):
        async with p.connection() as conn:
            await conn.execute("select pg_sleep(0.1)")

    ts = [create_task(worker(i)) for i in range(4)]
    await asyncio.gather(*ts)

    await asyncio.sleep(1)
    await p.close()
    assert results == [(4, 4), (4, 3), (3, 2), (2, 2), (2, 2)]


@pytest.mark.slow
async def test_reconnect(proxy, caplog, monkeypatch):
    assert pool.base.ConnectionAttempt.INITIAL_DELAY == 1.0
    assert pool.base.ConnectionAttempt.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.base.ConnectionAttempt, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.base.ConnectionAttempt, "DELAY_JITTER", 0.0)

    proxy.start()
    p = pool.AsyncConnectionPool(proxy.client_dsn, minconn=1)
    await p.wait_ready(2.0)
    proxy.stop()

    with pytest.raises(psycopg3.OperationalError):
        async with p.connection() as conn:
            await conn.execute("select 1")

    await asyncio.sleep(1.0)
    proxy.start()
    await p.wait_ready()

    async with p.connection() as conn:
        await conn.execute("select 1")

    await p.close()

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

    p = pool.AsyncConnectionPool(
        proxy.client_dsn,
        name="this-one",
        minconn=1,
        reconnect_timeout=1.0,
        reconnect_failed=failed,
    )
    await p.wait_ready(2.0)
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
    await p.close()


@pytest.mark.slow
async def test_uniform_use(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=4)
    counts = Counter()
    for i in range(8):
        async with p.connection() as conn:
            await asyncio.sleep(0.1)
            counts[id(conn)] += 1

    await p.close()
    assert len(counts) == 4
    assert set(counts.values()) == set([2])


@pytest.mark.slow
async def test_resize(dsn):
    p = pool.AsyncConnectionPool(dsn, minconn=2, max_idle=0.2)
    size = []

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
    await p.close()
    await asyncio.gather(s, c)

    assert size == [2, 1, 3, 4, 3, 2, 2]


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """
    connect_orig = psycopg3.AsyncConnection.connect

    async def connect_delay(*args, **kwargs):
        t0 = time()
        rv = await connect_orig(*args, **kwargs)
        t1 = time()
        await asyncio.sleep(sec - (t1 - t0))
        return rv

    monkeypatch.setattr(psycopg3.AsyncConnection, "connect", connect_delay)
