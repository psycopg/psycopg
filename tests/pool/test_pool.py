# WARNING: this file is auto-generated by 'async_to_sync.py'
# from the original file 'test_pool_async.py'
# DO NOT CHANGE! Change the original file instead.
import logging
import weakref
from time import time
from typing import Any, Dict, List, Tuple

import pytest

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import class_row, Row, TupleRow

from ..utils import assert_type, Counter, set_autocommit
from ..acompat import Event, spawn, gather, sleep, skip_sync
from .test_pool_common import delay_connection

try:
    import psycopg_pool as pool
except ImportError:
    # Tests should have been skipped if the package is not available
    pass


def test_default_sizes(dsn):
    with pool.ConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 4


@pytest.mark.parametrize("min_size, max_size", [(2, None), (0, 2), (2, 4)])
def test_min_size_max_size(dsn, min_size, max_size):
    with pool.ConnectionPool(dsn, min_size=min_size, max_size=max_size) as p:
        assert p.min_size == min_size
        assert p.max_size == max_size if max_size is not None else min_size


@pytest.mark.parametrize("min_size, max_size", [(0, 0), (0, None), (-1, None), (4, 2)])
def test_bad_size(dsn, min_size, max_size):
    with pytest.raises(ValueError):
        pool.ConnectionPool(min_size=min_size, max_size=max_size)


class MyRow(Dict[str, Any]):
    ...


def test_generic_connection_type(dsn):
    def configure(conn: psycopg.Connection[Any]) -> None:
        set_autocommit(conn, True)

    class MyConnection(psycopg.Connection[Row]):
        pass

    with pool.ConnectionPool(
        dsn,
        connection_class=MyConnection[MyRow],
        kwargs=dict(row_factory=class_row(MyRow)),
        configure=configure,
    ) as p1:
        with p1.connection() as conn1:
            cur1 = conn1.execute("select 1 as x")
            (row1,) = cur1.fetchall()
    assert_type(p1, pool.ConnectionPool[MyConnection[MyRow]])
    assert_type(conn1, MyConnection[MyRow])
    assert_type(row1, MyRow)
    assert conn1.autocommit
    assert row1 == {"x": 1}

    with pool.ConnectionPool(dsn, connection_class=MyConnection[TupleRow]) as p2:
        with p2.connection() as conn2:
            cur2 = conn2.execute("select 2 as y")
            (row2,) = cur2.fetchall()
    assert_type(p2, pool.ConnectionPool[MyConnection[TupleRow]])
    assert_type(conn2, MyConnection[TupleRow])
    assert_type(row2, TupleRow)
    assert row2 == (2,)


def test_non_generic_connection_type(dsn):
    def configure(conn: psycopg.Connection[Any]) -> None:
        set_autocommit(conn, True)

    class MyConnection(psycopg.Connection[MyRow]):
        def __init__(self, *args: Any, **kwargs: Any):
            kwargs["row_factory"] = class_row(MyRow)
            super().__init__(*args, **kwargs)

    with pool.ConnectionPool(
        dsn, connection_class=MyConnection, configure=configure
    ) as p1:
        with p1.connection() as conn1:
            cur1 = conn1.execute("select 1 as x")
            (row1,) = cur1.fetchall()
    assert_type(p1, pool.ConnectionPool[MyConnection])
    assert_type(conn1, MyConnection)
    assert_type(row1, MyRow)
    assert conn1.autocommit
    assert row1 == {"x": 1}


@pytest.mark.crdb_skip("backend pid")
def test_its_really_a_pool(dsn):
    with pool.ConnectionPool(dsn, min_size=2) as p:
        with p.connection() as conn:
            pid1 = conn.info.backend_pid

            with p.connection() as conn2:
                pid2 = conn2.info.backend_pid

        with p.connection() as conn:
            assert conn.info.backend_pid in (pid1, pid2)


@pytest.mark.crdb_skip("backend pid")
def test_connection_not_lost(dsn):
    with pool.ConnectionPool(dsn, min_size=1) as p:
        with pytest.raises(ZeroDivisionError):
            with p.connection() as conn:
                pid = conn.info.backend_pid
                1 / 0

        with p.connection() as conn2:
            assert conn2.info.backend_pid == pid


@pytest.mark.slow
@pytest.mark.timing
def test_concurrent_filling(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)

    def add_time(self, conn):
        times.append(time() - t0)
        add_orig(self, conn)

    add_orig = pool.ConnectionPool._add_to_pool
    monkeypatch.setattr(pool.ConnectionPool, "_add_to_pool", add_time)

    times: List[float] = []
    t0 = time()

    with pool.ConnectionPool(dsn, min_size=5, num_workers=2) as p:
        p.wait(1.0)
        want_times = [0.1, 0.1, 0.2, 0.2, 0.3]
        assert len(times) == len(want_times)
        for got, want in zip(times, want_times):
            assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
@pytest.mark.timing
def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        with pool.ConnectionPool(dsn, min_size=4, num_workers=1) as p:
            p.wait(0.3)

    with pool.ConnectionPool(dsn, min_size=4, num_workers=1) as p:
        p.wait(0.5)

    with pool.ConnectionPool(dsn, min_size=4, num_workers=2) as p:
        p.wait(0.3)
        p.wait(0.0001)  # idempotent


def test_configure(dsn):
    inits = 0

    def configure(conn):
        nonlocal inits
        inits += 1
        with conn.transaction():
            conn.execute("set default_transaction_read_only to on")

    with pool.ConnectionPool(dsn, min_size=1, configure=configure) as p:
        p.wait(timeout=1.0)
        with p.connection() as conn:
            assert inits == 1
            res = conn.execute("show default_transaction_read_only")
            assert res.fetchone()[0] == "on"

        with p.connection() as conn:
            assert inits == 1
            res = conn.execute("show default_transaction_read_only")
            assert res.fetchone()[0] == "on"
            conn.close()

        with p.connection() as conn:
            assert inits == 2
            res = conn.execute("show default_transaction_read_only")
            assert res.fetchone()[0] == "on"


def test_reset(dsn):
    resets = 0

    def setup(conn):
        with conn.transaction():
            conn.execute("set timezone to '+1:00'")

    def reset(conn):
        nonlocal resets
        resets += 1
        with conn.transaction():
            conn.execute("set timezone to utc")

    with pool.ConnectionPool(dsn, min_size=1, reset=reset) as p:
        with p.connection() as conn:
            assert resets == 0
            conn.execute("set timezone to '+2:00'")

        p.wait()
        assert resets == 1

        with p.connection() as conn:
            cur = conn.execute("show timezone")
            assert cur.fetchone() == ("UTC",)

        p.wait()
        assert resets == 2


@pytest.mark.crdb_skip("backend pid")
def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def reset(conn):
        conn.execute("reset all")

    with pool.ConnectionPool(dsn, min_size=1, reset=reset) as p:
        with p.connection() as conn:
            conn.execute("select 1")
            pid1 = conn.info.backend_pid

        with p.connection() as conn:
            conn.execute("select 1")
            pid2 = conn.info.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def reset(conn):
        with conn.transaction():
            conn.execute("WAT")

    with pool.ConnectionPool(dsn, min_size=1, reset=reset) as p:
        with p.connection() as conn:
            conn.execute("select 1")
            pid1 = conn.info.backend_pid

        with p.connection() as conn:
            conn.execute("select 1")
            pid2 = conn.info.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = p.getconn()
        pid = conn.info.backend_pid
        conn.execute("create table test_intrans_rollback ()")
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        p.putconn(conn)

        with p.connection() as conn2:
            assert conn2.info.backend_pid == pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE
            cur = conn2.execute(
                "select 1 from pg_class where relname = 'test_intrans_rollback'"
            )
            assert not cur.fetchone()

    assert len(caplog.records) == 1
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
def test_inerror_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = p.getconn()
        pid = conn.info.backend_pid
        with pytest.raises(psycopg.ProgrammingError):
            conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        p.putconn(conn)

        with p.connection() as conn2:
            assert conn2.info.backend_pid == pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
@pytest.mark.crdb_skip("copy")
def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = p.getconn()
        pid = conn.info.backend_pid
        conn.pgconn.exec_(b"copy (select * from generate_series(1, 10)) to stdout")
        assert conn.info.transaction_status == TransactionStatus.ACTIVE
        p.putconn(conn)

        with p.connection() as conn2:
            assert conn2.info.backend_pid != pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 2
    assert "ACTIVE" in caplog.records[0].message
    assert "BAD" in caplog.records[1].message


@pytest.mark.crdb_skip("backend pid")
def test_fail_rollback_close(dsn, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = p.getconn()

        def bad_rollback():
            conn.pgconn.finish()
            orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        pid = conn.info.backend_pid
        with pytest.raises(psycopg.ProgrammingError):
            conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        p.putconn(conn)

        with p.connection() as conn2:
            assert conn2.info.backend_pid != pid
            assert conn2.info.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 3
    assert "INERROR" in caplog.records[0].message
    assert "OperationalError" in caplog.records[1].message
    assert "BAD" in caplog.records[2].message


def test_del_no_warning(dsn, recwarn):
    p = pool.ConnectionPool(dsn, min_size=2, open=False)
    p.open()
    with p.connection() as conn:
        conn.execute("select 1")

    p.wait()
    ref = weakref.ref(p)
    del p
    assert not ref()
    assert not recwarn, [str(w.message) for w in recwarn.list]


def test_closed_putconn(dsn):
    with pool.ConnectionPool(dsn, min_size=1) as p:
        with p.connection() as conn:
            pass
        assert not conn.closed


@pytest.mark.slow
@pytest.mark.timing
def test_open_wait(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        p = pool.ConnectionPool(dsn, min_size=4, num_workers=1, open=False)
        try:
            p.open(wait=True, timeout=0.3)
        finally:
            p.close()

    p = pool.ConnectionPool(dsn, min_size=4, num_workers=1, open=False)
    try:
        p.open(wait=True, timeout=0.5)
    finally:
        p.close()


@pytest.mark.slow
@pytest.mark.timing
def test_open_as_wait(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        with pool.ConnectionPool(dsn, min_size=4, num_workers=1) as p:
            p.open(wait=True, timeout=0.3)

    with pool.ConnectionPool(dsn, min_size=4, num_workers=1) as p:
        p.open(wait=True, timeout=0.5)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.parametrize(
    "min_size, want_times",
    [
        (2, [0.25, 0.25, 0.35, 0.45, 0.5, 0.5, 0.6, 0.7]),
        (0, [0.35, 0.45, 0.55, 0.6, 0.65, 0.7, 0.8, 0.85]),
    ],
)
def test_grow(dsn, monkeypatch, min_size, want_times):
    delay_connection(monkeypatch, 0.1)

    def worker(n):
        t0 = time()
        with p.connection() as conn:
            conn.execute("select 1 from pg_sleep(0.25)")
        t1 = time()
        results.append((n, t1 - t0))

    with pool.ConnectionPool(dsn, min_size=min_size, max_size=4, num_workers=3) as p:
        p.wait(1.0)
        results: List[Tuple[int, float]] = []
        ts = [spawn(worker, args=(i,)) for i in range(len(want_times))]
        gather(*ts)

    times = [item[1] for item in results]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
@pytest.mark.timing
def test_shrink(dsn, monkeypatch):
    from psycopg_pool.pool import ShrinkPool

    results: List[Tuple[int, int]] = []

    def run_hacked(self, pool):
        n0 = pool._nconns
        orig_run(self, pool)
        n1 = pool._nconns
        results.append((n0, n1))

    orig_run = ShrinkPool._run
    monkeypatch.setattr(ShrinkPool, "_run", run_hacked)

    def worker(n):
        with p.connection() as conn:
            conn.execute("select pg_sleep(0.1)")

    with pool.ConnectionPool(dsn, min_size=2, max_size=4, max_idle=0.2) as p:
        p.wait(5.0)
        assert p.max_idle == 0.2

        ts = [spawn(worker, args=(i,)) for i in range(4)]
        gather(*ts)

        sleep(1)

    assert results == [(4, 4), (4, 3), (3, 2), (2, 2), (2, 2)]


@pytest.mark.slow
@pytest.mark.timing
def test_reconnect(proxy, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    assert pool.base.ConnectionAttempt.INITIAL_DELAY == 1.0
    assert pool.base.ConnectionAttempt.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.base.ConnectionAttempt, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.base.ConnectionAttempt, "DELAY_JITTER", 0.0)

    caplog.clear()
    proxy.start()
    with pool.ConnectionPool(proxy.client_dsn, min_size=1) as p:
        p.wait(2.0)
        proxy.stop()

        with pytest.raises(psycopg.OperationalError):
            with p.connection() as conn:
                conn.execute("select 1")

        sleep(1.0)
        proxy.start()
        p.wait()

        with p.connection() as conn:
            conn.execute("select 1")

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
def test_reconnect_failure(proxy, async_cb):
    proxy.start()

    t1 = None

    if async_cb:

        def failed(pool):
            assert pool.name == "this-one"
            nonlocal t1
            t1 = time()

    else:

        def failed(pool):
            assert pool.name == "this-one"
            nonlocal t1
            t1 = time()

    with pool.ConnectionPool(
        proxy.client_dsn,
        name="this-one",
        min_size=1,
        reconnect_timeout=1.0,
        reconnect_failed=failed,
    ) as p:
        p.wait(2.0)
        proxy.stop()

        with pytest.raises(psycopg.OperationalError):
            with p.connection() as conn:
                conn.execute("select 1")

        t0 = time()
        sleep(1.5)
        assert t1
        assert t1 - t0 == pytest.approx(1.0, 0.1)
        assert p._nconns == 0

        proxy.start()
        t0 = time()
        with p.connection() as conn:
            conn.execute("select 1")
        t1 = time()
        assert t1 - t0 < 0.2


@pytest.mark.slow
def test_reconnect_after_grow_failed(proxy):
    # Retry reconnection after a failed connection attempt has put the pool
    # in grow mode. See issue #370.
    proxy.stop()

    ev = Event()

    def failed(pool):
        ev.set()

    with pool.ConnectionPool(
        proxy.client_dsn, min_size=4, reconnect_timeout=1.0, reconnect_failed=failed
    ) as p:
        ev.wait(2.0)

        with pytest.raises(pool.PoolTimeout):
            with p.connection(timeout=0.5) as conn:
                pass

        ev.clear()
        ev.wait(2.0)

        proxy.start()

        with p.connection(timeout=2) as conn:
            conn.execute("select 1")

        p.wait(timeout=3.0)
        assert len(p._pool) == p.min_size == 4


@pytest.mark.slow
def test_refill_on_check(proxy):
    proxy.start()
    ev = Event()

    def failed(pool):
        ev.set()

    with pool.ConnectionPool(
        proxy.client_dsn, min_size=4, reconnect_timeout=1.0, reconnect_failed=failed
    ) as p:
        # The pool is full
        p.wait(timeout=2)

        # Break all the connection
        proxy.stop()

        # Checking the pool will empty it
        p.check()
        ev.wait(2.0)
        assert len(p._pool) == 0

        # Allow to connect again
        proxy.start()

        # Make sure that check has refilled the pool
        p.check()
        p.wait(timeout=2)
        assert len(p._pool) == 4


@pytest.mark.slow
def test_uniform_use(dsn):
    with pool.ConnectionPool(dsn, min_size=4) as p:
        counts = Counter[int]()
        for i in range(8):
            with p.connection() as conn:
                sleep(0.1)
                counts[id(conn)] += 1

    assert len(counts) == 4
    assert set(counts.values()) == set([2])


@pytest.mark.slow
@pytest.mark.timing
def test_resize(dsn):
    def sampler():
        sleep(0.05)  # ensure sampling happens after shrink check
        while True:
            sleep(0.2)
            if p.closed:
                break
            size.append(len(p._pool))

    def client(t):
        with p.connection() as conn:
            conn.execute("select pg_sleep(%s)", [t])

    size: List[int] = []

    with pool.ConnectionPool(dsn, min_size=2, max_idle=0.2) as p:
        s = spawn(sampler)

        sleep(0.3)

        c = spawn(client, args=(0.4,))

        sleep(0.2)
        p.resize(4)
        assert p.min_size == 4
        assert p.max_size == 4

        sleep(0.4)
        p.resize(2)
        assert p.min_size == 2
        assert p.max_size == 2

        sleep(0.6)

    gather(s, c)
    assert size == [2, 1, 3, 4, 3, 2, 2]


@pytest.mark.parametrize("min_size, max_size", [(0, 0), (-1, None), (4, 2)])
def test_bad_resize(dsn, min_size, max_size):
    with pool.ConnectionPool() as p:
        with pytest.raises(ValueError):
            p.resize(min_size=min_size, max_size=max_size)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
def test_max_lifetime(dsn):
    with pool.ConnectionPool(dsn, min_size=1, max_lifetime=0.2) as p:
        sleep(0.1)
        pids = []
        for i in range(5):
            with p.connection() as conn:
                pids.append(conn.info.backend_pid)
            sleep(0.2)

    assert pids[0] == pids[1] != pids[4], pids


@pytest.mark.crdb_skip("backend pid")
def test_check(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    with pool.ConnectionPool(dsn, min_size=4) as p:
        p.wait(1.0)
        with p.connection() as conn:
            pid = conn.info.backend_pid

        p.wait(1.0)
        pids = set((conn.info.backend_pid for conn in p._pool))
        assert pid in pids
        conn.close()

        assert len(caplog.records) == 0
        p.check()
        assert len(caplog.records) == 1
        p.wait(1.0)
        pids2 = set((conn.info.backend_pid for conn in p._pool))
        assert len(pids & pids2) == 3
        assert pid not in pids2


def test_check_idle(dsn):
    with pool.ConnectionPool(dsn, min_size=2) as p:
        p.wait(1.0)
        p.check()
        with p.connection() as conn:
            assert conn.info.transaction_status == TransactionStatus.IDLE


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_connect_no_check(dsn):
    with pool.ConnectionPool(dsn, min_size=2) as p:
        p.wait(1.0)
        with p.connection() as conn:
            with p.connection() as conn2:
                pid2 = conn2.info.backend_pid
            conn.execute("select pg_terminate_backend(%s)", [pid2])

        with pytest.raises(psycopg.OperationalError):
            with p.connection() as conn:
                conn.execute("select 1")
                with p.connection() as conn2:
                    conn2.execute("select 2")


@pytest.mark.crdb_skip("pg_terminate_backend")
@pytest.mark.parametrize("autocommit", [True, False])
def test_connect_check(dsn, caplog, autocommit):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(
        dsn,
        min_size=2,
        kwargs={"autocommit": autocommit},
        check=pool.ConnectionPool.check_connection,
    ) as p:
        p.wait(1.0)
        with p.connection() as conn:
            pid1 = conn.info.backend_pid
            with p.connection() as conn2:
                pid2 = conn2.info.backend_pid
            conn.execute("select pg_terminate_backend(%s)", [pid2])

        with p.connection() as conn:
            assert conn.info.transaction_status == TransactionStatus.IDLE
            conn.execute("select 1")
            with p.connection() as conn2:
                assert conn2.info.transaction_status == TransactionStatus.IDLE
                conn2.execute("select 2")

                pids = {c.info.backend_pid for c in [conn, conn2]}

    assert pid1 in pids
    assert pid2 not in pids
    assert not caplog.records


@pytest.mark.parametrize("autocommit", [True, False])
@pytest.mark.crdb_skip("pg_terminate_backend")
def test_getconn_check(dsn, caplog, autocommit):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(
        dsn,
        kwargs={"autocommit": autocommit},
        min_size=2,
        check=pool.ConnectionPool.check_connection,
    ) as p:
        p.wait(1.0)
        with p.connection() as conn:
            pid1 = conn.info.backend_pid
            with p.connection() as conn2:
                pid2 = conn2.info.backend_pid
            conn.execute("select pg_terminate_backend(%s)", [pid2])

        conn = p.getconn()
        try:
            assert conn.info.transaction_status == TransactionStatus.IDLE
            conn.execute("select 1")
            conn.rollback()
            conn2 = p.getconn()
            try:
                assert conn2.info.transaction_status == TransactionStatus.IDLE
                conn2.execute("select 1")
                conn2.rollback()
                pids = {c.info.backend_pid for c in [conn, conn2]}
            finally:
                p.putconn(conn2)
        finally:
            p.putconn(conn)

    assert pid1 in pids
    assert pid2 not in pids
    assert not caplog.records


@pytest.mark.slow
def test_connect_check_timeout(dsn, proxy):
    proxy.start()
    with pool.ConnectionPool(
        proxy.client_dsn,
        min_size=1,
        timeout=1.0,
        check=pool.ConnectionPool.check_connection,
    ) as p:
        p.wait()

        proxy.stop()
        t0 = time()
        with pytest.raises(pool.PoolTimeout):
            with p.connection():
                pass
        assert 1.0 <= time() - t0 <= 1.1

        proxy.start()
        with p.connection(timeout=10) as conn:
            conn.execute("select 1")


@pytest.mark.slow
def test_check_max_lifetime(dsn):
    with pool.ConnectionPool(dsn, min_size=1, max_lifetime=0.2) as p:
        with p.connection() as conn:
            pid = conn.info.backend_pid
        with p.connection() as conn:
            assert conn.info.backend_pid == pid
        sleep(0.3)
        p.check()
        with p.connection() as conn:
            assert conn.info.backend_pid != pid


@pytest.mark.slow
def test_stats_connect(proxy, monkeypatch):
    proxy.start()
    delay_connection(monkeypatch, 0.2)
    with pool.ConnectionPool(proxy.client_dsn, min_size=3) as p:
        p.wait()
        stats = p.get_stats()
        assert stats["connections_num"] == 3
        assert stats.get("connections_errors", 0) == 0
        assert stats.get("connections_lost", 0) == 0
        assert 580 <= stats["connections_ms"] < 1200

        proxy.stop()
        p.check()
        sleep(0.1)
        stats = p.get_stats()
        assert stats["connections_num"] > 3
        assert stats["connections_errors"] > 0
        assert stats["connections_lost"] == 3


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_stats_check(dsn):
    with pool.ConnectionPool(
        dsn, min_size=1, check=pool.ConnectionPool.check_connection
    ) as p:
        p.wait()
        with p.connection() as conn:
            pid = conn.info.backend_pid

        with psycopg.Connection.connect(dsn) as conn:
            conn.execute("select pg_terminate_backend(%s)", [pid])

        with p.connection() as conn:
            assert conn.info.backend_pid != pid

        stats = p.get_stats()
        assert stats["connections_lost"] == 1


@pytest.mark.slow
def test_spike(dsn, monkeypatch):
    # Inspired to https://github.com/brettwooldridge/HikariCP/blob/dev/
    # documents/Welcome-To-The-Jungle.md
    delay_connection(monkeypatch, 0.15)

    def worker():
        with p.connection():
            sleep(0.002)

    with pool.ConnectionPool(dsn, min_size=5, max_size=10) as p:
        p.wait()

        ts = [spawn(worker) for i in range(50)]
        gather(*ts)
        p.wait()

        assert len(p._pool) < 7


def test_debug_deadlock(dsn):
    # https://github.com/psycopg/psycopg/issues/230
    logger = logging.getLogger("psycopg")
    handler = logging.StreamHandler()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        with pool.ConnectionPool(dsn, min_size=4) as p:
            p.wait(timeout=2)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


@skip_sync
def test_cancellation_in_queue(dsn):
    # https://github.com/psycopg/psycopg/issues/509

    nconns = 3

    with pool.ConnectionPool(dsn, min_size=nconns, timeout=1) as p:
        p.wait()

        got_conns = []
        ev = Event()

        def worker(i):
            try:
                logging.info("worker %s started", i)
                nonlocal got_conns

                with p.connection() as conn:
                    logging.info("worker %s got conn", i)
                    cur = conn.execute("select 1")
                    assert cur.fetchone() == (1,)

                    got_conns.append(conn)
                    if len(got_conns) >= nconns:
                        ev.set()

                    sleep(5)
            except BaseException as ex:
                logging.info("worker %s stopped: %r", i, ex)
                raise

        # Start tasks taking up all the connections and getting in the queue
        tasks = [spawn(worker, (i,)) for i in range(nconns * 3)]

        # wait until the pool has served all the connections and clients are queued.
        ev.wait(3.0)
        for i in range(10):
            if p.get_stats().get("requests_queued", 0):
                break
            else:
                sleep(0.1)
        else:
            pytest.fail("no client got in the queue")

        [task.cancel() for task in reversed(tasks)]
        gather(*tasks, return_exceptions=True, timeout=1.0)

        stats = p.get_stats()
        assert stats["pool_available"] == 3
        assert stats.get("requests_waiting", 0) == 0

        with p.connection() as conn:
            cur = conn.execute("select 1")
            assert cur.fetchone() == (1,)


@pytest.mark.slow
@pytest.mark.timing
def test_check_backoff(dsn, caplog, monkeypatch):
    caplog.set_level(logging.INFO, logger="psycopg.pool")

    assert pool.base.ConnectionAttempt.INITIAL_DELAY == 1.0
    assert pool.base.ConnectionAttempt.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.base.ConnectionAttempt, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.base.ConnectionAttempt, "DELAY_JITTER", 0.0)

    def check(conn):
        raise Exception()

    caplog.clear()
    with pool.ConnectionPool(dsn, min_size=1, check=check) as p:
        p.wait(2.0)

        with pytest.raises(pool.PoolTimeout):
            with p.connection(timeout=1.0):
                assert False

    times = [rec.created for rec in caplog.records if "failed check" in rec.message]
    assert times[1] - times[0] < 0.05
    deltas = [times[i + 1] - times[i] for i in range(1, len(times) - 1)]
    assert len(deltas) == 3
    want = 0.1
    for delta in deltas:
        assert delta == pytest.approx(want, 0.05), deltas
        want *= 2
