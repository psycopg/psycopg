import logging
from time import sleep, time
from threading import Thread, Event
from typing import Any, List, Tuple

import pytest
from packaging.version import parse as ver  # noqa: F401  # used in skipif

import psycopg
from psycopg.pq import TransactionStatus

from .test_pool import delay_connection, ensure_waiting

try:
    from psycopg_pool import NullConnectionPool
    from psycopg_pool import PoolClosed, PoolTimeout, TooManyRequests
except ImportError:
    pass


def test_defaults(dsn):
    with NullConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 0
        assert p.timeout == 30
        assert p.max_idle == 10 * 60
        assert p.max_lifetime == 60 * 60
        assert p.num_workers == 3


def test_min_size_max_size(dsn):
    with NullConnectionPool(dsn, min_size=0, max_size=2) as p:
        assert p.min_size == 0
        assert p.max_size == 2


@pytest.mark.parametrize("min_size, max_size", [(1, None), (-1, None), (0, -2)])
def test_bad_size(dsn, min_size, max_size):
    with pytest.raises(ValueError):
        NullConnectionPool(min_size=min_size, max_size=max_size)


def test_connection_class(dsn):
    class MyConn(psycopg.Connection[Any]):
        pass

    with NullConnectionPool(dsn, connection_class=MyConn) as p:
        with p.connection() as conn:
            assert isinstance(conn, MyConn)


def test_kwargs(dsn):
    with NullConnectionPool(dsn, kwargs={"autocommit": True}) as p:
        with p.connection() as conn:
            assert conn.autocommit


@pytest.mark.crdb_skip("backend pid")
def test_its_no_pool_at_all(dsn):
    with NullConnectionPool(dsn, max_size=2) as p:
        with p.connection() as conn:
            pid1 = conn.info.backend_pid

            with p.connection() as conn2:
                pid2 = conn2.info.backend_pid

        with p.connection() as conn:
            assert conn.info.backend_pid not in (pid1, pid2)


def test_context(dsn):
    with NullConnectionPool(dsn) as p:
        assert not p.closed
    assert p.closed


@pytest.mark.slow
@pytest.mark.timing
def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.2)
    with pytest.raises(PoolTimeout):
        with NullConnectionPool(dsn, num_workers=1) as p:
            p.wait(0.1)

    with NullConnectionPool(dsn, num_workers=1) as p:
        p.wait(0.4)


def test_wait_closed(dsn):
    with NullConnectionPool(dsn) as p:
        pass

    with pytest.raises(PoolClosed):
        p.wait()


@pytest.mark.slow
def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(PoolTimeout):
        with NullConnectionPool(proxy.client_dsn, num_workers=1) as p:
            p.wait(0.2)

    with NullConnectionPool(proxy.client_dsn, num_workers=1) as p:
        sleep(0.5)
        assert not p._pool
        proxy.start()

        with p.connection() as conn:
            conn.execute("select 1")


def test_configure(dsn):
    inits = 0

    def configure(conn):
        nonlocal inits
        inits += 1
        with conn.transaction():
            conn.execute("set default_transaction_read_only to on")

    with NullConnectionPool(dsn, configure=configure) as p:
        with p.connection() as conn:
            assert inits == 1
            res = conn.execute("show default_transaction_read_only")
            assert res.fetchone()[0] == "on"  # type: ignore[index]

        with p.connection() as conn:
            assert inits == 2
            res = conn.execute("show default_transaction_read_only")
            assert res.fetchone()[0] == "on"  # type: ignore[index]
            conn.close()

        with p.connection() as conn:
            assert inits == 3
            res = conn.execute("show default_transaction_read_only")
            assert res.fetchone()[0] == "on"  # type: ignore[index]


@pytest.mark.slow
def test_configure_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def configure(conn):
        conn.execute("select 1")

    with NullConnectionPool(dsn, configure=configure) as p:
        with pytest.raises(PoolTimeout):
            p.wait(timeout=0.5)

    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.slow
def test_configure_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def configure(conn):
        with conn.transaction():
            conn.execute("WAT")

    with NullConnectionPool(dsn, configure=configure) as p:
        with pytest.raises(PoolTimeout):
            p.wait(timeout=0.5)

    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
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

    pids = []

    def worker():
        with p.connection() as conn:
            assert resets == 1
            with conn.execute("show timezone") as cur:
                assert cur.fetchone() == ("UTC",)
            pids.append(conn.info.backend_pid)

    with NullConnectionPool(dsn, max_size=1, reset=reset) as p:
        with p.connection() as conn:

            # Queue the worker so it will take the same connection a second time
            # instead of making a new one.
            t = Thread(target=worker)
            t.start()
            ensure_waiting(p)

            assert resets == 0
            conn.execute("set timezone to '+2:00'")
            pids.append(conn.info.backend_pid)

        t.join()
        p.wait()

    assert resets == 1
    assert pids[0] == pids[1]


@pytest.mark.crdb_skip("backend pid")
def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def reset(conn):
        conn.execute("reset all")

    pids = []

    def worker():
        with p.connection() as conn:
            conn.execute("select 1")
            pids.append(conn.info.backend_pid)

    with NullConnectionPool(dsn, max_size=1, reset=reset) as p:
        with p.connection() as conn:

            t = Thread(target=worker)
            t.start()
            ensure_waiting(p)

            conn.execute("select 1")
            pids.append(conn.info.backend_pid)

        t.join()

    assert pids[0] != pids[1]
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def reset(conn):
        with conn.transaction():
            conn.execute("WAT")

    pids = []

    def worker():
        with p.connection() as conn:
            conn.execute("select 1")
            pids.append(conn.info.backend_pid)

    with NullConnectionPool(dsn, max_size=1, reset=reset) as p:
        with p.connection() as conn:

            t = Thread(target=worker)
            t.start()
            ensure_waiting(p)

            conn.execute("select 1")
            pids.append(conn.info.backend_pid)

        t.join()

    assert pids[0] != pids[1]
    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.slow
@pytest.mark.skipif("ver(psycopg.__version__) < ver('3.0.8')")
def test_no_queue_timeout(deaf_port):
    with NullConnectionPool(kwargs={"host": "localhost", "port": deaf_port}) as p:
        with pytest.raises(PoolTimeout):
            with p.connection(timeout=1):
                pass


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
def test_queue(dsn):
    def worker(n):
        t0 = time()
        with p.connection() as conn:
            conn.execute("select pg_sleep(0.2)")
            pid = conn.info.backend_pid

        t1 = time()
        results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    with NullConnectionPool(dsn, max_size=2) as p:
        p.wait()
        ts = [Thread(target=worker, args=(i,)) for i in range(6)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    times = [item[1] for item in results]
    want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times

    assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
def test_queue_size(dsn):
    def worker(t, ev=None):
        try:
            with p.connection():
                if ev:
                    ev.set()
                sleep(t)
        except TooManyRequests as e:
            errors.append(e)
        else:
            success.append(True)

    errors: List[Exception] = []
    success: List[bool] = []

    with NullConnectionPool(dsn, max_size=1, max_waiting=3) as p:
        p.wait()
        ev = Event()
        t = Thread(target=worker, args=(0.3, ev))
        t.start()
        ev.wait()

        ts = [Thread(target=worker, args=(0.1,)) for i in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    assert len(success) == 4
    assert len(errors) == 1
    assert isinstance(errors[0], TooManyRequests)
    assert p.name in str(errors[0])
    assert str(p.max_waiting) in str(errors[0])
    assert p.get_stats()["requests_errors"] == 1


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
def test_queue_timeout(dsn):
    def worker(n):
        t0 = time()
        try:
            with p.connection() as conn:
                conn.execute("select pg_sleep(0.2)")
                pid = conn.info.backend_pid
        except PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    with NullConnectionPool(dsn, max_size=2, timeout=0.1) as p:
        ts = [Thread(target=worker, args=(i,)) for i in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    assert len(results) == 2
    assert len(errors) == 2
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.slow
@pytest.mark.timing
def test_dead_client(dsn):
    def worker(i, timeout):
        try:
            with p.connection(timeout=timeout) as conn:
                conn.execute("select pg_sleep(0.3)")
                results.append(i)
        except PoolTimeout:
            if timeout > 0.2:
                raise

    results: List[int] = []

    with NullConnectionPool(dsn, max_size=2) as p:
        ts = [
            Thread(target=worker, args=(i, timeout))
            for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4])
        ]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        sleep(0.2)
        assert set(results) == set([0, 1, 3, 4])


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
def test_queue_timeout_override(dsn):
    def worker(n):
        t0 = time()
        timeout = 0.25 if n == 3 else None
        try:
            with p.connection(timeout=timeout) as conn:
                conn.execute("select pg_sleep(0.2)")
                pid = conn.info.backend_pid
        except PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    results: List[Tuple[int, float, int]] = []
    errors: List[Tuple[int, float, Exception]] = []

    with NullConnectionPool(dsn, max_size=2, timeout=0.1) as p:
        ts = [Thread(target=worker, args=(i,)) for i in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.crdb_skip("backend pid")
def test_broken_reconnect(dsn):
    with NullConnectionPool(dsn, max_size=1) as p:
        with p.connection() as conn:
            pid1 = conn.info.backend_pid
            conn.close()

        with p.connection() as conn2:
            pid2 = conn2.info.backend_pid

    assert pid1 != pid2


@pytest.mark.crdb_skip("backend pid")
def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    def worker():
        with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE
            assert not conn.execute(
                "select 1 from pg_class where relname = 'test_intrans_rollback'"
            ).fetchone()

    with NullConnectionPool(dsn, max_size=1) as p:
        conn = p.getconn()

        # Queue the worker so it will take the connection a second time instead
        # of making a new one.
        t = Thread(target=worker)
        t.start()
        ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        conn.execute("create table test_intrans_rollback ()")
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        p.putconn(conn)
        t.join()

    assert pids[0] == pids[1]
    assert len(caplog.records) == 1
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
def test_inerror_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    def worker():
        with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    with NullConnectionPool(dsn, max_size=1) as p:
        conn = p.getconn()

        # Queue the worker so it will take the connection a second time instead
        # of making a new one.
        t = Thread(target=worker)
        t.start()
        ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        with pytest.raises(psycopg.ProgrammingError):
            conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        p.putconn(conn)
        t.join()

    assert pids[0] == pids[1]
    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
@pytest.mark.crdb_skip("copy")
def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    def worker():
        with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    with NullConnectionPool(dsn, max_size=1) as p:
        conn = p.getconn()

        t = Thread(target=worker)
        t.start()
        ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        conn.pgconn.exec_(b"copy (select * from generate_series(1, 10)) to stdout")
        assert conn.info.transaction_status == TransactionStatus.ACTIVE
        p.putconn(conn)
        t.join()

    assert pids[0] != pids[1]
    assert len(caplog.records) == 2
    assert "ACTIVE" in caplog.records[0].message
    assert "BAD" in caplog.records[1].message


@pytest.mark.crdb_skip("backend pid")
def test_fail_rollback_close(dsn, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    def worker(p):
        with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    with NullConnectionPool(dsn, max_size=1) as p:
        conn = p.getconn()

        def bad_rollback():
            conn.pgconn.finish()
            orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        t = Thread(target=worker, args=(p,))
        t.start()
        ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        with pytest.raises(psycopg.ProgrammingError):
            conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        p.putconn(conn)
        t.join()

    assert pids[0] != pids[1]
    assert len(caplog.records) == 3
    assert "INERROR" in caplog.records[0].message
    assert "OperationalError" in caplog.records[1].message
    assert "BAD" in caplog.records[2].message


def test_close_no_threads(dsn):
    p = NullConnectionPool(dsn)
    assert p._sched_runner and p._sched_runner.is_alive()
    workers = p._workers[:]
    assert workers
    for t in workers:
        assert t.is_alive()

    p.close()
    assert p._sched_runner is None
    assert not p._workers
    for t in workers:
        assert not t.is_alive()


def test_putconn_no_pool(conn_cls, dsn):
    with NullConnectionPool(dsn) as p:
        conn = conn_cls.connect(dsn)
        with pytest.raises(ValueError):
            p.putconn(conn)

    conn.close()


def test_putconn_wrong_pool(dsn):
    with NullConnectionPool(dsn) as p1:
        with NullConnectionPool(dsn) as p2:
            conn = p1.getconn()
            with pytest.raises(ValueError):
                p2.putconn(conn)


@pytest.mark.slow
def test_del_stop_threads(dsn):
    p = NullConnectionPool(dsn)
    assert p._sched_runner is not None
    ts = [p._sched_runner] + p._workers
    del p
    sleep(0.1)
    for t in ts:
        assert not t.is_alive()


def test_closed_getconn(dsn):
    p = NullConnectionPool(dsn)
    assert not p.closed
    with p.connection():
        pass

    p.close()
    assert p.closed

    with pytest.raises(PoolClosed):
        with p.connection():
            pass


def test_closed_putconn(dsn):
    p = NullConnectionPool(dsn)

    with p.connection() as conn:
        pass
    assert conn.closed

    with p.connection() as conn:
        p.close()
    assert conn.closed


def test_closed_queue(dsn):
    def w1():
        with p.connection() as conn:
            e1.set()  # Tell w0 that w1 got a connection
            cur = conn.execute("select 1")
            assert cur.fetchone() == (1,)
            e2.wait()  # Wait until w0 has tested w2
        success.append("w1")

    def w2():
        try:
            with p.connection():
                pass  # unexpected
        except PoolClosed:
            success.append("w2")

    e1 = Event()
    e2 = Event()

    p = NullConnectionPool(dsn, max_size=1)
    p.wait()
    success: List[str] = []

    t1 = Thread(target=w1)
    t1.start()
    # Wait until w1 has received a connection
    e1.wait()

    t2 = Thread(target=w2)
    t2.start()
    # Wait until w2 is in the queue
    ensure_waiting(p)

    p.close(0)

    # Wait for the workers to finish
    e2.set()
    t1.join()
    t2.join()
    assert len(success) == 2


def test_open_explicit(dsn):
    p = NullConnectionPool(dsn, open=False)
    assert p.closed
    with pytest.raises(PoolClosed, match="is not open yet"):
        p.getconn()

    with pytest.raises(PoolClosed):
        with p.connection():
            pass

    p.open()
    try:
        assert not p.closed

        with p.connection() as conn:
            cur = conn.execute("select 1")
            assert cur.fetchone() == (1,)

    finally:
        p.close()

    with pytest.raises(PoolClosed, match="is already closed"):
        p.getconn()


def test_open_context(dsn):
    p = NullConnectionPool(dsn, open=False)
    assert p.closed

    with p:
        assert not p.closed

        with p.connection() as conn:
            cur = conn.execute("select 1")
            assert cur.fetchone() == (1,)

    assert p.closed


def test_open_no_op(dsn):
    p = NullConnectionPool(dsn)
    try:
        assert not p.closed
        p.open()
        assert not p.closed

        with p.connection() as conn:
            cur = conn.execute("select 1")
            assert cur.fetchone() == (1,)

    finally:
        p.close()


def test_reopen(dsn):
    p = NullConnectionPool(dsn)
    with p.connection() as conn:
        conn.execute("select 1")
    p.close()
    assert p._sched_runner is None
    assert not p._workers

    with pytest.raises(psycopg.OperationalError, match="cannot be reused"):
        p.open()


@pytest.mark.parametrize("min_size, max_size", [(1, None), (-1, None), (0, -2)])
def test_bad_resize(dsn, min_size, max_size):
    with NullConnectionPool() as p:
        with pytest.raises(ValueError):
            p.resize(min_size=min_size, max_size=max_size)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
def test_max_lifetime(dsn):
    pids = []

    def worker(p):
        with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            sleep(0.1)

    ts = []
    with NullConnectionPool(dsn, max_size=1, max_lifetime=0.2) as p:
        for i in range(5):
            ts.append(Thread(target=worker, args=(p,)))
            ts[-1].start()

        for t in ts:
            t.join()

    assert pids[0] == pids[1] != pids[4], pids


def test_check(dsn):
    with NullConnectionPool(dsn) as p:
        # No-op
        p.check()


@pytest.mark.slow
@pytest.mark.timing
def test_stats_measures(dsn):
    def worker(n):
        with p.connection() as conn:
            conn.execute("select pg_sleep(0.2)")

    with NullConnectionPool(dsn, max_size=4) as p:
        p.wait(2.0)

        stats = p.get_stats()
        assert stats["pool_min"] == 0
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 0
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 0

        ts = [Thread(target=worker, args=(i,)) for i in range(3)]
        for t in ts:
            t.start()
        sleep(0.1)
        stats = p.get_stats()
        for t in ts:
            t.join()
        assert stats["pool_min"] == 0
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 3
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 0

        p.wait(2.0)
        ts = [Thread(target=worker, args=(i,)) for i in range(7)]
        for t in ts:
            t.start()
        sleep(0.1)
        stats = p.get_stats()
        for t in ts:
            t.join()
        assert stats["pool_min"] == 0
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 4
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 3


@pytest.mark.slow
@pytest.mark.timing
def test_stats_usage(dsn):
    def worker(n):
        try:
            with p.connection(timeout=0.3) as conn:
                conn.execute("select pg_sleep(0.2)")
        except PoolTimeout:
            pass

    with NullConnectionPool(dsn, max_size=3) as p:
        p.wait(2.0)

        ts = [Thread(target=worker, args=(i,)) for i in range(7)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        stats = p.get_stats()
        assert stats["requests_num"] == 7
        assert stats["requests_queued"] == 4
        assert 850 <= stats["requests_wait_ms"] <= 950
        assert stats["requests_errors"] == 1
        assert 1150 <= stats["usage_ms"] <= 1250
        assert stats.get("returns_bad", 0) == 0

        with p.connection() as conn:
            conn.close()
        p.wait()
        stats = p.pop_stats()
        assert stats["requests_num"] == 8
        assert stats["returns_bad"] == 1
        with p.connection():
            pass
        assert p.get_stats()["requests_num"] == 1


@pytest.mark.slow
def test_stats_connect(dsn, proxy, monkeypatch):
    proxy.start()
    delay_connection(monkeypatch, 0.2)
    with NullConnectionPool(proxy.client_dsn, max_size=3) as p:
        p.wait()
        stats = p.get_stats()
        assert stats["connections_num"] == 1
        assert stats.get("connections_errors", 0) == 0
        assert stats.get("connections_lost", 0) == 0
        assert 200 <= stats["connections_ms"] < 300
