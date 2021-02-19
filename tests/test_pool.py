import logging
import weakref
from time import time, sleep
from threading import Thread

import pytest

import psycopg3
from psycopg3 import pool
from psycopg3.pq import TransactionStatus


def test_minconn_maxconn(dsn):
    p = pool.ConnectionPool(dsn, num_workers=0)
    assert p.minconn == p.maxconn == 4

    p = pool.ConnectionPool(dsn, minconn=2, num_workers=0)
    assert p.minconn == p.maxconn == 2

    p = pool.ConnectionPool(dsn, minconn=2, maxconn=4, num_workers=0)
    assert p.minconn == 2
    assert p.maxconn == 4

    with pytest.raises(ValueError):
        pool.ConnectionPool(dsn, minconn=4, maxconn=2, num_workers=0)


def test_its_really_a_pool(dsn):
    p = pool.ConnectionPool(dsn, minconn=2)
    with p.connection() as conn:
        with conn.execute("select pg_backend_pid()") as cur:
            (pid1,) = cur.fetchone()

        with p.connection() as conn2:
            with conn2.execute("select pg_backend_pid()") as cur:
                (pid2,) = cur.fetchone()

    with p.connection() as conn:
        assert conn.pgconn.backend_pid in (pid1, pid2)


def test_connection_not_lost(dsn):
    p = pool.ConnectionPool(dsn, minconn=1)
    with pytest.raises(ZeroDivisionError):
        with p.connection() as conn:
            pid = conn.pgconn.backend_pid
            1 / 0

    with p.connection() as conn2:
        assert conn2.pgconn.backend_pid == pid


@pytest.mark.slow
def test_queue(dsn):
    p = pool.ConnectionPool(dsn, minconn=2)
    results = []

    def worker(n):
        t0 = time()
        with p.connection() as conn:
            (pid,) = conn.execute(
                "select pg_backend_pid() from pg_sleep(0.2)"
            ).fetchone()
        t1 = time()
        results.append((n, t1 - t0, pid))

    ts = []
    for i in range(6):
        t = Thread(target=worker, args=(i,))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    assert len([r for r in results if 0.2 < r[1] < 0.35]) == 2
    assert len([r for r in results if 0.4 < r[1] < 0.55]) == 2
    assert len([r for r in results if 0.5 < r[1] < 0.75]) == 2
    assert len(set(r[2] for r in results)) == 2


@pytest.mark.slow
def test_queue_timeout(dsn):
    p = pool.ConnectionPool(dsn, minconn=2, timeout_sec=0.1)
    results = []
    errors = []

    def worker(n):
        t0 = time()
        try:
            with p.connection() as conn:
                (pid,) = conn.execute(
                    "select pg_backend_pid() from pg_sleep(0.2)"
                ).fetchone()
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    ts = []
    for i in range(4):
        t = Thread(target=worker, args=(i,))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    assert len(results) == 2
    assert len(errors) == 2
    for e in errors:
        assert 0.1 < e[1] < 0.15


@pytest.mark.slow
def test_queue_timeout_override(dsn):
    p = pool.ConnectionPool(dsn, minconn=2, timeout_sec=0.1)
    results = []
    errors = []

    def worker(n):
        t0 = time()
        timeout = 0.25 if n == 3 else None
        try:
            with p.connection(timeout_sec=timeout) as conn:
                (pid,) = conn.execute(
                    "select pg_backend_pid() from pg_sleep(0.2)"
                ).fetchone()
        except pool.PoolTimeout as e:
            t1 = time()
            errors.append((n, t1 - t0, e))
        else:
            t1 = time()
            results.append((n, t1 - t0, pid))

    ts = []
    for i in range(4):
        t = Thread(target=worker, args=(i,))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    assert len(results) == 3
    assert len(errors) == 1
    for e in errors:
        assert 0.1 < e[1] < 0.15


def test_broken_reconnect(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")
    p = pool.ConnectionPool(dsn, minconn=1)
    with pytest.raises(psycopg3.OperationalError):
        with p.connection() as conn:
            with conn.execute("select pg_backend_pid()") as cur:
                (pid1,) = cur.fetchone()
            conn.close()

    with p.connection() as conn2:
        with conn2.execute("select pg_backend_pid()") as cur:
            (pid2,) = cur.fetchone()

    assert pid1 != pid2


def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")
    p = pool.ConnectionPool(dsn, minconn=1)
    conn = p.getconn()
    pid = conn.pgconn.backend_pid
    conn.execute("create table test_intrans_rollback ()")
    assert conn.pgconn.transaction_status == TransactionStatus.INTRANS
    p.putconn(conn)

    with p.connection() as conn2:
        assert conn2.pgconn.backend_pid == pid
        assert conn2.pgconn.transaction_status == TransactionStatus.IDLE
        assert not conn.execute(
            "select 1 from pg_class where relname = 'test_intrans_rollback'"
        ).fetchone()

    assert len(caplog.records) == 1
    assert "INTRANS" in caplog.records[0].message


def test_inerror_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")
    p = pool.ConnectionPool(dsn, minconn=1)
    conn = p.getconn()
    pid = conn.pgconn.backend_pid
    with pytest.raises(psycopg3.ProgrammingError):
        conn.execute("wat")
    assert conn.pgconn.transaction_status == TransactionStatus.INERROR
    p.putconn(conn)

    with p.connection() as conn2:
        assert conn2.pgconn.backend_pid == pid
        assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")
    p = pool.ConnectionPool(dsn, minconn=1)
    conn = p.getconn()
    pid = conn.pgconn.backend_pid
    cur = conn.cursor()
    with cur.copy("copy (select * from generate_series(1, 10)) to stdout"):
        pass
    assert conn.pgconn.transaction_status == TransactionStatus.ACTIVE
    p.putconn(conn)

    with p.connection() as conn2:
        assert conn2.pgconn.backend_pid != pid
        assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 2
    assert "ACTIVE" in caplog.records[0].message
    assert "BAD" in caplog.records[1].message


def test_fail_rollback_close(dsn, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")
    p = pool.ConnectionPool(dsn, minconn=1)
    conn = p.getconn()

    # Make the rollback fail
    orig_rollback = conn.rollback

    def bad_rollback():
        conn.pgconn.finish()
        orig_rollback()

    monkeypatch.setattr(conn, "rollback", bad_rollback)

    pid = conn.pgconn.backend_pid
    with pytest.raises(psycopg3.ProgrammingError):
        conn.execute("wat")
    assert conn.pgconn.transaction_status == TransactionStatus.INERROR
    p.putconn(conn)

    with p.connection() as conn2:
        assert conn2.pgconn.backend_pid != pid
        assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 3
    assert "INERROR" in caplog.records[0].message
    assert "OperationalError" in caplog.records[1].message
    assert "BAD" in caplog.records[2].message


def test_putconn_no_pool(dsn):
    p = pool.ConnectionPool(dsn, minconn=1)
    conn = psycopg3.connect(dsn)
    with pytest.raises(ValueError):
        p.putconn(conn)


def test_putconn_wrong_pool(dsn):
    p1 = pool.ConnectionPool(dsn, minconn=1)
    p2 = pool.ConnectionPool(dsn, minconn=1)
    conn = p1.getconn()
    with pytest.raises(ValueError):
        p2.putconn(conn)


def test_del_no_warning(dsn, recwarn):
    p = pool.ConnectionPool(minconn=2)
    with p.connection() as conn:
        conn.execute("select 1")

    wait_pool_full(p)

    ref = weakref.ref(p)
    del p
    assert not ref()
    assert not recwarn


def test_closed_getconn(dsn):
    p = pool.ConnectionPool(dsn, minconn=1)
    assert not p.closed
    with p.connection():
        pass

    p.close()
    assert p.closed

    with pytest.raises(pool.PoolClosed):
        with p.connection():
            pass


def test_closed_putconn(dsn):
    p = pool.ConnectionPool(dsn, minconn=1)

    with p.connection() as conn:
        pass
    assert not conn.closed

    with p.connection() as conn:
        p.close()
    assert conn.closed


@pytest.mark.slow
def test_closed_queue(dsn):
    p = pool.ConnectionPool(dsn, minconn=1)
    success = []

    def w1():
        with p.connection() as conn:
            assert (
                conn.execute("select 1 from pg_sleep(0.2)").fetchone()[0] == 1
            )
        success.append("w1")

    def w2():
        with pytest.raises(pool.PoolClosed):
            with p.connection():
                pass
        success.append("w2")

    t1 = Thread(target=w1)
    t2 = Thread(target=w2)
    t1.start()
    sleep(0.1)
    t2.start()
    p.close()
    t1.join()
    t2.join()
    assert len(success) == 2


@pytest.mark.slow
def test_grow(dsn, monkeypatch):
    p = pool.ConnectionPool(dsn, minconn=2, maxconn=4, num_workers=3)
    wait_pool_full(p)
    delay_connection(monkeypatch, 0.1)
    ts = []
    results = []

    def worker(n):
        t0 = time()
        with p.connection() as conn:
            conn.execute("select 1 from pg_sleep(0.2)")
        t1 = time()
        results.append((n, t1 - t0))

    for i in range(6):
        t = Thread(target=worker, args=(i,))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    deltas = [0.2, 0.2, 0.3, 0.3, 0.4, 0.4]
    for (_, got), want in zip(results, deltas):
        assert got == pytest.approx(want, 0.1)


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """
    connect_gen_orig = psycopg3.Connection._connect_gen

    def connect_gen_delayed(*args, **kwargs):
        psycopg3.pool.logger.debug("delaying connection")
        sleep(sec)
        rv = yield from connect_gen_orig(*args, **kwargs)
        return rv

    monkeypatch.setattr(
        psycopg3.Connection, "_connect_gen", connect_gen_delayed
    )


def wait_pool_full(pool):
    while len(pool._pool) < pool.minconn:
        sleep(0.01)
