from time import time
from threading import Thread

import pytest

from psycopg3 import pool


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


def test_pool(dsn):
    p = pool.ConnectionPool(dsn, minconn=2, timeout_sec=1.0)
    with p.connection() as conn:
        with conn.execute("select pg_backend_pid()") as cur:
            (pid1,) = cur.fetchone()

        with p.connection() as conn2:
            with conn2.execute("select pg_backend_pid()") as cur:
                (pid2,) = cur.fetchone()

    with p.connection() as conn:
        assert conn.pgconn.backend_pid in (pid1, pid2)


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
