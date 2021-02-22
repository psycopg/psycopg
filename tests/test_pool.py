import logging
import weakref
from time import monotonic, sleep, time
from threading import Thread

import pytest

import psycopg3
from psycopg3 import pool
from psycopg3.pq import TransactionStatus


def test_defaults(dsn):
    p = pool.ConnectionPool(dsn)
    assert p.minconn == p.maxconn == 4
    assert p.timeout == 30
    assert p.max_idle == 600
    assert p.num_workers == 3


def test_minconn_maxconn(dsn):
    p = pool.ConnectionPool(dsn, minconn=2)
    assert p.minconn == p.maxconn == 2

    p = pool.ConnectionPool(dsn, minconn=2, maxconn=4)
    assert p.minconn == 2
    assert p.maxconn == 4

    with pytest.raises(ValueError):
        pool.ConnectionPool(dsn, minconn=4, maxconn=2)


def test_kwargs(dsn):
    p = pool.ConnectionPool(dsn, kwargs={"autocommit": True}, minconn=1)
    with p.connection() as conn:
        assert conn.autocommit


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
def test_concurrent_filling(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    t0 = monotonic()
    p = pool.ConnectionPool(dsn, minconn=5, num_workers=2)
    times = [item[1] - t0 for item in p._pool]
    want_times = [0.1, 0.1, 0.2, 0.2, 0.3]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.1), times


@pytest.mark.slow
def test_setup_timeout(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.1)
    with pytest.raises(pool.PoolTimeout):
        pool.ConnectionPool(dsn, minconn=4, num_workers=1, setup_timeout=0.3)

    p = pool.ConnectionPool(dsn, minconn=4, num_workers=1, setup_timeout=0.5)
    p.close()
    p = pool.ConnectionPool(dsn, minconn=4, num_workers=2, setup_timeout=0.3)
    p.close()


@pytest.mark.slow
def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(pool.PoolTimeout):
        pool.ConnectionPool(
            proxy.client_dsn, minconn=1, num_workers=1, setup_timeout=0.2
        )

    p = pool.ConnectionPool(
        proxy.client_dsn, minconn=1, num_workers=1, setup_timeout=0
    )
    sleep(0.5)
    assert not p._pool
    proxy.start()

    with p.connection() as conn:
        conn.execute("select 1")


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

    times = [item[1] for item in results]
    want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times

    assert len(set(r[2] for r in results)) == 2


@pytest.mark.slow
def test_queue_timeout(dsn):
    p = pool.ConnectionPool(dsn, minconn=2, timeout=0.1)
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
def test_dead_client(dsn):
    p = pool.ConnectionPool(dsn, minconn=2)

    results = []

    def worker(i, timeout):
        try:
            with p.connection(timeout=timeout) as conn:
                conn.execute("select pg_sleep(0.3)")
                results.append(i)
        except pool.PoolTimeout:
            if timeout > 0.2:
                raise

    ts = []
    for i, timeout in enumerate([0.4, 0.4, 0.1, 0.4, 0.4]):
        t = Thread(target=worker, args=(i, timeout))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    sleep(0.2)
    assert set(results) == set([0, 1, 3, 4])
    assert len(p._pool) == 2  # no connection was lost


@pytest.mark.slow
def test_queue_timeout_override(dsn):
    p = pool.ConnectionPool(dsn, minconn=2, timeout=0.1)
    results = []
    errors = []

    def worker(n):
        t0 = time()
        timeout = 0.25 if n == 3 else None
        try:
            with p.connection(timeout=timeout) as conn:
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


def test_close_no_threads(dsn):
    p = pool.ConnectionPool(dsn)
    assert p._sched_runner.is_alive()
    for t in p._workers:
        assert t.is_alive()

    p.close()
    assert not p._sched_runner.is_alive()
    for t in p._workers:
        assert not t.is_alive()


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
    p = pool.ConnectionPool(dsn, minconn=2)
    with p.connection() as conn:
        conn.execute("select 1")

    wait_pool_full(p)

    ref = weakref.ref(p)
    del p
    assert not ref()
    assert not recwarn


@pytest.mark.slow
def test_del_stop_threads(dsn):
    p = pool.ConnectionPool(dsn)
    ts = [p._sched_runner] + p._workers
    del p
    sleep(0.2)
    for t in ts:
        assert not t.is_alive()


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

    want_times = [0.2, 0.2, 0.3, 0.3, 0.4, 0.4]
    times = [item[1] for item in results]
    for got, want in zip(times, want_times):
        assert got == pytest.approx(want, 0.2), times


@pytest.mark.slow
def test_shrink(dsn, monkeypatch):
    p = pool.ConnectionPool(
        dsn, minconn=2, maxconn=4, num_workers=3, max_idle=0.2
    )
    assert p.max_idle == 0.2

    def worker(n):
        with p.connection() as conn:
            conn.execute("select 1 from pg_sleep(0.2)")

    ts = []
    for i in range(4):
        t = Thread(target=worker, args=(i,))
        t.start()
        ts.append(t)

    for t in ts:
        t.join()

    wait_pool_full(p)
    assert len(p._pool) == 4

    t0 = time()
    t = None
    while time() < t0 + 0.4:
        with p.connection():
            sleep(0.01)
            if p._nconns < 4:
                t = time() - t0
                break

    assert t == pytest.approx(0.2, 0.1)


@pytest.mark.slow
def test_reconnect(proxy, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg3.pool")

    assert pool.AddConnection.INITIAL_DELAY == 1.0
    assert pool.AddConnection.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.AddConnection, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.AddConnection, "DELAY_JITTER", 0.0)

    proxy.start()
    p = pool.ConnectionPool(proxy.client_dsn, minconn=1, setup_timeout=2.0)
    proxy.stop()

    with pytest.raises(psycopg3.OperationalError):
        with p.connection() as conn:
            conn.execute("select 1")

    sleep(1.0)
    proxy.start()
    wait_pool_full(p)

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
def test_reconnect_failure(proxy):
    proxy.start()

    t1 = None

    def failed(pool):
        assert pool.name == "this-one"
        nonlocal t1
        t1 = time()

    p = pool.ConnectionPool(
        proxy.client_dsn,
        name="this-one",
        minconn=1,
        setup_timeout=2.0,
        reconnect_timeout=1.0,
        reconnect_failed=failed,
    )
    proxy.stop()

    with pytest.raises(psycopg3.OperationalError):
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


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """
    connect_orig = psycopg3.Connection.connect

    def connect_delay(*args, **kwargs):
        t0 = time()
        rv = connect_orig(*args, **kwargs)
        t1 = time()
        sleep(sec - (t1 - t0))
        return rv

    monkeypatch.setattr(psycopg3.Connection, "connect", connect_delay)


def wait_pool_full(pool):
    while len(pool._pool) < pool._nconns:
        sleep(0.01)
