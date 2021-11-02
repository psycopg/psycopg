import sys
import logging
import weakref
from time import sleep, time
from threading import Thread, Event
from collections import Counter
from typing import Any, List, Tuple

import pytest

import psycopg
from psycopg.pq import TransactionStatus

pytestmark = []

try:
    from psycopg_pool import ConnectionPool  # noqa: F401
except ImportError as ex:
    pytestmark.append(pytest.mark.skip(reason=str(ex)))
else:
    import psycopg_pool as pool


def test_package_version(mypy):
    cp = mypy.run_on_source(
        """\
from psycopg_pool import __version__
assert __version__
"""
    )
    assert not cp.stdout


def test_defaults(dsn):
    with pool.ConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 4
        assert p.timeout == 30
        assert p.max_idle == 10 * 60
        assert p.max_lifetime == 60 * 60
        assert p.num_workers == 3


def test_min_size_max_size(dsn):
    with pool.ConnectionPool(dsn, min_size=2) as p:
        assert p.min_size == p.max_size == 2

    with pool.ConnectionPool(dsn, min_size=2, max_size=4) as p:
        assert p.min_size == 2
        assert p.max_size == 4

    with pytest.raises(ValueError):
        pool.ConnectionPool(dsn, min_size=4, max_size=2)


def test_connection_class(dsn):
    class MyConn(psycopg.Connection[Any]):
        pass

    with pool.ConnectionPool(dsn, connection_class=MyConn, min_size=1) as p:
        with p.connection() as conn:
            assert isinstance(conn, MyConn)


def test_kwargs(dsn):
    with pool.ConnectionPool(
        dsn, kwargs={"autocommit": True}, min_size=1
    ) as p:
        with p.connection() as conn:
            assert conn.autocommit


def test_its_really_a_pool(dsn):
    with pool.ConnectionPool(dsn, min_size=2) as p:
        with p.connection() as conn:
            with conn.execute("select pg_backend_pid()") as cur:
                (pid1,) = cur.fetchone()

            with p.connection() as conn2:
                with conn2.execute("select pg_backend_pid()") as cur:
                    (pid2,) = cur.fetchone()

        with p.connection() as conn:
            assert conn.pgconn.backend_pid in (pid1, pid2)


def test_context(dsn):
    with pool.ConnectionPool(dsn, min_size=1) as p:
        assert not p.closed
    assert p.closed


def test_connection_not_lost(dsn):
    with pool.ConnectionPool(dsn, min_size=1) as p:
        with pytest.raises(ZeroDivisionError):
            with p.connection() as conn:
                pid = conn.pgconn.backend_pid
                1 / 0

        with p.connection() as conn2:
            assert conn2.pgconn.backend_pid == pid


@pytest.mark.slow
@pytest.mark.timing
def test_concurrent_filling(dsn, monkeypatch, retries):
    delay_connection(monkeypatch, 0.1)

    def add_time(self, conn):
        times.append(time() - t0)
        add_orig(self, conn)

    add_orig = pool.ConnectionPool._add_to_pool
    monkeypatch.setattr(pool.ConnectionPool, "_add_to_pool", add_time)

    for retry in retries:
        with retry:
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


@pytest.mark.slow
def test_setup_no_timeout(dsn, proxy):
    with pytest.raises(pool.PoolTimeout):
        with pool.ConnectionPool(
            proxy.client_dsn, min_size=1, num_workers=1
        ) as p:
            p.wait(0.2)

    with pool.ConnectionPool(proxy.client_dsn, min_size=1, num_workers=1) as p:
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


@pytest.mark.slow
def test_configure_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def configure(conn):
        conn.execute("select 1")

    with pool.ConnectionPool(dsn, min_size=1, configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            p.wait(timeout=0.5)

    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.slow
def test_configure_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def configure(conn):
        with conn.transaction():
            conn.execute("WAT")

    with pool.ConnectionPool(dsn, min_size=1, configure=configure) as p:
        with pytest.raises(pool.PoolTimeout):
            p.wait(timeout=0.5)

    assert caplog.records
    assert "WAT" in caplog.records[0].message


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
            with conn.execute("show timezone") as cur:
                assert cur.fetchone() == ("UTC",)

        p.wait()
        assert resets == 2


def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def reset(conn):
        conn.execute("reset all")

    with pool.ConnectionPool(dsn, min_size=1, reset=reset) as p:
        with p.connection() as conn:
            conn.execute("select 1")
            pid1 = conn.pgconn.backend_pid

        with p.connection() as conn:
            conn.execute("select 1")
            pid2 = conn.pgconn.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    def reset(conn):
        with conn.transaction():
            conn.execute("WAT")

    with pool.ConnectionPool(dsn, min_size=1, reset=reset) as p:
        with p.connection() as conn:
            conn.execute("select 1")
            pid1 = conn.pgconn.backend_pid

        with p.connection() as conn:
            conn.execute("select 1")
            pid2 = conn.pgconn.backend_pid

    assert pid1 != pid2
    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.slow
@pytest.mark.timing
def test_queue(dsn, retries):
    def worker(n):
        t0 = time()
        with p.connection() as conn:
            (pid,) = conn.execute(
                "select pg_backend_pid() from pg_sleep(0.2)"
            ).fetchone()
        t1 = time()
        results.append((n, t1 - t0, pid))

    for retry in retries:
        with retry:
            results: List[Tuple[int, float, int]] = []
            with pool.ConnectionPool(dsn, min_size=2) as p:
                p.wait()
                ts = [Thread(target=worker, args=(i,)) for i in range(6)]
                for t in ts:
                    t.start()
                for t in ts:
                    t.join()

            times = [item[1] for item in results]
            want_times = [0.2, 0.2, 0.4, 0.4, 0.6, 0.6]
            for got, want in zip(times, want_times):
                assert got == pytest.approx(want, 0.1), times

            assert len(set(r[2] for r in results)) == 2, results


@pytest.mark.slow
def test_queue_size(dsn):
    def worker(t, ev=None):
        try:
            with p.connection():
                if ev:
                    ev.set()
                sleep(t)
        except pool.TooManyRequests as e:
            errors.append(e)
        else:
            success.append(True)

    errors: List[Exception] = []
    success: List[bool] = []

    with pool.ConnectionPool(dsn, min_size=1, max_waiting=3) as p:
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
    assert isinstance(errors[0], pool.TooManyRequests)
    assert p.name in str(errors[0])
    assert str(p.max_waiting) in str(errors[0])
    assert p.get_stats()["requests_errors"] == 1


@pytest.mark.slow
@pytest.mark.timing
def test_queue_timeout(dsn, retries):
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

    for retry in retries:
        with retry:
            results: List[Tuple[int, float, int]] = []
            errors: List[Tuple[int, float, Exception]] = []

            with pool.ConnectionPool(dsn, min_size=2, timeout=0.1) as p:
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
        except pool.PoolTimeout:
            if timeout > 0.2:
                raise

    results: List[int] = []

    with pool.ConnectionPool(dsn, min_size=2) as p:
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
        assert len(p._pool) == 2  # no connection was lost


@pytest.mark.slow
@pytest.mark.timing
def test_queue_timeout_override(dsn, retries):
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

    for retry in retries:
        with retry:
            results: List[Tuple[int, float, int]] = []
            errors: List[Tuple[int, float, Exception]] = []

            with pool.ConnectionPool(dsn, min_size=2, timeout=0.1) as p:
                ts = [Thread(target=worker, args=(i,)) for i in range(4)]
                for t in ts:
                    t.start()
                for t in ts:
                    t.join()

            assert len(results) == 3
            assert len(errors) == 1
            for e in errors:
                assert 0.1 < e[1] < 0.15


def test_broken_reconnect(dsn):
    with pool.ConnectionPool(dsn, min_size=1) as p:
        with p.connection() as conn:
            with conn.execute("select pg_backend_pid()") as cur:
                (pid1,) = cur.fetchone()
            conn.close()

        with p.connection() as conn2:
            with conn2.execute("select pg_backend_pid()") as cur:
                (pid2,) = cur.fetchone()

    assert pid1 != pid2


def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
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
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = p.getconn()
        pid = conn.pgconn.backend_pid
        with pytest.raises(psycopg.ProgrammingError):
            conn.execute("wat")
        assert conn.pgconn.transaction_status == TransactionStatus.INERROR
        p.putconn(conn)

        with p.connection() as conn2:
            assert conn2.pgconn.backend_pid == pid
            assert conn2.pgconn.transaction_status == TransactionStatus.IDLE

    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
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
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = p.getconn()

        def bad_rollback():
            conn.pgconn.finish()
            orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        pid = conn.pgconn.backend_pid
        with pytest.raises(psycopg.ProgrammingError):
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
    with pool.ConnectionPool(dsn, min_size=1) as p:
        conn = psycopg.connect(dsn)
        with pytest.raises(ValueError):
            p.putconn(conn)


def test_putconn_wrong_pool(dsn):
    with pool.ConnectionPool(dsn, min_size=1) as p1:
        with pool.ConnectionPool(dsn, min_size=1) as p2:
            conn = p1.getconn()
            with pytest.raises(ValueError):
                p2.putconn(conn)


def test_del_no_warning(dsn, recwarn):
    p = pool.ConnectionPool(dsn, min_size=2)
    with p.connection() as conn:
        conn.execute("select 1")

    p.wait()
    ref = weakref.ref(p)
    del p
    assert not ref()
    assert not recwarn, [str(w.message) for w in recwarn.list]


@pytest.mark.slow
def test_del_stop_threads(dsn):
    p = pool.ConnectionPool(dsn)
    ts = [p._sched_runner] + p._workers
    del p
    sleep(0.1)
    for t in ts:
        assert not t.is_alive()


def test_closed_getconn(dsn):
    p = pool.ConnectionPool(dsn, min_size=1)
    assert not p.closed
    with p.connection():
        pass

    p.close()
    assert p.closed

    with pytest.raises(pool.PoolClosed):
        with p.connection():
            pass


def test_closed_putconn(dsn):
    p = pool.ConnectionPool(dsn, min_size=1)

    with p.connection() as conn:
        pass
    assert not conn.closed

    with p.connection() as conn:
        p.close()
    assert conn.closed


@pytest.mark.slow
def test_closed_queue(dsn, retries):
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

    for retry in retries:
        with retry:
            p = pool.ConnectionPool(dsn, min_size=1)
            success: List[str] = []

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
@pytest.mark.timing
def test_grow(dsn, monkeypatch, retries):
    delay_connection(monkeypatch, 0.1)

    def worker(n):
        t0 = time()
        with p.connection() as conn:
            conn.execute("select 1 from pg_sleep(0.2)")
        t1 = time()
        results.append((n, t1 - t0))

    for retry in retries:
        with retry:
            with pool.ConnectionPool(
                dsn, min_size=2, max_size=4, num_workers=3
            ) as p:
                p.wait(1.0)
                results: List[Tuple[int, float]] = []

                ts = [Thread(target=worker, args=(i,)) for i in range(6)]
                for t in ts:
                    t.start()
                for t in ts:
                    t.join()

            want_times = [0.2, 0.2, 0.3, 0.4, 0.4, 0.4]
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

        ts = [Thread(target=worker, args=(i,)) for i in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        sleep(1)

    assert results == [(4, 4), (4, 3), (3, 2), (2, 2), (2, 2)]


@pytest.mark.slow
def test_reconnect(proxy, caplog, monkeypatch, retries):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    assert pool.base.ConnectionAttempt.INITIAL_DELAY == 1.0
    assert pool.base.ConnectionAttempt.DELAY_JITTER == 0.1
    monkeypatch.setattr(pool.base.ConnectionAttempt, "INITIAL_DELAY", 0.1)
    monkeypatch.setattr(pool.base.ConnectionAttempt, "DELAY_JITTER", 0.0)

    for retry in retries:
        with retry:
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
            deltas = [
                times[i + 1] - times[i] for i in range(1, len(times) - 1)
            ]
            assert len(deltas) == 3
            want = 0.1
            for delta in deltas:
                assert delta == pytest.approx(want, 0.05), deltas
                want *= 2


@pytest.mark.slow
@pytest.mark.timing
def test_reconnect_failure(proxy):
    proxy.start()

    t1 = None

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
def test_uniform_use(dsn, retries):
    for retry in retries:
        with retry:
            with pool.ConnectionPool(dsn, min_size=4) as p:
                counts = Counter()  # type: Counter[int]
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
        s = Thread(target=sampler)
        s.start()

        sleep(0.3)
        c = Thread(target=client, args=(0.4,))
        c.start()

        sleep(0.2)
        p.resize(4)
        assert p.min_size == 4
        assert p.max_size == 4

        sleep(0.4)
        p.resize(2)
        assert p.min_size == 2
        assert p.max_size == 2

        sleep(0.6)

    s.join()
    assert size == [2, 1, 3, 4, 3, 2, 2]


def test_jitter():
    rnds = [pool.ConnectionPool._jitter(30, -0.1, +0.2) for i in range(100)]
    rnds.sort()
    assert 27 <= min(rnds) <= 28
    assert 35 < max(rnds) < 36


@pytest.mark.slow
@pytest.mark.timing
def test_max_lifetime(dsn):
    with pool.ConnectionPool(dsn, min_size=1, max_lifetime=0.2) as p:
        sleep(0.1)
        pids = []
        for i in range(5):
            with p.connection() as conn:
                pids.append(conn.pgconn.backend_pid)
            sleep(0.2)

    assert pids[0] == pids[1] != pids[4], pids


def test_check(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    with pool.ConnectionPool(dsn, min_size=4) as p:
        p.wait(1.0)
        with p.connection() as conn:
            pid = conn.pgconn.backend_pid

        p.wait(1.0)
        pids = set(conn.pgconn.backend_pid for conn in p._pool)
        assert pid in pids
        conn.close()

        assert len(caplog.records) == 0
        p.check()
        assert len(caplog.records) == 1
        p.wait(1.0)
        pids2 = set(conn.pgconn.backend_pid for conn in p._pool)
        assert len(pids & pids2) == 3
        assert pid not in pids2


@pytest.mark.skipif(
    sys.version_info >= (3, 7), reason="async pool supported from Python 3.7"
)
def test_async_pool_not_supported(dsn):
    # note: this test is here because the all the ones in test_pool_async are
    # skipped on Py 3.6
    with pytest.raises(psycopg.NotSupportedError):
        pool.AsyncConnectionPool(dsn)


@pytest.mark.slow
@pytest.mark.timing
def test_stats_measures(dsn):
    def worker(n):
        with p.connection() as conn:
            conn.execute("select pg_sleep(0.2)")

    with pool.ConnectionPool(dsn, min_size=2, max_size=4) as p:
        p.wait(2.0)

        stats = p.get_stats()
        assert stats["pool_min"] == 2
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 2
        assert stats["pool_available"] == 2
        assert stats["requests_waiting"] == 0

        ts = [Thread(target=worker, args=(i,)) for i in range(3)]
        for t in ts:
            t.start()
        sleep(0.1)
        stats = p.get_stats()
        for t in ts:
            t.join()
        assert stats["pool_min"] == 2
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
        assert stats["pool_min"] == 2
        assert stats["pool_max"] == 4
        assert stats["pool_size"] == 4
        assert stats["pool_available"] == 0
        assert stats["requests_waiting"] == 3


@pytest.mark.slow
@pytest.mark.timing
def test_stats_usage(dsn, retries):
    def worker(n):
        try:
            with p.connection(timeout=0.3) as conn:
                conn.execute("select pg_sleep(0.2)")
        except pool.PoolTimeout:
            pass

    for retry in retries:
        with retry:
            with pool.ConnectionPool(dsn, min_size=3) as p:
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
    with pool.ConnectionPool(proxy.client_dsn, min_size=3) as p:
        p.wait()
        stats = p.get_stats()
        assert stats["connections_num"] == 3
        assert stats.get("connections_errors", 0) == 0
        assert stats.get("connections_lost", 0) == 0
        assert 600 <= stats["connections_ms"] < 1200

        proxy.stop()
        p.check()
        sleep(0.1)
        stats = p.get_stats()
        assert stats["connections_num"] > 3
        assert stats["connections_errors"] > 0
        assert stats["connections_lost"] == 3


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

        ts = [Thread(target=worker) for i in range(50)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        p.wait()

        assert len(p._pool) < 7


def delay_connection(monkeypatch, sec):
    """
    Return a _connect_gen function delayed by the amount of seconds
    """

    def connect_delay(*args, **kwargs):
        t0 = time()
        rv = connect_orig(*args, **kwargs)
        t1 = time()
        sleep(sec - (t1 - t0))
        return rv

    connect_orig = psycopg.Connection.connect
    monkeypatch.setattr(psycopg.Connection, "connect", connect_delay)
