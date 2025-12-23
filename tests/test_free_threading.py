import time
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import psycopg

from ._test_connection import testctx  # noqa: F401  # fixture


@pytest.mark.slow
@pytest.mark.usefixtures("testctx")
def test_concurrent_connection_insert(conn):
    nthreads = 10
    barrier = threading.Barrier(parties=nthreads)

    def worker(i):
        barrier.wait()
        with conn.cursor() as cur:
            cur.execute("insert into testctx values (%s)", (i,))

    with ThreadPoolExecutor(max_workers=nthreads) as tpe:
        futures = [tpe.submit(worker, i) for i in range(100)]
        for future in futures:
            future.result()  # to verify nothing raises

    with conn.cursor() as cur:
        cur.execute("select id from testctx")
        data = set(cur)

    assert data == set((i,) for i in range(100))


@pytest.mark.slow
@pytest.mark.usefixtures("testctx")
def test_concurrent_connection_select(conn):
    nthreads = 10
    barrier = threading.Barrier(parties=nthreads)

    with conn.cursor() as cur:
        cur.execute("insert into testctx values (1), (2), (3)")

    def worker():
        barrier.wait()
        with conn.cursor() as cur:
            cur.execute("select id from testctx")
            assert cur.fetchall() == [(1,), (2,), (3,)]

    with ThreadPoolExecutor(max_workers=nthreads) as tpe:
        futures = [tpe.submit(worker) for _ in range(100)]
        for future in futures:
            future.result()  # to verify nothing raises


@pytest.mark.slow
@pytest.mark.usefixtures("testctx")
def test_concurrent_connection_update(conn):
    nthreads = 10
    barrier = threading.Barrier(parties=nthreads)

    with conn.cursor() as cur:
        cur.execute("insert into testctx values (0)")

    def worker():
        barrier.wait()
        with conn.cursor() as cur:
            cur.execute("update testctx set id = id + 1")

    with ThreadPoolExecutor(max_workers=nthreads) as tpe:
        futures = [tpe.submit(worker) for _ in range(100)]
        for future in futures:
            future.result()  # to verify nothing raises

    with conn.cursor() as cur:
        cur.execute("select id from testctx")
        assert cur.fetchone()[0] == 100


@pytest.mark.slow
@pytest.mark.usefixtures("testctx")
def test_concurrent_connection_cursors_share_transaction_state(conn):
    with conn.cursor() as cur:
        cur.execute("insert into testctx values (1)")
    conn.commit()

    barrier = threading.Barrier(parties=2)
    row_added = threading.Event()
    row_read = threading.Event()
    transaction_rolled_back = threading.Event()

    def writer():
        """Thread that inserts a new row but doesn't commit"""
        barrier.wait()
        with conn.cursor() as cur:
            cur.execute("insert into testctx values (2)")
        row_added.set()
        row_read.wait()
        conn.rollback()
        transaction_rolled_back.set()

    def reader():
        """Thread that should see uncommitted changes from writer"""
        barrier.wait()

        row_added.wait()
        with conn.cursor() as cur:
            cur.execute("select id from testctx order by id")
            data = [row[0] for row in cur.fetchall()]
            reader_saw = data
        row_read.set()
        transaction_rolled_back.wait()
        with conn.cursor() as cur:
            cur.execute("select id from testctx order by id")
            assert [row[0] for row in cur.fetchall()] == [1]

        return reader_saw

    with ThreadPoolExecutor(max_workers=2) as tpe:
        t1 = tpe.submit(writer)
        t2 = tpe.submit(reader)
        t1.result()  # No exception
        assert t2.result() == [1, 2]  # No exception + correct data


@pytest.mark.slow
@pytest.mark.usefixtures("testctx")
def test_error_in_one_cursor_affects_all_cursors(conn):
    with conn.cursor() as cur:
        cur.execute("insert into testctx values (1)")
    conn.commit()

    error_happened = threading.Event()

    def cause_error():
        with pytest.raises(psycopg.errors.UndefinedTable):
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM nonexistent_table")
        error_happened.set()

    def try_query_after_error():
        error_happened.wait()

        with pytest.raises(psycopg.errors.InFailedSqlTransaction):
            with conn.cursor() as cur:
                cur.execute("select id from testctx")

        # After rollback, should work again
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("select id from testctx")
            assert [row[0] for row in cur.fetchall()] == [1]

    with ThreadPoolExecutor(max_workers=2) as tpe:
        t1 = tpe.submit(cause_error)
        t2 = tpe.submit(try_query_after_error)
        t1.result()
        t2.result()


@pytest.mark.slow
def test_same_cursor_from_multiple_threads_no_crash(conn):
    """
    This is only there to verify that there's no hard crash.
    All exceptions are fine.
    """
    nthreads = 10
    barrier = threading.Barrier(parties=nthreads)

    cur = conn.cursor()

    def worker():
        """Multiple threads trying to use the same cursor"""
        barrier.wait()
        try:
            cur.execute("select 1")
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=nthreads) as tpe:
        futures = [tpe.submit(worker) for _ in range(100)]
        for future in futures:
            future.result()

    cur.close()


@pytest.mark.slow
@pytest.mark.usefixtures("testctx")
def test_connection_finish_while_executing(conn):
    with conn.cursor() as cur:
        cur.execute("insert into testctx values (1)")
    conn.commit()

    def closer():
        time.sleep(1)
        conn.close()

    def reader():
        cur = conn.cursor()
        try:
            while True:
                cur.execute("select id from testctx")
                assert [row[0] for row in cur.fetchall()] == [1]
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=2) as tpe:
        future2 = tpe.submit(reader)
        future1 = tpe.submit(closer)
        future1.result()
        future2.result()


@pytest.mark.slow
def test_connection_close_race_condition(dsn):
    conn = psycopg.connect(dsn, autocommit=True)
    barrier = threading.Barrier(parties=2)

    def reader():
        barrier.wait()
        messages = [conn.pgconn.error_message for _ in range(100)]
        return messages

    def closer():
        barrier.wait()
        conn.pgconn.finish()

    with ThreadPoolExecutor(max_workers=2) as tpe:
        reader_future = tpe.submit(reader)
        closer_future = tpe.submit(closer)
        error_messages = reader_future.result()
        closer_future.result()

    for error_message in error_messages:
        assert error_message in (b"", b"connection pointer is NULL\n")
