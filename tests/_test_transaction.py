import sys
import pytest
import psycopg


# TODOCRDB: is this the expected behaviour?
crdb_skip_external_observer = pytest.mark.crdb(
    "skip", reason="deadlock on observer connection"
)


@pytest.fixture(autouse=True)
def create_test_table(svcconn):
    """Creates a table called 'test_table' for use in tests."""
    cur = svcconn.cursor()
    cur.execute("drop table if exists test_table")
    cur.execute("create table test_table (id text primary key)")
    yield
    cur.execute("drop table test_table")


def insert_row(conn, value):
    sql = "INSERT INTO test_table VALUES (%s)"
    if isinstance(conn, psycopg.Connection):
        conn.cursor().execute(sql, (value,))
    else:

        async def f():
            cur = conn.cursor()
            await cur.execute(sql, (value,))

        return f()


def inserted(conn):
    """Return the values inserted in the test table."""
    sql = "SELECT * FROM test_table"
    if isinstance(conn, psycopg.Connection):
        rows = conn.cursor().execute(sql).fetchall()
        return set(v for (v,) in rows)
    else:

        async def f():
            cur = conn.cursor()
            await cur.execute(sql)
            rows = await cur.fetchall()
            return set(v for (v,) in rows)

        return f()


def in_transaction(conn):
    if conn.pgconn.transaction_status == conn.TransactionStatus.IDLE:
        return False
    elif conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS:
        return True
    else:
        assert False, conn.pgconn.transaction_status


def get_exc_info(exc):
    """Return the exc info for an exception or a success if exc is None"""
    if not exc:
        return (None,) * 3
    try:
        raise exc
    except exc:
        return sys.exc_info()


class ExpectedException(Exception):
    pass
