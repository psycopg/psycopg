import gc
import pytest
import weakref

import psycopg3


def test_close(conn):
    cur = conn.cursor()
    assert not cur.closed
    cur.close()
    assert cur.closed

    with pytest.raises(psycopg3.OperationalError):
        cur.execute("select 'foo'")

    cur.close()
    assert cur.closed


def test_weakref(conn):
    cur = conn.cursor()
    w = weakref.ref(cur)
    cur.close()
    del cur
    gc.collect()
    assert w() is None


def test_status(conn):
    cur = conn.cursor()
    assert cur.status is None
    cur.execute("reset all")
    assert cur.status == cur.ExecStatus.COMMAND_OK
    cur.execute("select 1")
    assert cur.status == cur.ExecStatus.TUPLES_OK
    cur.close()
    assert cur.status is None


def test_execute_many_results(conn):
    cur = conn.cursor()
    assert cur.nextset() is None

    rv = cur.execute("select 'foo'; select 'bar'")
    assert rv is cur
    assert len(cur._results) == 2
    assert cur.pgresult.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur.pgresult.get_value(0, 0) == b"bar"
    assert cur.nextset() is None

    cur.close()
    assert cur.nextset() is None


def test_execute_sequence(conn):
    cur = conn.cursor()
    rv = cur.execute("select %s, %s, %s", [1, "foo", None])
    assert rv is cur
    assert len(cur._results) == 1
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.pgresult.get_value(0, 1) == b"foo"
    assert cur.pgresult.get_value(0, 2) is None
    assert cur.nextset() is None


@pytest.mark.parametrize("query", ["", " ", ";"])
def test_execute_empty_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    assert cur.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg3.ProgrammingError):
        cur.fetchone()


def test_fetchone(conn):
    cur = conn.cursor()
    cur.execute("select %s, %s, %s", [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = cur.fetchone()
    assert row[0] == 1
    assert row[1] == "foo"
    assert row[2] is None
    row = cur.fetchone()
    assert row is None


def test_execute_binary_result(conn):
    cur = conn.cursor(format=psycopg3.pq.Format.BINARY)
    cur.execute("select %s, %s", ["foo", None])
    assert cur.pgresult.fformat(0) == 1

    row = cur.fetchone()
    assert row[0] == "foo"
    assert row[1] is None
    row = cur.fetchone()
    assert row is None


@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_query_encode(conn, encoding):
    conn.client_encoding = encoding
    cur = conn.cursor()
    (res,) = cur.execute("select '\u20ac'").fetchone()
    assert res == "\u20ac"


def test_query_badenc(conn):
    conn.client_encoding = "latin1"
    cur = conn.cursor()
    with pytest.raises(UnicodeEncodeError):
        cur.execute("select '\u20ac'")


@pytest.fixture(scope="session")
def _execmany(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        drop table if exists execmany;
        create table execmany (id serial primary key, num integer, data text)
        """
    )


@pytest.fixture(scope="function")
def execmany(svcconn, _execmany):
    cur = svcconn.cursor()
    cur.execute("truncate table execmany")


def test_executemany(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        "insert into execmany(num, data) values (%s, %s)",
        [(10, "hello"), (20, "world")],
    )
    cur.execute("select num, data from execmany order by 1")
    assert cur.fetchall() == [(10, "hello"), (20, "world")]


def test_executemany_name(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        "insert into execmany(num, data) values (%(num)s, %(data)s)",
        [{"num": 11, "data": "hello", "x": 1}, {"num": 21, "data": "world"}],
    )
    cur.execute("select num, data from execmany order by 1")
    assert cur.fetchall() == [(11, "hello"), (21, "world")]


@pytest.mark.xfail
def test_executemany_rowcount(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        "insert into execmany(num, data) values (%s, %s)",
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2


@pytest.mark.parametrize(
    "query",
    [
        "insert into nosuchtable values (%s, %s)",
        "copy (select %s, %s) to stdout",
        "wat (%s, %s)",
    ],
)
def test_executemany_badquery(conn, query):
    cur = conn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        cur.executemany(query, [(10, "hello"), (20, "world")])
