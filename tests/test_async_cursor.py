import pytest
import psycopg3


def test_close(aconn, loop):
    cur = aconn.cursor()
    assert not cur.closed
    loop.run_until_complete(cur.close())
    assert cur.closed

    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(cur.execute("select 'foo'"))

    loop.run_until_complete(cur.close())
    assert cur.closed


def test_status(aconn, loop):
    cur = aconn.cursor()
    assert cur.status is None
    loop.run_until_complete(cur.execute("reset all"))
    assert cur.status == cur.ExecStatus.COMMAND_OK
    loop.run_until_complete(cur.execute("select 1"))
    assert cur.status == cur.ExecStatus.TUPLES_OK
    loop.run_until_complete(cur.close())
    assert cur.status is None


def test_execute_many_results(aconn, loop):
    cur = aconn.cursor()
    assert cur.nextset() is None

    rv = loop.run_until_complete(cur.execute("select 'foo'; select 'bar'"))
    assert rv is cur
    assert len(cur._results) == 2
    assert cur.pgresult.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur.pgresult.get_value(0, 0) == b"bar"
    assert cur.nextset() is None

    loop.run_until_complete(cur.close())
    assert cur.nextset() is None


def test_execute_sequence(aconn, loop):
    cur = aconn.cursor()
    rv = loop.run_until_complete(
        cur.execute("select %s, %s, %s", [1, "foo", None])
    )
    assert rv is cur
    assert len(cur._results) == 1
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.pgresult.get_value(0, 1) == b"foo"
    assert cur.pgresult.get_value(0, 2) is None
    assert cur.nextset() is None


@pytest.mark.parametrize("query", ["", " ", ";"])
def test_execute_empty_query(aconn, loop, query):
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute(query))
    assert cur.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg3.ProgrammingError):
        loop.run_until_complete(cur.fetchone())


def test_fetchone(aconn, loop):
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("select %s, %s, %s", [1, "foo", None]))
    assert cur.pgresult.fformat(0) == 0

    row = loop.run_until_complete(cur.fetchone())
    assert row[0] == 1
    assert row[1] == "foo"
    assert row[2] is None
    row = loop.run_until_complete(cur.fetchone())
    assert row is None


def test_execute_binary_result(aconn, loop):
    cur = aconn.cursor(binary=True)
    loop.run_until_complete(cur.execute("select %s, %s", ["foo", None]))
    assert cur.pgresult.fformat(0) == 1

    row = loop.run_until_complete(cur.fetchone())
    assert row[0] == "foo"
    assert row[1] is None
    row = loop.run_until_complete(cur.fetchone())
    assert row is None


@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_query_encode(aconn, loop, encoding):
    loop.run_until_complete(aconn.set_client_encoding(encoding))
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("select '\u20ac'"))
    (res,) = loop.run_until_complete(cur.fetchone())
    assert res == "\u20ac"


def test_query_badenc(aconn, loop):
    loop.run_until_complete(aconn.set_client_encoding("latin1"))
    cur = aconn.cursor()
    with pytest.raises(UnicodeEncodeError):
        loop.run_until_complete(cur.execute("select '\u20ac'"))


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


def test_executemany(aconn, loop, execmany):
    cur = aconn.cursor()
    loop.run_until_complete(
        cur.executemany(
            "insert into execmany(num, data) values (%s, %s)",
            [(10, "hello"), (20, "world")],
        )
    )
    loop.run_until_complete(
        cur.execute("select num, data from execmany order by 1")
    )
    rv = loop.run_until_complete(cur.fetchall())
    assert rv == [(10, "hello"), (20, "world")]


def test_executemany_name(aconn, loop, execmany):
    cur = aconn.cursor()
    loop.run_until_complete(
        cur.executemany(
            "insert into execmany(num, data) values (%(num)s, %(data)s)",
            [
                {"num": 11, "data": "hello", "x": 1},
                {"num": 21, "data": "world"},
            ],
        )
    )
    loop.run_until_complete(
        cur.execute("select num, data from execmany order by 1")
    )
    rv = loop.run_until_complete(cur.fetchall())
    assert rv == [(11, "hello"), (21, "world")]


@pytest.mark.xfail
def test_executemany_rowcount(aconn, loop, execmany):
    cur = aconn.cursor()
    loop.run_until_complete(
        cur.executemany(
            "insert into execmany(num, data) values (%s, %s)",
            [(10, "hello"), (20, "world")],
        )
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
def test_executemany_badquery(aconn, loop, query):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        loop.run_until_complete(
            cur.executemany(query, [(10, "hello"), (20, "world")])
        )
