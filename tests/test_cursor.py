import gc
import pickle
import weakref

import pytest

import psycopg3
from psycopg3.oids import builtins


def test_close(conn):
    cur = conn.cursor()
    assert not cur.closed
    cur.close()
    assert cur.closed

    with pytest.raises(psycopg3.InterfaceError):
        cur.execute("select 'foo'")

    cur.close()
    assert cur.closed


def test_context(conn):
    with conn.cursor() as cur:
        assert not cur.closed

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

    rv = cur.execute("select 'foo'; select generate_series(1,3)")
    assert rv is cur
    assert cur.fetchall() == [("foo",)]
    assert cur.rowcount == 1
    assert cur.nextset()
    assert cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.nextset() is None

    cur.close()
    assert cur.nextset() is None


def test_execute_sequence(conn):
    cur = conn.cursor()
    rv = cur.execute("select %s::int, %s::text, %s::text", [1, "foo", None])
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


@pytest.mark.parametrize(
    "query", ["copy testcopy from stdin", "copy testcopy to stdout"]
)
def test_execute_copy(conn, query):
    cur = conn.cursor()
    cur.execute("create table testcopy (id int)")
    with pytest.raises(psycopg3.ProgrammingError):
        cur.execute(query)


def test_fetchone(conn):
    cur = conn.cursor()
    cur.execute("select %s::int, %s::text, %s::text", [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = cur.fetchone()
    assert row[0] == 1
    assert row[1] == "foo"
    assert row[2] is None
    row = cur.fetchone()
    assert row is None


def test_execute_binary_result(conn):
    cur = conn.cursor(format=psycopg3.pq.Format.BINARY)
    cur.execute("select %s::text, %s::text", ["foo", None])
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


def test_rowcount(conn):
    cur = conn.cursor()

    cur.execute("select 1 from generate_series(1, 0)")
    assert cur.rowcount == 0

    cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rowcount == 42

    cur.execute("create table test_rowcount_notuples (id int primary key)")
    assert cur.rowcount == -1

    cur.execute(
        "insert into test_rowcount_notuples select generate_series(1, 42)"
    )
    assert cur.rowcount == 42

    cur.close()
    assert cur.rowcount == -1


def test_iter(conn):
    cur = conn.cursor()
    cur.execute("select generate_series(1, 3)")
    assert list(cur) == [(1,), (2,), (3,)]


def test_iter_stop(conn):
    cur = conn.cursor()
    cur.execute("select generate_series(1, 3)")
    for rec in cur:
        assert rec == (1,)
        break

    for rec in cur:
        assert rec == (2,)
        break

    assert cur.fetchone() == (3,)
    assert list(cur) == []


def test_query_params_execute(conn):
    cur = conn.cursor()
    assert cur.query is None
    assert cur.params is None

    cur.execute("select %s, %s::text", [1, None])
    assert cur.query == b"select $1, $2::text"
    assert cur.params == [b"1", None]

    cur.execute("select 1")
    assert cur.query == b"select 1"
    assert cur.params is None

    with pytest.raises(psycopg3.DataError):
        cur.execute("select %s::int", ["wat"])

    assert cur.query == b"select $1::int"
    assert cur.params == [b"wat"]


def test_query_params_executemany(conn):
    cur = conn.cursor()

    cur.executemany("select %s, %s", [[1, 2], [3, 4]])
    assert cur.query == b"select $1, $2"
    assert cur.params == [b"3", b"4"]

    with pytest.raises(psycopg3.DataError):
        cur.executemany("select %s::int", [[1], ["x"], [2]])
    assert cur.query == b"select $1::int"
    assert cur.params == [b"x"]


class TestColumn:
    def test_description_attribs(self, conn):
        curs = conn.cursor()
        curs.execute(
            """select
            3.14::decimal(10,2) as pi,
            'hello'::text as hi,
            '2010-02-18'::date as now
            """
        )
        assert len(curs.description) == 3
        for c in curs.description:
            len(c) == 7  # DBAPI happy
            for i, a in enumerate(
                """
                name type_code display_size internal_size precision scale null_ok
                """.split()
            ):
                assert c[i] == getattr(c, a)

            # Won't fill them up
            assert c.null_ok is None

        c = curs.description[0]
        assert c.name == "pi"
        assert c.type_code == builtins["numeric"].oid
        assert c.display_size is None
        assert c.internal_size is None
        assert c.precision == 10
        assert c.scale == 2

        c = curs.description[1]
        assert c.name == "hi"
        assert c.type_code == builtins["text"].oid
        assert c.display_size is None
        assert c.internal_size is None
        assert c.precision is None
        assert c.scale is None

        c = curs.description[2]
        assert c.name == "now"
        assert c.type_code == builtins["date"].oid
        assert c.display_size is None
        assert c.internal_size == 4
        assert c.precision is None
        assert c.scale is None

    def test_description_slice(self, conn):
        curs = conn.cursor()
        curs.execute("select 1::int as a")
        curs.description[0][0:2] == ("a", 23)

    @pytest.mark.parametrize(
        "type, precision, scale, dsize, isize",
        [
            ("text", None, None, None, None),
            ("varchar", None, None, None, None),
            ("varchar(42)", None, None, 42, None),
            ("int4", None, None, None, 4),
            ("numeric", None, None, None, None),
            ("numeric(10)", 10, 0, None, None),
            ("numeric(10, 3)", 10, 3, None, None),
            ("time", None, None, None, 8),
            ("time(4)", 4, None, None, 8),
            ("time(10)", 6, None, None, 8),
        ],
    )
    def test_details(self, conn, type, precision, scale, dsize, isize):
        cur = conn.cursor()
        cur.execute(f"select null::{type}")
        col = cur.description[0]
        repr(col)
        assert col.precision == precision
        assert col.scale == scale
        assert col.display_size == dsize
        assert col.internal_size == isize

    def test_pickle(self, conn):
        curs = conn.cursor()
        curs.execute(
            """select
            3.14::decimal(10,2) as pi,
            'hello'::text as hi,
            '2010-02-18'::date as now
            """
        )
        description = curs.description
        pickled = pickle.dumps(description, pickle.HIGHEST_PROTOCOL)
        unpickled = pickle.loads(pickled)
        assert [tuple(d) for d in description] == [tuple(d) for d in unpickled]
