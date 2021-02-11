import gc
import pickle
import weakref
import datetime as dt

import pytest

import psycopg3
from psycopg3 import sql
from psycopg3.oids import postgres_types as builtins
from psycopg3.adapt import Format


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
    cur = conn.cursor(binary=True)
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


def test_executemany_returning_rowcount(conn, execmany):
    cur = conn.cursor()
    cur.executemany(
        "insert into execmany(num, data) values (%s, %s) returning num",
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


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_executemany_null_first(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table testmany (a bigint, b bigint)")
    cur.executemany(
        f"insert into testmany values (%{fmt_in}, %{fmt_in})",
        [[1, None], [3, 4]],
    )
    with pytest.raises((psycopg3.DataError, psycopg3.ProgrammingError)):
        cur.executemany(
            f"insert into testmany values (%{fmt_in}, %{fmt_in})",
            [[1, ""], [3, 4]],
        )


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


def test_rownumber(conn):
    cur = conn.cursor()
    assert cur.rownumber is None

    cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rownumber == 0

    cur.fetchone()
    assert cur.rownumber == 1
    cur.fetchone()
    assert cur.rownumber == 2
    cur.fetchmany(10)
    assert cur.rownumber == 12
    rns = []
    for i in cur:
        rns.append(cur.rownumber)
        if len(rns) >= 3:
            break
    assert rns == [13, 14, 15]
    assert len(cur.fetchall()) == 42 - rns[-1]
    assert cur.rownumber == 42


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


def test_scroll(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg3.ProgrammingError):
        cur.scroll(0)

    cur.execute("select generate_series(0,9)")
    cur.scroll(2)
    assert cur.fetchone() == (2,)
    cur.scroll(2)
    assert cur.fetchone() == (5,)
    cur.scroll(2, mode="relative")
    assert cur.fetchone() == (8,)
    cur.scroll(-1)
    assert cur.fetchone() == (8,)
    cur.scroll(-2)
    assert cur.fetchone() == (7,)
    cur.scroll(2, mode="absolute")
    assert cur.fetchone() == (2,)

    # on the boundary
    cur.scroll(0, mode="absolute")
    assert cur.fetchone() == (0,)
    with pytest.raises(IndexError):
        cur.scroll(-1, mode="absolute")

    cur.scroll(0, mode="absolute")
    with pytest.raises(IndexError):
        cur.scroll(-1)

    cur.scroll(9, mode="absolute")
    assert cur.fetchone() == (9,)
    with pytest.raises(IndexError):
        cur.scroll(10, mode="absolute")

    cur.scroll(9, mode="absolute")
    with pytest.raises(IndexError):
        cur.scroll(1)

    with pytest.raises(ValueError):
        cur.scroll(1, "wat")


def test_query_params_execute(conn):
    cur = conn.cursor()
    assert cur.query is None
    assert cur.params is None

    cur.execute("select %t, %s::text", [1, None])
    assert cur.query == b"select $1, $2::text"
    assert cur.params == [b"1", None]

    cur.execute("select 1")
    assert cur.query == b"select 1"
    assert cur.params is None

    with pytest.raises(psycopg3.DataError):
        cur.execute("select %t::int", ["wat"])

    assert cur.query == b"select $1::int"
    assert cur.params == [b"wat"]


def test_query_params_executemany(conn):
    cur = conn.cursor()

    cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur.query == b"select $1, $2"
    assert cur.params == [b"3", b"4"]

    with pytest.raises((psycopg3.DataError, TypeError)):
        cur.executemany("select %t::int", [[1], ["x"], [2]])
    assert cur.query == b"select $1::int"
    # TODO: cannot really check this: after introduced row_dumpers, this
    # fails dumping, not query passing.
    # assert cur.params == [b"x"]


def test_stream(conn):
    cur = conn.cursor()
    recs = []
    for rec in cur.stream(
        "select i, '2021-01-01'::date + i from generate_series(1, %s) as i",
        [2],
    ):
        recs.append(rec)

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


def test_stream_sql(conn):
    cur = conn.cursor()
    recs = list(
        cur.stream(
            sql.SQL(
                "select i, '2021-01-01'::date + i from generate_series(1, {}) as i"
            ).format(2)
        )
    )

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


@pytest.mark.parametrize(
    "query",
    [
        "create table test_stream_badq ()",
        "copy (select 1) to stdout",
        "wat?",
    ],
)
def test_stream_badquery(conn, query):
    cur = conn.cursor()
    with pytest.raises(psycopg3.ProgrammingError):
        for rec in cur.stream(query):
            pass


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


def test_str(conn):
    cur = conn.cursor()
    assert "[IDLE]" in str(cur)
    assert "[closed]" not in str(cur)
    assert "[no result]" in str(cur)
    cur.execute("select 1")
    assert "[INTRANS]" in str(cur)
    assert "[TUPLES_OK]" in str(cur)
    assert "[closed]" not in str(cur)
    assert "[no result]" not in str(cur)
    cur.close()
    assert "[closed]" in str(cur)
    assert "[INTRANS]" in str(cur)


@pytest.mark.slow
@pytest.mark.parametrize("fmt", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
def test_leak(dsn, faker, fmt, fetch):
    faker.format = fmt
    faker.choose_schema(ncols=5)
    faker.make_records(10)

    n = []
    for i in range(3):
        with psycopg3.connect(dsn) as conn:
            with conn.cursor(binary=Format.as_pq(fmt)) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)
                cur.executemany(faker.insert_stmt, faker.records)
                cur.execute(faker.select_stmt)

                if fetch == "one":
                    while 1:
                        tmp = cur.fetchone()
                        if tmp is None:
                            break
                elif fetch == "many":
                    while 1:
                        tmp = cur.fetchmany(3)
                        if not tmp:
                            break
                elif fetch == "all":
                    cur.fetchall()
                elif fetch == "iter":
                    for rec in cur:
                        pass

                tmp = None

        del cur, conn
        gc.collect()
        gc.collect()
        n.append(len(gc.get_objects()))

    assert (
        n[0] == n[1] == n[2]
    ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"
