import gc
import pickle
import weakref
import datetime as dt

import pytest

import psycopg
from psycopg import pq, sql, rows
from psycopg.adapt import PyFormat
from psycopg.postgres import types as builtins

from .utils import gc_collect


def test_close(conn):
    cur = conn.cursor()
    assert not cur.closed
    cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
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
    gc_collect()
    assert w() is None


def test_statusmessage(conn):
    cur = conn.cursor()
    assert cur.statusmessage is None

    cur.execute("select generate_series(1, 10)")
    assert cur.statusmessage == "SELECT 10"

    cur.execute("create table statusmessage ()")
    assert cur.statusmessage == "CREATE TABLE"

    with pytest.raises(psycopg.ProgrammingError):
        cur.execute("wat")
    assert cur.statusmessage is None


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
    assert cur.pgresult.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()


@pytest.mark.parametrize(
    "query", ["copy testcopy from stdin", "copy testcopy to stdout"]
)
def test_execute_copy(conn, query):
    cur = conn.cursor()
    cur.execute("create table testcopy (id int)")
    with pytest.raises(psycopg.ProgrammingError):
        cur.execute(query)


def test_fetchone(conn):
    cur = conn.cursor()
    cur.execute("select %s::int, %s::text, %s::text", [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = cur.fetchone()
    assert row == (1, "foo", None)
    row = cur.fetchone()
    assert row is None


def test_binary_cursor_execute(conn):
    cur = conn.cursor(binary=True)
    cur.execute("select %s, %s", [1, None])
    assert cur.fetchone() == (1, None)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x01"


def test_execute_binary(conn):
    cur = conn.cursor()
    cur.execute("select %s, %s", [1, None], binary=True)
    assert cur.fetchone() == (1, None)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x01"


def test_binary_cursor_text_override(conn):
    cur = conn.cursor(binary=True)
    cur.execute("select %s, %s", [1, None], binary=False)
    assert cur.fetchone() == (1, None)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"


@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_query_encode(conn, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    cur = conn.cursor()
    (res,) = cur.execute("select '\u20ac'").fetchone()
    assert res == "\u20ac"


def test_query_badenc(conn):
    conn.execute("set client_encoding to latin1")
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
    with pytest.raises(psycopg.DatabaseError):
        cur.executemany(query, [(10, "hello"), (20, "world")])


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_executemany_null_first(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table testmany (a bigint, b bigint)")
    cur.executemany(
        f"insert into testmany values (%{fmt_in}, %{fmt_in})",
        [[1, None], [3, 4]],
    )
    with pytest.raises((psycopg.DataError, psycopg.ProgrammingError)):
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
    assert cur.rowcount == 42


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


def test_row_factory(conn):
    cur = conn.cursor(row_factory=my_row_factory)

    cur.execute("reset search_path")
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()

    cur.execute("select 'foo' as bar")
    (r,) = cur.fetchone()
    assert r == "FOObar"

    cur.execute("select 'x' as x; select 'y' as y, 'z' as z")
    assert cur.fetchall() == [["Xx"]]
    assert cur.nextset()
    assert cur.fetchall() == [["Yy", "Zz"]]

    cur.scroll(-1)
    cur.row_factory = rows.dict_row
    assert cur.fetchone() == {"y": "y", "z": "z"}


def test_row_factory_none(conn):
    cur = conn.cursor(row_factory=None)
    assert cur.row_factory is rows.tuple_row
    r = cur.execute("select 1 as a, 2 as b").fetchone()
    assert type(r) is tuple
    assert r == (1, 2)


def test_bad_row_factory(conn):
    def broken_factory(cur):
        1 / 0

    cur = conn.cursor(row_factory=broken_factory)
    with pytest.raises(ZeroDivisionError):
        cur.execute("select 1")

    def broken_maker(cur):
        def make_row(seq):
            1 / 0

        return make_row

    cur = conn.cursor(row_factory=broken_maker)
    cur.execute("select 1")
    with pytest.raises(ZeroDivisionError):
        cur.fetchone()


def test_scroll(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
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
    assert cur._query is None

    cur.execute("select %t, %s::text", [1, None])
    assert cur._query.query == b"select $1, $2::text"
    assert cur._query.params == [b"1", None]

    cur.execute("select 1")
    assert cur._query.query == b"select 1"
    assert not cur._query.params

    with pytest.raises(psycopg.DataError):
        cur.execute("select %t::int", ["wat"])

    assert cur._query.query == b"select $1::int"
    assert cur._query.params == [b"wat"]


def test_query_params_executemany(conn):
    cur = conn.cursor()

    cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur._query.query == b"select $1, $2"
    assert cur._query.params == [b"3", b"4"]


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


def test_stream_row_factory(conn):
    cur = conn.cursor(row_factory=rows.dict_row)
    it = iter(cur.stream("select generate_series(1,2) as a"))
    assert next(it)["a"] == 1
    cur.row_factory = rows.namedtuple_row
    assert next(it).a == 2


def test_stream_no_col(conn):
    cur = conn.cursor()
    it = iter(cur.stream("select"))
    assert list(it) == [()]


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
    with pytest.raises(psycopg.ProgrammingError):
        for rec in cur.stream(query):
            pass


def test_stream_binary_cursor(conn):
    cur = conn.cursor(binary=True)
    recs = []
    for rec in cur.stream("select generate_series(1, 2)"):
        recs.append(rec)
        assert cur.pgresult.fformat(0) == 1
        assert cur.pgresult.get_value(0, 0) == bytes([0, 0, 0, rec[0]])

    assert recs == [(1,), (2,)]


def test_stream_execute_binary(conn):
    cur = conn.cursor()
    recs = []
    for rec in cur.stream("select generate_series(1, 2)", binary=True):
        recs.append(rec)
        assert cur.pgresult.fformat(0) == 1
        assert cur.pgresult.get_value(0, 0) == bytes([0, 0, 0, rec[0]])

    assert recs == [(1,), (2,)]


def test_stream_binary_cursor_text_override(conn):
    cur = conn.cursor(binary=True)
    recs = []
    for rec in cur.stream("select generate_series(1, 2)", binary=False):
        recs.append(rec)
        assert cur.pgresult.fformat(0) == 0
        assert cur.pgresult.get_value(0, 0) == str(rec[0]).encode()

    assert recs == [(1,), (2,)]


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

    def test_no_col_query(self, conn):
        cur = conn.execute("select")
        assert cur.description == []
        assert cur.fetchall() == [()]


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
@pytest.mark.parametrize("fmt", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
@pytest.mark.parametrize(
    "row_factory", ["tuple_row", "dict_row", "namedtuple_row"]
)
def test_leak(dsn, faker, fmt, fmt_out, fetch, row_factory, retries):
    faker.format = fmt
    faker.choose_schema(ncols=5)
    faker.make_records(10)
    row_factory = getattr(rows, row_factory)

    def work():
        with psycopg.connect(dsn) as conn:
            with conn.cursor(binary=fmt_out, row_factory=row_factory) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)
                with faker.find_insert_problem(conn):
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

    for retry in retries:
        with retry:
            n = []
            gc_collect()
            for i in range(3):
                work()
                gc_collect()
                n.append(len(gc.get_objects()))
            assert (
                n[0] == n[1] == n[2]
            ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


def my_row_factory(cursor):
    if cursor.description is not None:
        titles = [c.name for c in cursor.description]

        def mkrow(values):
            return [
                f"{value.upper()}{title}"
                for title, value in zip(titles, values)
            ]

        return mkrow
    else:
        return rows.no_result
