import pytest

import psycopg
from psycopg import rows, errors as e
from psycopg.pq import Format

pytestmark = pytest.mark.crdb_skip("server-side cursor")


def test_init_row_factory(conn):
    with psycopg.ServerCursor(conn, "foo") as cur:
        assert cur.name == "foo"
        assert cur.connection is conn
        assert cur.row_factory is conn.row_factory

    conn.row_factory = rows.dict_row

    with psycopg.ServerCursor(conn, "bar") as cur:
        assert cur.name == "bar"
        assert cur.row_factory is rows.dict_row  # type: ignore

    with psycopg.ServerCursor(conn, "baz", row_factory=rows.namedtuple_row) as cur:
        assert cur.name == "baz"
        assert cur.row_factory is rows.namedtuple_row  # type: ignore


def test_init_params(conn):
    with psycopg.ServerCursor(conn, "foo") as cur:
        assert cur.scrollable is None
        assert cur.withhold is False

    with psycopg.ServerCursor(conn, "bar", withhold=True, scrollable=False) as cur:
        assert cur.scrollable is False
        assert cur.withhold is True


@pytest.mark.crdb_skip("cursor invalid name")
def test_funny_name(conn):
    cur = conn.cursor("1-2-3")
    cur.execute("select generate_series(1, 3) as bar")
    assert cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.name == "1-2-3"
    cur.close()


def test_repr(conn):
    cur = conn.cursor("my-name")
    assert "psycopg.ServerCursor" in str(cur)
    assert "my-name" in repr(cur)
    cur.close()


def test_connection(conn):
    cur = conn.cursor("foo")
    assert cur.connection is conn
    cur.close()


def test_description(conn):
    cur = conn.cursor("foo")
    assert cur.name == "foo"
    cur.execute("select generate_series(1, 10)::int4 as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0
    cur.close()


def test_format(conn):
    cur = conn.cursor("foo")
    assert cur.format == Format.TEXT
    cur.close()

    cur = conn.cursor("foo", binary=True)
    assert cur.format == Format.BINARY
    cur.close()


def test_query_params(conn):
    with conn.cursor("foo") as cur:
        assert cur._query is None
        cur.execute("select generate_series(1, %s) as bar", (3,))
        assert cur._query
        assert b"declare" in cur._query.query.lower()
        assert b"(1, $1)" in cur._query.query.lower()
        assert cur._query.params == [bytes([0, 3])]  # 3 as binary int2


def test_binary_cursor_execute(conn):
    cur = conn.cursor("foo", binary=True)
    cur.execute("select generate_series(1, 2)::int4")
    assert cur.fetchone() == (1,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x01"
    assert cur.fetchone() == (2,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x02"
    cur.close()


def test_execute_binary(conn):
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 2)::int4", binary=True)
    assert cur.fetchone() == (1,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x01"
    assert cur.fetchone() == (2,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x02"

    cur.execute("select generate_series(1, 1)::int4")
    assert cur.fetchone() == (1,)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"
    cur.close()


def test_binary_cursor_text_override(conn):
    cur = conn.cursor("foo", binary=True)
    cur.execute("select generate_series(1, 2)", binary=False)
    assert cur.fetchone() == (1,)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.fetchone() == (2,)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"2"

    cur.execute("select generate_series(1, 2)::int4")
    assert cur.fetchone() == (1,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x01"
    cur.close()


def test_close(conn, recwarn):
    if conn.info.transaction_status == conn.TransactionStatus.INTRANS:
        # connection dirty from previous failure
        conn.execute("close foo")
    recwarn.clear()
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 10) as bar")
    cur.close()
    assert cur.closed

    assert not conn.execute("select * from pg_cursors where name = 'foo'").fetchone()
    del cur
    assert not recwarn, [str(w.message) for w in recwarn.list]


def test_close_idempotent(conn):
    cur = conn.cursor("foo")
    cur.execute("select 1")
    cur.fetchall()
    cur.close()
    cur.close()


def test_close_broken_conn(conn):
    cur = conn.cursor("foo")
    conn.close()
    cur.close()
    assert cur.closed


def test_cursor_close_fetchone(conn):
    cur = conn.cursor("foo")
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    cur.execute(query)
    for _ in range(5):
        cur.fetchone()

    cur.close()
    assert cur.closed

    with pytest.raises(e.InterfaceError):
        cur.fetchone()


def test_cursor_close_fetchmany(conn):
    cur = conn.cursor("foo")
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    cur.execute(query)
    assert len(cur.fetchmany(2)) == 2

    cur.close()
    assert cur.closed

    with pytest.raises(e.InterfaceError):
        cur.fetchmany(2)


def test_cursor_close_fetchall(conn):
    cur = conn.cursor("foo")
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    cur.execute(query)
    assert len(cur.fetchall()) == 10

    cur.close()
    assert cur.closed

    with pytest.raises(e.InterfaceError):
        cur.fetchall()


def test_close_noop(conn, recwarn):
    recwarn.clear()
    cur = conn.cursor("foo")
    cur.close()
    assert not recwarn, [str(w.message) for w in recwarn.list]


def test_close_on_error(conn):
    cur = conn.cursor("foo")
    cur.execute("select 1")
    with pytest.raises(e.ProgrammingError):
        conn.execute("wat")
    assert conn.info.transaction_status == conn.TransactionStatus.INERROR
    cur.close()


def test_pgresult(conn):
    cur = conn.cursor()
    cur.execute("select 1")
    assert cur.pgresult
    cur.close()
    assert not cur.pgresult


def test_context(conn, recwarn):
    recwarn.clear()
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, 10) as bar")

    assert cur.closed
    assert not conn.execute("select * from pg_cursors where name = 'foo'").fetchone()
    del cur
    assert not recwarn, [str(w.message) for w in recwarn.list]


def test_close_no_clobber(conn):
    with pytest.raises(e.DivisionByZero):
        with conn.cursor("foo") as cur:
            cur.execute("select 1 / %s", (0,))
            cur.fetchall()


def test_warn_close(conn, recwarn):
    recwarn.clear()
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 10) as bar")
    del cur
    assert ".close()" in str(recwarn.pop(ResourceWarning).message)


def test_execute_reuse(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as foo", (3,))
        assert cur.fetchone() == (1,)

        cur.execute("select %s::text as bar, %s::text as baz", ("hello", "world"))
        assert cur.fetchone() == ("hello", "world")
        assert cur.description[0].name == "bar"
        assert cur.description[0].type_code == cur.adapters.types["text"].oid
        assert cur.description[1].name == "baz"


@pytest.mark.parametrize(
    "stmt", ["", "wat", "create table ssc ()", "select 1; select 2"]
)
def test_execute_error(conn, stmt):
    cur = conn.cursor("foo")
    with pytest.raises(e.ProgrammingError):
        cur.execute(stmt)
    cur.close()


def test_executemany(conn):
    cur = conn.cursor("foo")
    with pytest.raises(e.NotSupportedError):
        cur.executemany("select %s", [(1,), (2,)])
    cur.close()


def test_fetchone(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (2,))
        assert cur.fetchone() == (1,)
        assert cur.fetchone() == (2,)
        assert cur.fetchone() is None


def test_fetchmany(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (5,))
        assert cur.fetchmany(3) == [(1,), (2,), (3,)]
        assert cur.fetchone() == (4,)
        assert cur.fetchmany(3) == [(5,)]
        assert cur.fetchmany(3) == []


def test_fetchall(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (3,))
        assert cur.fetchall() == [(1,), (2,), (3,)]
        assert cur.fetchall() == []

    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (3,))
        assert cur.fetchone() == (1,)
        assert cur.fetchall() == [(2,), (3,)]
        assert cur.fetchall() == []


def test_nextset(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (3,))
        assert not cur.nextset()


def test_no_result(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar where false", (3,))
        assert len(cur.description) == 1
        assert cur.fetchall() == []


@pytest.mark.crdb_skip("scroll cursor")
def test_row_factory(conn):
    n = 0

    def my_row_factory(cur):
        nonlocal n
        n += 1
        return lambda values: [n] + [-v for v in values]

    cur = conn.cursor("foo", row_factory=my_row_factory, scrollable=True)
    cur.execute("select generate_series(1, 3) as x")
    recs = cur.fetchall()
    cur.scroll(0, "absolute")
    while True:
        rec = cur.fetchone()
        if not rec:
            break
        recs.append(rec)
    assert recs == [[1, -1], [1, -2], [1, -3]] * 2

    cur.scroll(0, "absolute")
    cur.row_factory = rows.dict_row
    assert cur.fetchone() == {"x": 1}
    cur.close()


def test_rownumber(conn):
    cur = conn.cursor("foo")
    assert cur.rownumber is None

    cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rownumber == 0

    cur.fetchone()
    assert cur.rownumber == 1
    cur.fetchone()
    assert cur.rownumber == 2
    cur.fetchmany(10)
    assert cur.rownumber == 12
    cur.fetchall()
    assert cur.rownumber == 42
    cur.close()


def test_iter(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (3,))
        recs = list(cur)
    assert recs == [(1,), (2,), (3,)]

    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (3,))
        assert cur.fetchone() == (1,)
        recs = list(cur)
    assert recs == [(2,), (3,)]


def test_iter_rownumber(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as bar", (3,))
        for row in cur:
            assert cur.rownumber == row[0]


def test_itersize(conn, commands):
    with conn.cursor("foo") as cur:
        assert cur.itersize == 100
        cur.itersize = 2
        cur.execute("select generate_series(1, %s) as bar", (3,))
        commands.popall()  # flush begin and other noise

        list(cur)
        cmds = commands.popall()
        assert len(cmds) == 2
        for cmd in cmds:
            assert "fetch forward 2" in cmd.lower()


def test_cant_scroll_by_default(conn):
    cur = conn.cursor("tmp")
    assert cur.scrollable is None
    with pytest.raises(e.ProgrammingError):
        cur.scroll(0)
    cur.close()


@pytest.mark.crdb_skip("scroll cursor")
def test_scroll(conn):
    cur = conn.cursor("tmp", scrollable=True)
    cur.execute("select generate_series(0,9)")
    cur.scroll(2)
    assert cur.fetchone() == (2,)
    cur.scroll(2)
    assert cur.fetchone() == (5,)
    cur.scroll(2, mode="relative")
    assert cur.fetchone() == (8,)
    cur.scroll(9, mode="absolute")
    assert cur.fetchone() == (9,)

    with pytest.raises(ValueError):
        cur.scroll(9, mode="wat")
    cur.close()


@pytest.mark.crdb_skip("scroll cursor")
def test_scrollable(conn):
    curs = conn.cursor("foo", scrollable=True)
    assert curs.scrollable is True
    curs.execute("select generate_series(0, 5)")
    curs.scroll(5)
    for i in range(4, -1, -1):
        curs.scroll(-1)
        assert i == curs.fetchone()[0]
        curs.scroll(-1)
    curs.close()


def test_non_scrollable(conn):
    curs = conn.cursor("foo", scrollable=False)
    assert curs.scrollable is False
    curs.execute("select generate_series(0, 5)")
    curs.scroll(5)
    with pytest.raises(e.OperationalError):
        curs.scroll(-1)
    curs.close()


@pytest.mark.parametrize("kwargs", [{}, {"withhold": False}])
def test_no_hold(conn, kwargs):
    with conn.cursor("foo", **kwargs) as curs:
        assert curs.withhold is False
        curs.execute("select generate_series(0, 2)")
        assert curs.fetchone() == (0,)
        conn.commit()
        with pytest.raises(e.InvalidCursorName):
            curs.fetchone()


@pytest.mark.crdb_skip("cursor with hold")
def test_hold(conn):
    with conn.cursor("foo", withhold=True) as curs:
        assert curs.withhold is True
        curs.execute("select generate_series(0, 5)")
        assert curs.fetchone() == (0,)
        conn.commit()
        assert curs.fetchone() == (1,)


def test_steal_cursor(conn):
    cur1 = conn.cursor()
    cur1.execute("declare test cursor for select generate_series(1, 6)")

    cur2 = conn.cursor("test")
    # can call fetch without execute
    assert cur2.fetchone() == (1,)
    assert cur2.fetchmany(3) == [(2,), (3,), (4,)]
    assert cur2.fetchall() == [(5,), (6,)]
    cur2.close()


def test_stolen_cursor_close(conn):
    cur1 = conn.cursor()
    cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = conn.cursor("test")
    cur2.close()

    cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = conn.cursor("test")
    cur2.close()
