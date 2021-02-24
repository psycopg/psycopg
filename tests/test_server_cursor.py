import pytest

from psycopg3 import errors as e
from psycopg3.pq import Format
from psycopg3.rows import dict_row


def test_funny_name(conn):
    cur = conn.cursor("1-2-3")
    cur.execute("select generate_series(1, 3) as bar")
    assert cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.name == "1-2-3"


def test_repr(conn):
    cur = conn.cursor("my-name")
    assert "ServerCursor" in repr(cur)
    assert "my-name" in repr(cur)


def test_connection(conn):
    cur = conn.cursor("foo")
    assert cur.connection is conn


def test_description(conn):
    cur = conn.cursor("foo")
    assert cur.name == "foo"
    cur.execute("select generate_series(1, 10) as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0


def test_format(conn):
    cur = conn.cursor("foo")
    assert cur.format == Format.TEXT

    cur = conn.cursor("foo", binary=True)
    assert cur.format == Format.BINARY


def test_query_params(conn):
    with conn.cursor("foo") as cur:
        assert cur.query is None
        assert cur.params is None
        cur.execute("select generate_series(1, %s) as bar", (3,))
        assert b"declare" in cur.query.lower()
        assert b"(1, $1)" in cur.query.lower()
        assert cur.params == [bytes([0, 3])]  # 3 as binary int2


def test_close(conn, recwarn):
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 10) as bar")
    cur.close()
    assert cur.closed

    assert not conn.execute(
        "select * from pg_cursors where name = 'foo'"
    ).fetchone()
    del cur
    assert not recwarn


def test_close_noop(conn, recwarn):
    cur = conn.cursor("foo")
    cur.close()
    assert not recwarn


def test_context(conn, recwarn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, 10) as bar")

    assert cur.closed
    assert not conn.execute(
        "select * from pg_cursors where name = 'foo'"
    ).fetchone()
    del cur
    assert not recwarn


def test_close_no_clobber(conn):
    with pytest.raises(e.DivisionByZero):
        with conn.cursor("foo") as cur:
            cur.execute("select 1 / %s", (0,))


def test_warn_close(conn, recwarn):
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 10) as bar")
    del cur
    assert ".close()" in str(recwarn.pop(ResourceWarning).message)


def test_execute_reuse(conn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, %s) as foo", (3,))
        assert cur.fetchone() == (1,)

        cur.execute(
            "select %s::text as bar, %s::text as baz", ("hello", "world")
        )
        assert cur.fetchone() == ("hello", "world")
        assert cur.description[0].name == "bar"
        assert cur.description[0].type_code == cur.adapters.types["text"].oid
        assert cur.description[1].name == "baz"


def test_executemany(conn):
    cur = conn.cursor("foo")
    with pytest.raises(e.NotSupportedError):
        cur.executemany("select %s", [(1,), (2,)])


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


def test_row_factory(conn):
    n = 0

    def my_row_factory(cur):
        nonlocal n
        n += 1
        return lambda values: [n] + [-v for v in values]

    cur = conn.cursor("foo", row_factory=my_row_factory)
    cur.execute("select generate_series(1, 3) as x", scrollable=True)
    rows = cur.fetchall()
    cur.scroll(0, "absolute")
    while 1:
        row = cur.fetchone()
        if not row:
            break
        rows.append(row)
    assert rows == [[1, -1], [1, -2], [1, -3]] * 2

    cur.scroll(0, "absolute")
    cur.row_factory = dict_row
    assert cur.fetchone() == {"x": 1}


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
            assert ("fetch forward 2") in cmd.lower()


def test_scroll(conn):
    cur = conn.cursor("tmp")
    with pytest.raises(e.ProgrammingError):
        cur.scroll(0)

    cur.execute("select generate_series(0,9)", scrollable=True)
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


def test_scrollable(conn):
    curs = conn.cursor("foo")
    curs.execute("select generate_series(0, 5)", scrollable=True)
    curs.scroll(5)
    for i in range(4, -1, -1):
        curs.scroll(-1)
        assert i == curs.fetchone()[0]
        curs.scroll(-1)


def test_non_scrollable(conn):
    curs = conn.cursor("foo")
    curs.execute("select generate_series(0, 5)", scrollable=False)
    curs.scroll(5)
    with pytest.raises(e.OperationalError):
        curs.scroll(-1)


@pytest.mark.parametrize("kwargs", [{}, {"hold": False}])
def test_no_hold(conn, kwargs):
    with pytest.raises(e.InvalidCursorName):
        with conn.cursor("foo") as curs:
            curs.execute("select generate_series(0, 2)", **kwargs)
            assert curs.fetchone() == (0,)
            conn.commit()
            curs.fetchone()


def test_hold(conn):
    with conn.cursor("foo") as curs:
        curs.execute("select generate_series(0, 5)", hold=True)
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


def test_stolen_cursor_close(conn):
    cur1 = conn.cursor()
    cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = conn.cursor("test")
    cur2.close()

    cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = conn.cursor("test")
    cur2.close()
