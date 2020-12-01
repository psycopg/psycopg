import pytest

import psycopg3
from psycopg3.adapt import Transformer, Format, Dumper, Loader
from psycopg3.oids import builtins

TEXT_OID = builtins["text"].oid


@pytest.mark.parametrize(
    "data, format, result, type",
    [
        (1, Format.TEXT, b"1", "numeric"),
        ("hello", Format.TEXT, b"hello", "text"),
        ("hello", Format.BINARY, b"hello", "text"),
    ],
)
def test_dump(data, format, result, type):
    t = Transformer()
    dumper = t.get_dumper(data, format)
    assert dumper.dump(data) == result
    assert dumper.oid == 0 if type == "text" else builtins[type].oid


@pytest.mark.parametrize(
    "data, result",
    [
        (1, b"1"),
        ("hello", b"'hello'"),
        ("he'llo", b"'he''llo'"),
        (True, b"true"),
        (None, b"NULL"),
    ],
)
def test_quote(data, result):
    t = Transformer()
    dumper = t.get_dumper(data, Format.TEXT)
    assert dumper.quote(data) == result


def test_dump_connection_ctx(conn):
    make_dumper("t").register(str, conn)
    make_dumper("b").register(str, conn, format=Format.BINARY)

    cur = conn.cursor()
    cur.execute("select %s, %b", ["hello", "world"])
    assert cur.fetchone() == ("hellot", "worldb")


def test_dump_cursor_ctx(conn):
    make_dumper("t").register(str, conn)
    make_dumper("b").register(str, conn, format=Format.BINARY)

    cur = conn.cursor()
    make_dumper("tc").register(str, cur)
    make_dumper("bc").register(str, cur, format=Format.BINARY)

    cur.execute("select %s, %b", ["hello", "world"])
    assert cur.fetchone() == ("hellotc", "worldbc")

    cur = conn.cursor()
    cur.execute("select %s, %b", ["hello", "world"])
    assert cur.fetchone() == ("hellot", "worldb")


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_dump_subclass(conn, fmt_out):
    class MyString(str):
        pass

    cur = conn.cursor()
    cur.execute(
        "select %s::text, %b::text", [MyString("hello"), MyString("world")]
    )
    assert cur.fetchone() == ("hello", "world")


@pytest.mark.parametrize(
    "data, format, type, result",
    [
        (b"1", Format.TEXT, "int4", 1),
        (b"hello", Format.TEXT, "text", "hello"),
        (b"hello", Format.BINARY, "text", "hello"),
    ],
)
def test_cast(data, format, type, result):
    t = Transformer()
    rv = t.get_loader(builtins[type].oid, format).load(data)
    assert rv == result


def test_load_connection_ctx(conn):
    make_loader("t").register(TEXT_OID, conn)
    make_loader("b").register(TEXT_OID, conn, format=Format.BINARY)

    r = conn.cursor().execute("select 'hello'::text").fetchone()
    assert r == ("hellot",)
    r = conn.cursor(format=1).execute("select 'hello'::text").fetchone()
    assert r == ("hellob",)


def test_load_cursor_ctx(conn):
    make_loader("t").register(TEXT_OID, conn)
    make_loader("b").register(TEXT_OID, conn, format=Format.BINARY)

    cur = conn.cursor()
    make_loader("tc").register(TEXT_OID, cur)
    make_loader("bc").register(TEXT_OID, cur, format=Format.BINARY)

    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellotc",)
    cur.format = Format.BINARY
    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellobc",)

    cur = conn.cursor()
    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellot",)
    cur.format = Format.BINARY
    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellob",)


@pytest.mark.parametrize(
    "sql, obj",
    [("'{hello}'::text[]", ["helloc"]), ("row('hello'::text)", ("helloc",))],
)
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_cursor_ctx_nested(conn, sql, obj, fmt_out):
    cur = conn.cursor(format=fmt_out)
    make_loader("c").register(TEXT_OID, cur, format=fmt_out)
    cur.execute(f"select {sql}")
    res = cur.fetchone()[0]
    assert res == obj


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_none_type_argument(conn, fmt_in):
    cur = conn.cursor()
    cur.execute(
        """
        create table test_none_type_argument (
            id serial primary key, num integer
        )
        """
    )
    cur.execute(
        "insert into test_none_type_argument (num) values (%s) returning id",
        (None,),
    )
    assert cur.fetchone()[0]


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_return_untyped(conn, fmt_in):
    # Analyze and check for changes using strings in untyped/typed contexts
    cur = conn.cursor()
    # Currently string are passed as unknown oid to libpq. This is because
    # unknown is more easily cast by postgres to different types (see jsonb
    # later). However Postgres < 10 refuses to emit unknown types.
    if conn.pgconn.server_version > 100000:
        cur.execute("select %s, %s", ["hello", 10])
        assert cur.fetchone() == ("hello", 10)
    else:
        with pytest.raises(psycopg3.errors.IndeterminateDatatype):
            cur.execute("select %s, %s", ["hello", 10])
        conn.rollback()
        cur.execute("select %s::text, %s", ["hello", 10])
        assert cur.fetchone() == ("hello", 10)

    # It would be nice if above all postgres version behaved consistently.
    # However this below shouldn't break either.
    cur.execute("create table testjson(data jsonb)")
    cur.execute("insert into testjson (data) values (%s)", ["{}"])
    assert cur.execute("select data from testjson").fetchone() == ({},)


def make_dumper(suffix):
    """Create a test dumper appending a suffix to the bytes representation."""

    class TestDumper(Dumper):
        oid = TEXT_OID

        def dump(self, s):
            return (s + suffix).encode("ascii")

    return TestDumper


def make_loader(suffix):
    """Create a test loader appending a suffix to the data returned."""

    class TestLoader(Loader):
        def load(self, b):
            return b.decode("ascii") + suffix

    return TestLoader
