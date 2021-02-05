import datetime as dt

import pytest

import psycopg3
from psycopg3 import pq
from psycopg3.adapt import Transformer, Format, Dumper, Loader
from psycopg3.oids import postgres_types as builtins, TEXT_OID


@pytest.mark.parametrize(
    "data, format, result, type",
    [
        (1, Format.TEXT, b"1", "int2"),
        ("hello", Format.TEXT, b"hello", "text"),
        ("hello", Format.BINARY, b"hello", "text"),
    ],
)
def test_dump(data, format, result, type):
    t = Transformer()
    dumper = t.get_dumper(data, format)
    assert dumper.dump(data) == result
    if type == "text" and format != Format.BINARY:
        assert dumper.oid == 0
    else:
        assert dumper.oid == builtins[type].oid


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
    make_dumper("t").register(MyStr, conn)
    make_bin_dumper("b").register(MyStr, conn)

    cur = conn.cursor()
    cur.execute("select %s", [MyStr("hello")])
    assert cur.fetchone() == ("hellob",)
    cur.execute("select %t", [MyStr("hello")])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %b", [MyStr("hello")])
    assert cur.fetchone() == ("hellob",)


def test_dump_cursor_ctx(conn):
    make_dumper("t").register(str, conn)
    make_bin_dumper("b").register(str, conn)

    cur = conn.cursor()
    make_dumper("tc").register(str, cur)
    make_bin_dumper("bc").register(str, cur)

    cur.execute("select %s", [MyStr("hello")])
    assert cur.fetchone() == ("hellobc",)
    cur.execute("select %t", [MyStr("hello")])
    assert cur.fetchone() == ("hellotc",)
    cur.execute("select %b", [MyStr("hello")])
    assert cur.fetchone() == ("hellobc",)

    cur = conn.cursor()
    cur.execute("select %s", [MyStr("hello")])
    assert cur.fetchone() == ("hellob",)
    cur.execute("select %t", [MyStr("hello")])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %b", [MyStr("hello")])
    assert cur.fetchone() == ("hellob",)


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_dump_subclass(conn, fmt_out):
    class MyString(str):
        pass

    cur = conn.cursor()
    cur.execute(
        "select %s::text, %b::text", [MyString("hello"), MyString("world")]
    )
    assert cur.fetchone() == ("hello", "world")


def test_subclass_dumper(conn):
    # This might be a C fast object: make sure that the Python code is called
    from psycopg3.types import StringDumper

    class MyStringDumper(StringDumper):
        def dump(self, obj):
            return (obj * 2).encode("utf-8")

    MyStringDumper.register(str, conn)
    assert conn.execute("select %t", ["hello"]).fetchone()[0] == "hellohello"


def test_subclass_loader(conn):
    # This might be a C fast object: make sure that the Python code is called
    from psycopg3.types import TextLoader

    class MyTextLoader(TextLoader):
        def load(self, data):
            return (bytes(data) * 2).decode("utf-8")

    MyTextLoader.register("text", conn)
    assert conn.execute("select 'hello'::text").fetchone()[0] == "hellohello"


@pytest.mark.parametrize(
    "data, format, type, result",
    [
        (b"1", pq.Format.TEXT, "int4", 1),
        (b"hello", pq.Format.TEXT, "text", "hello"),
        (b"hello", pq.Format.BINARY, "text", "hello"),
    ],
)
def test_cast(data, format, type, result):
    t = Transformer()
    rv = t.get_loader(builtins[type].oid, format).load(data)
    assert rv == result


def test_load_connection_ctx(conn):
    make_loader("t").register(TEXT_OID, conn)
    make_bin_loader("b").register(TEXT_OID, conn)

    r = conn.cursor(binary=False).execute("select 'hello'::text").fetchone()
    assert r == ("hellot",)
    r = conn.cursor(binary=True).execute("select 'hello'::text").fetchone()
    assert r == ("hellob",)


def test_load_cursor_ctx(conn):
    make_loader("t").register(TEXT_OID, conn)
    make_bin_loader("b").register(TEXT_OID, conn)

    cur = conn.cursor()
    make_loader("tc").register(TEXT_OID, cur)
    make_bin_loader("bc").register(TEXT_OID, cur)

    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellotc",)
    cur.format = pq.Format.BINARY
    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellobc",)

    cur = conn.cursor()
    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellot",)
    cur.format = pq.Format.BINARY
    r = cur.execute("select 'hello'::text").fetchone()
    assert r == ("hellob",)


@pytest.mark.parametrize(
    "sql, obj",
    [("'{hello}'::text[]", ["helloc"]), ("row('hello'::text)", ("helloc",))],
)
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_cursor_ctx_nested(conn, sql, obj, fmt_out):
    cur = conn.cursor(binary=fmt_out == pq.Format.BINARY)
    if fmt_out == pq.Format.TEXT:
        make_loader("c").register(TEXT_OID, cur)
    else:
        make_bin_loader("c").register(TEXT_OID, cur)

    cur.execute(f"select {sql}")
    res = cur.fetchone()[0]
    assert res == obj


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_array_dumper(conn, fmt_out):
    t = Transformer(conn)
    fmt_in = Format.from_pq(fmt_out)
    dint = t.get_dumper([0], fmt_in)
    assert dint.oid == builtins["int2"].array_oid
    assert dint.sub_dumper.oid == builtins["int2"].oid

    dstr = t.get_dumper([""], fmt_in)
    if fmt_in == Format.BINARY:
        assert dstr.oid == builtins["text"].array_oid
        assert dstr.sub_dumper.oid == builtins["text"].oid
    else:
        assert dstr.oid == 0
        assert dstr.sub_dumper.oid == 0

    assert dstr is not dint

    assert t.get_dumper([1], fmt_in) is dint
    assert t.get_dumper([None, [1]], fmt_in) is dint

    dempty = t.get_dumper([], fmt_in)
    assert t.get_dumper([None, [None]], fmt_in) is dempty
    assert dempty.oid == 0
    assert dempty.dump([]) == b"{}"

    L = []
    L.append(L)
    with pytest.raises(psycopg3.DataError):
        assert t.get_dumper(L, fmt_in)


def test_string_connection_ctx(conn):
    make_dumper("t").register(str, conn)
    make_bin_dumper("b").register(str, conn)

    cur = conn.cursor()
    cur.execute("select %s", ["hello"])
    assert cur.fetchone() == ("hellot",)  # str prefers text
    cur.execute("select %t", ["hello"])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %b", ["hello"])
    assert cur.fetchone() == ("hellob",)


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_none_type_argument(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table none_args (id serial primary key, num integer)")
    cur.execute(
        "insert into none_args (num) values (%s) returning id", (None,)
    )
    assert cur.fetchone()[0]


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_return_untyped(conn, fmt_in):
    # Analyze and check for changes using strings in untyped/typed contexts
    cur = conn.cursor()
    # Currently string are passed as unknown oid to libpq. This is because
    # unknown is more easily cast by postgres to different types (see jsonb
    # later).
    cur.execute("select %s, %s", ["hello", 10])
    assert cur.fetchone() == ("hello", 10)

    cur.execute("create table testjson(data jsonb)")
    cur.execute("insert into testjson (data) values (%s)", ["{}"])
    assert cur.execute("select data from testjson").fetchone() == ({},)


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_no_cast_needed(conn, fmt_in):
    # Verify that there is no need of cast in certain common scenario
    cur = conn.execute("select '2021-01-01'::date + %s", [3])
    assert cur.fetchone()[0] == dt.date(2021, 1, 4)

    cur = conn.execute("select '[10, 20, 30]'::jsonb -> %s", [1])
    assert cur.fetchone()[0] == 20


def test_optimised_adapters():
    if psycopg3.pq.__impl__ == "python":
        pytest.skip("test C module only")

    from psycopg3_c import _psycopg3

    # All the optimised adapters available
    c_adapters = {}
    for n in dir(_psycopg3):
        if n.startswith("_") or n in ("CDumper", "CLoader"):
            continue
        obj = getattr(_psycopg3, n)
        if not isinstance(obj, type):
            continue
        if not issubclass(obj, (_psycopg3.CDumper, _psycopg3.CLoader)):
            continue
        c_adapters[n] = obj

    # All the registered adapters
    reg_adapters = set()
    adapters = (
        psycopg3.global_adapters._dumpers + psycopg3.global_adapters._loaders
    )
    assert len(adapters) == 4
    for m in adapters:
        reg_adapters |= set(m.values())

    # Check that the registered adapters are the optimised one
    n = 0
    for cls in reg_adapters:
        if cls.__name__ in c_adapters:
            assert cls is c_adapters[cls.__name__]
            n += 1

    assert n >= 10

    # Check that every optimised adapter is the optimised version of a Py one
    for n in dir(psycopg3.types):
        obj = getattr(psycopg3.types, n)
        if not isinstance(obj, type):
            continue
        if not issubclass(obj, (Dumper, Loader)):
            continue
        c_adapters.pop(obj.__name__, None)

    # TODO: This dumper is not registered yet as not implemented
    del c_adapters["IntNumericBinaryDumper"]

    assert not c_adapters


@pytest.mark.slow
@pytest.mark.parametrize("fmt", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_random(conn, faker, fmt):
    faker.format = fmt
    faker.choose_schema(ncols=20)
    faker.make_records(50)

    with conn.cursor(binary=Format.as_pq(fmt)) as cur:
        cur.execute(faker.drop_stmt)
        cur.execute(faker.create_stmt)
        cur.executemany(faker.insert_stmt, faker.records)
        cur.execute(faker.select_stmt)
        recs = cur.fetchall()

    for got, want in zip(recs, faker.records):
        faker.assert_record(got, want)


class MyStr(str):
    pass


def make_dumper(suffix):
    """Create a test dumper appending a suffix to the bytes representation."""

    class TestDumper(Dumper):
        oid = TEXT_OID
        format = pq.Format.TEXT

        def dump(self, s):
            return (s + suffix).encode("ascii")

    return TestDumper


def make_bin_dumper(suffix):
    cls = make_dumper(suffix)
    cls.format = pq.Format.BINARY
    return cls


def make_loader(suffix):
    """Create a test loader appending a suffix to the data returned."""

    class TestLoader(Loader):
        format = pq.Format.TEXT

        def load(self, b):
            return bytes(b).decode("ascii") + suffix

    return TestLoader


def make_bin_loader(suffix):
    cls = make_loader(suffix)
    cls.format = pq.Format.BINARY
    return cls
