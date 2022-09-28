import datetime as dt
from types import ModuleType
from typing import Any, List

import pytest

import psycopg
from psycopg import pq, sql, postgres
from psycopg import errors as e
from psycopg.adapt import Transformer, PyFormat, Dumper, Loader
from psycopg._cmodule import _psycopg
from psycopg.postgres import types as builtins, TEXT_OID
from psycopg.types.array import ListDumper, ListBinaryDumper


@pytest.mark.parametrize(
    "data, format, result, type",
    [
        (1, PyFormat.TEXT, b"1", "int2"),
        ("hello", PyFormat.TEXT, b"hello", "text"),
        ("hello", PyFormat.BINARY, b"hello", "text"),
    ],
)
def test_dump(data, format, result, type):
    t = Transformer()
    dumper = t.get_dumper(data, format)
    assert dumper.dump(data) == result
    if type == "text" and format != PyFormat.BINARY:
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
    dumper = t.get_dumper(data, PyFormat.TEXT)
    assert dumper.quote(data) == result


def test_register_dumper_by_class(conn):
    dumper = make_dumper("x")
    assert conn.adapters.get_dumper(MyStr, PyFormat.TEXT) is not dumper
    conn.adapters.register_dumper(MyStr, dumper)
    assert conn.adapters.get_dumper(MyStr, PyFormat.TEXT) is dumper


def test_register_dumper_by_class_name(conn):
    dumper = make_dumper("x")
    assert conn.adapters.get_dumper(MyStr, PyFormat.TEXT) is not dumper
    conn.adapters.register_dumper(f"{MyStr.__module__}.{MyStr.__qualname__}", dumper)
    assert conn.adapters.get_dumper(MyStr, PyFormat.TEXT) is dumper


@pytest.mark.crdb("skip", reason="global adapters don't affect crdb")
def test_dump_global_ctx(conn_cls, dsn, global_adapters, pgconn):
    psycopg.adapters.register_dumper(MyStr, make_bin_dumper("gb"))
    psycopg.adapters.register_dumper(MyStr, make_dumper("gt"))
    with conn_cls.connect(dsn) as conn:
        cur = conn.execute("select %s", [MyStr("hello")])
        assert cur.fetchone() == ("hellogt",)
        cur = conn.execute("select %b", [MyStr("hello")])
        assert cur.fetchone() == ("hellogb",)
        cur = conn.execute("select %t", [MyStr("hello")])
        assert cur.fetchone() == ("hellogt",)


def test_dump_connection_ctx(conn):
    conn.adapters.register_dumper(MyStr, make_bin_dumper("b"))
    conn.adapters.register_dumper(MyStr, make_dumper("t"))

    cur = conn.cursor()
    cur.execute("select %s", [MyStr("hello")])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %t", [MyStr("hello")])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %b", [MyStr("hello")])
    assert cur.fetchone() == ("hellob",)


def test_dump_cursor_ctx(conn):
    conn.adapters.register_dumper(str, make_bin_dumper("b"))
    conn.adapters.register_dumper(str, make_dumper("t"))

    cur = conn.cursor()
    cur.adapters.register_dumper(str, make_bin_dumper("bc"))
    cur.adapters.register_dumper(str, make_dumper("tc"))

    cur.execute("select %s", [MyStr("hello")])
    assert cur.fetchone() == ("hellotc",)
    cur.execute("select %t", [MyStr("hello")])
    assert cur.fetchone() == ("hellotc",)
    cur.execute("select %b", [MyStr("hello")])
    assert cur.fetchone() == ("hellobc",)

    cur = conn.cursor()
    cur.execute("select %s", [MyStr("hello")])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %t", [MyStr("hello")])
    assert cur.fetchone() == ("hellot",)
    cur.execute("select %b", [MyStr("hello")])
    assert cur.fetchone() == ("hellob",)


def test_dump_subclass(conn):
    class MyString(str):
        pass

    cur = conn.cursor()
    cur.execute("select %s::text, %b::text", [MyString("hello"), MyString("world")])
    assert cur.fetchone() == ("hello", "world")


def test_subclass_dumper(conn):
    # This might be a C fast object: make sure that the Python code is called
    from psycopg.types.string import StrDumper

    class MyStrDumper(StrDumper):
        def dump(self, obj):
            return (obj * 2).encode()

    conn.adapters.register_dumper(str, MyStrDumper)
    assert conn.execute("select %t", ["hello"]).fetchone()[0] == "hellohello"


def test_dumper_protocol(conn):

    # This class doesn't inherit from adapt.Dumper but passes a mypy check
    from .adapters_example import MyStrDumper

    conn.adapters.register_dumper(str, MyStrDumper)
    cur = conn.execute("select %s", ["hello"])
    assert cur.fetchone()[0] == "hellohello"
    cur = conn.execute("select %s", [["hi", "ha"]])
    assert cur.fetchone()[0] == ["hihi", "haha"]
    assert sql.Literal("hello").as_string(conn) == "'qelloqello'"


def test_loader_protocol(conn):

    # This class doesn't inherit from adapt.Loader but passes a mypy check
    from .adapters_example import MyTextLoader

    conn.adapters.register_loader("text", MyTextLoader)
    cur = conn.execute("select 'hello'::text")
    assert cur.fetchone()[0] == "hellohello"
    cur = conn.execute("select '{hi,ha}'::text[]")
    assert cur.fetchone()[0] == ["hihi", "haha"]


def test_subclass_loader(conn):
    # This might be a C fast object: make sure that the Python code is called
    from psycopg.types.string import TextLoader

    class MyTextLoader(TextLoader):
        def load(self, data):
            return (bytes(data) * 2).decode()

    conn.adapters.register_loader("text", MyTextLoader)
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


def test_register_loader_by_oid(conn):
    assert TEXT_OID == 25
    loader = make_loader("x")
    assert conn.adapters.get_loader(TEXT_OID, pq.Format.TEXT) is not loader
    conn.adapters.register_loader(TEXT_OID, loader)
    assert conn.adapters.get_loader(TEXT_OID, pq.Format.TEXT) is loader


def test_register_loader_by_type_name(conn):
    loader = make_loader("x")
    assert conn.adapters.get_loader(TEXT_OID, pq.Format.TEXT) is not loader
    conn.adapters.register_loader("text", loader)
    assert conn.adapters.get_loader(TEXT_OID, pq.Format.TEXT) is loader


@pytest.mark.crdb("skip", reason="global adapters don't affect crdb")
def test_load_global_ctx(conn_cls, dsn, global_adapters):
    psycopg.adapters.register_loader("text", make_loader("gt"))
    psycopg.adapters.register_loader("text", make_bin_loader("gb"))
    with conn_cls.connect(dsn) as conn:
        cur = conn.cursor(binary=False).execute("select 'hello'::text")
        assert cur.fetchone() == ("hellogt",)
        cur = conn.cursor(binary=True).execute("select 'hello'::text")
        assert cur.fetchone() == ("hellogb",)


def test_load_connection_ctx(conn):
    conn.adapters.register_loader("text", make_loader("t"))
    conn.adapters.register_loader("text", make_bin_loader("b"))

    r = conn.cursor(binary=False).execute("select 'hello'::text").fetchone()
    assert r == ("hellot",)
    r = conn.cursor(binary=True).execute("select 'hello'::text").fetchone()
    assert r == ("hellob",)


def test_load_cursor_ctx(conn):
    conn.adapters.register_loader("text", make_loader("t"))
    conn.adapters.register_loader("text", make_bin_loader("b"))

    cur = conn.cursor()
    cur.adapters.register_loader("text", make_loader("tc"))
    cur.adapters.register_loader("text", make_bin_loader("bc"))

    assert cur.execute("select 'hello'::text").fetchone() == ("hellotc",)
    cur.format = pq.Format.BINARY
    assert cur.execute("select 'hello'::text").fetchone() == ("hellobc",)

    cur = conn.cursor()
    assert cur.execute("select 'hello'::text").fetchone() == ("hellot",)
    cur.format = pq.Format.BINARY
    assert cur.execute("select 'hello'::text").fetchone() == ("hellob",)


def test_cow_dumpers(conn):
    conn.adapters.register_dumper(str, make_dumper("t"))

    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur2.adapters.register_dumper(str, make_dumper("c2"))

    r = cur1.execute("select %s::text -- 1", ["hello"]).fetchone()
    assert r == ("hellot",)
    r = cur2.execute("select %s::text -- 1", ["hello"]).fetchone()
    assert r == ("helloc2",)

    conn.adapters.register_dumper(str, make_dumper("t1"))
    r = cur1.execute("select %s::text -- 2", ["hello"]).fetchone()
    assert r == ("hellot",)
    r = cur2.execute("select %s::text -- 2", ["hello"]).fetchone()
    assert r == ("helloc2",)


def test_cow_loaders(conn):
    conn.adapters.register_loader("text", make_loader("t"))

    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur2.adapters.register_loader("text", make_loader("c2"))

    assert cur1.execute("select 'hello'::text").fetchone() == ("hellot",)
    assert cur2.execute("select 'hello'::text").fetchone() == ("helloc2",)

    conn.adapters.register_loader("text", make_loader("t1"))
    assert cur1.execute("select 'hello2'::text").fetchone() == ("hello2t",)
    assert cur2.execute("select 'hello2'::text").fetchone() == ("hello2c2",)


@pytest.mark.parametrize(
    "sql, obj",
    [("'{hello}'::text[]", ["helloc"]), ("row('hello'::text)", ("helloc",))],
)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_cursor_ctx_nested(conn, sql, obj, fmt_out):
    cur = conn.cursor(binary=fmt_out == pq.Format.BINARY)
    if fmt_out == pq.Format.TEXT:
        cur.adapters.register_loader("text", make_loader("c"))
    else:
        cur.adapters.register_loader("text", make_bin_loader("c"))

    cur.execute(f"select {sql}")
    res = cur.fetchone()[0]
    assert res == obj


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_list_dumper(conn, fmt_out):
    t = Transformer(conn)
    fmt_in = PyFormat.from_pq(fmt_out)
    dint = t.get_dumper([0], fmt_in)
    assert isinstance(dint, (ListDumper, ListBinaryDumper))
    assert dint.oid == builtins["int2"].array_oid
    assert dint.sub_dumper and dint.sub_dumper.oid == builtins["int2"].oid

    dstr = t.get_dumper([""], fmt_in)
    assert dstr is not dint

    assert t.get_dumper([1], fmt_in) is dint
    assert t.get_dumper([None, [1]], fmt_in) is dint

    dempty = t.get_dumper([], fmt_in)
    assert t.get_dumper([None, [None]], fmt_in) is dempty
    assert dempty.oid == 0
    assert dempty.dump([]) == b"{}"

    L: List[List[Any]] = []
    L.append(L)
    with pytest.raises(psycopg.DataError):
        assert t.get_dumper(L, fmt_in)


@pytest.mark.crdb("skip", reason="test in crdb test suite")
def test_str_list_dumper_text(conn):
    t = Transformer(conn)
    dstr = t.get_dumper([""], PyFormat.TEXT)
    assert isinstance(dstr, ListDumper)
    assert dstr.oid == 0
    assert dstr.sub_dumper and dstr.sub_dumper.oid == 0


def test_str_list_dumper_binary(conn):
    t = Transformer(conn)
    dstr = t.get_dumper([""], PyFormat.BINARY)
    assert isinstance(dstr, ListBinaryDumper)
    assert dstr.oid == builtins["text"].array_oid
    assert dstr.sub_dumper and dstr.sub_dumper.oid == builtins["text"].oid


def test_last_dumper_registered_ctx(conn):
    cur = conn.cursor()

    bd = make_bin_dumper("b")
    cur.adapters.register_dumper(str, bd)
    td = make_dumper("t")
    cur.adapters.register_dumper(str, td)

    assert cur.execute("select %s", ["hello"]).fetchone()[0] == "hellot"
    assert cur.execute("select %t", ["hello"]).fetchone()[0] == "hellot"
    assert cur.execute("select %b", ["hello"]).fetchone()[0] == "hellob"

    cur.adapters.register_dumper(str, bd)
    assert cur.execute("select %s", ["hello"]).fetchone()[0] == "hellob"


@pytest.mark.parametrize("fmt_in", [PyFormat.TEXT, PyFormat.BINARY])
def test_none_type_argument(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table none_args (id serial primary key, num integer)")
    cur.execute("insert into none_args (num) values (%s) returning id", (None,))
    assert cur.fetchone()[0]


@pytest.mark.crdb("skip", reason="test in crdb test suite")
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_return_untyped(conn, fmt_in):
    # Analyze and check for changes using strings in untyped/typed contexts
    cur = conn.cursor()
    # Currently string are passed as unknown oid to libpq. This is because
    # unknown is more easily cast by postgres to different types (see jsonb
    # later).
    cur.execute(f"select %{fmt_in.value}, %{fmt_in.value}", ["hello", 10])
    assert cur.fetchone() == ("hello", 10)

    cur.execute("create table testjson(data jsonb)")
    if fmt_in != PyFormat.BINARY:
        cur.execute(f"insert into testjson (data) values (%{fmt_in.value})", ["{}"])
        assert cur.execute("select data from testjson").fetchone() == ({},)
    else:
        # Binary types cannot be passed as unknown oids.
        with pytest.raises(e.DatatypeMismatch):
            cur.execute(f"insert into testjson (data) values (%{fmt_in.value})", ["{}"])


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_no_cast_needed(conn, fmt_in):
    # Verify that there is no need of cast in certain common scenario
    cur = conn.execute(f"select '2021-01-01'::date + %{fmt_in.value}", [3])
    assert cur.fetchone()[0] == dt.date(2021, 1, 4)

    cur = conn.execute(f"select '[10, 20, 30]'::jsonb -> %{fmt_in.value}", [1])
    assert cur.fetchone()[0] == 20


@pytest.mark.slow
@pytest.mark.skipif(_psycopg is None, reason="C module test")
def test_optimised_adapters():

    # All the optimised adapters available
    c_adapters = {}
    for n in dir(_psycopg):
        if n.startswith("_") or n in ("CDumper", "CLoader"):
            continue
        obj = getattr(_psycopg, n)
        if not isinstance(obj, type):
            continue
        if not issubclass(
            obj,
            (_psycopg.CDumper, _psycopg.CLoader),  # type: ignore[attr-defined]
        ):
            continue
        c_adapters[n] = obj

    # All the registered adapters
    reg_adapters = set()
    adapters = list(postgres.adapters._dumpers.values()) + postgres.adapters._loaders
    assert len(adapters) == 5
    for m in adapters:
        reg_adapters |= set(m.values())

    # Check that the registered adapters are the optimised one
    i = 0
    for cls in reg_adapters:
        if cls.__name__ in c_adapters:
            assert cls is c_adapters[cls.__name__]
            i += 1

    assert i >= 10

    # Check that every optimised adapter is the optimised version of a Py one
    for n in dir(psycopg.types):
        mod = getattr(psycopg.types, n)
        if not isinstance(mod, ModuleType):
            continue
        for n1 in dir(mod):
            obj = getattr(mod, n1)
            if not isinstance(obj, type):
                continue
            if not issubclass(obj, (Dumper, Loader)):
                continue
            c_adapters.pop(obj.__name__, None)

    assert not c_adapters


def test_dumper_init_error(conn):
    class BadDumper(Dumper):
        def __init__(self, cls, context):
            super().__init__(cls, context)
            1 / 0

        def dump(self, obj):
            return obj.encode()

    cur = conn.cursor()
    cur.adapters.register_dumper(str, BadDumper)
    with pytest.raises(ZeroDivisionError):
        cur.execute("select %s::text", ["hi"])


def test_loader_init_error(conn):
    class BadLoader(Loader):
        def __init__(self, oid, context):
            super().__init__(oid, context)
            1 / 0

        def load(self, data):
            return data.decode()

    cur = conn.cursor()
    cur.adapters.register_loader("text", BadLoader)
    with pytest.raises(ZeroDivisionError):
        cur.execute("select 'hi'::text")
        assert cur.fetchone() == ("hi",)


@pytest.mark.slow
@pytest.mark.parametrize("fmt", PyFormat)
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_random(conn, faker, fmt, fmt_out):
    faker.format = fmt
    faker.choose_schema(ncols=20)
    faker.make_records(50)

    with conn.cursor(binary=fmt_out) as cur:
        cur.execute(faker.drop_stmt)
        cur.execute(faker.create_stmt)
        with faker.find_insert_problem(conn):
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
