import pytest

from psycopg import pq, postgres, sql
from psycopg.adapt import PyFormat
from psycopg.postgres import types as builtins
from psycopg.types.range import Range
from psycopg.types.composite import CompositeInfo, register_composite
from psycopg.types.composite import TupleDumper, TupleBinaryDumper

from ..utils import eur
from ..fix_crdb import is_crdb, crdb_skip_message


pytestmark = pytest.mark.crdb_skip("composite")

tests_str = [
    ("", ()),
    # Funnily enough there's no way to represent (None,) in Postgres
    ("null", ()),
    ("null,null", (None, None)),
    ("null, ''", (None, "")),
    (
        "42,'foo','ba,r','ba''z','qu\"x'",
        ("42", "foo", "ba,r", "ba'z", 'qu"x'),
    ),
    ("'foo''', '''foo', '\"bar', 'bar\"' ", ("foo'", "'foo", '"bar', 'bar"')),
]


@pytest.mark.parametrize("rec, want", tests_str)
def test_load_record(conn, want, rec):
    cur = conn.cursor()
    res = cur.execute(f"select row({rec})").fetchone()[0]
    assert res == want


@pytest.mark.parametrize("rec, obj", tests_str)
def test_dump_tuple(conn, rec, obj):
    cur = conn.cursor()
    fields = [f"f{i} text" for i in range(len(obj))]
    cur.execute(
        f"""
        drop type if exists tmptype;
        create type tmptype as ({', '.join(fields)});
        """
    )
    info = CompositeInfo.fetch(conn, "tmptype")
    register_composite(info, conn)

    res = conn.execute("select %s::tmptype", [obj]).fetchone()[0]
    assert res == obj


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_all_chars(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 256):
        res = cur.execute("select row(chr(%s::int))", (i,)).fetchone()[0]
        assert res == (chr(i),)

    cur.execute("select row(%s)" % ",".join(f"chr({i}::int)" for i in range(1, 256)))
    res = cur.fetchone()[0]
    assert res == tuple(map(chr, range(1, 256)))

    s = "".join(map(chr, range(1, 256)))
    res = cur.execute("select row(%s::text)", [s]).fetchone()[0]
    assert res == (s,)


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty_range(conn, fmt_in):
    conn.execute(
        """
        drop type if exists tmptype;
        create type tmptype as (num integer, range daterange, nums integer[])
        """
    )
    info = CompositeInfo.fetch(conn, "tmptype")
    register_composite(info, conn)

    cur = conn.execute(
        f"select pg_typeof(%{fmt_in.value})",
        [info.python_type(10, Range(empty=True), [])],
    )
    assert cur.fetchone()[0] == "tmptype"


@pytest.mark.parametrize(
    "rec, want",
    [
        ("", ()),
        ("null", (None,)),  # Unlike text format, this is a thing
        ("null,null", (None, None)),
        ("null, ''", (None, b"")),
        (
            "42,'foo','ba,r','ba''z','qu\"x'",
            (42, b"foo", b"ba,r", b"ba'z", b'qu"x'),
        ),
        (
            "'foo''', '''foo', '\"bar', 'bar\"' ",
            (b"foo'", b"'foo", b'"bar', b'bar"'),
        ),
        (
            "10::int, null::text, 20::float, null::text, 'foo'::text, 'bar'::bytea ",
            (10, None, 20.0, None, "foo", b"bar"),
        ),
    ],
)
def test_load_record_binary(conn, want, rec):
    cur = conn.cursor(binary=True)
    res = cur.execute(f"select row({rec})").fetchone()[0]
    assert res == want
    for o1, o2 in zip(res, want):
        assert type(o1) is type(o2)


@pytest.fixture(scope="session")
def testcomp(svcconn):
    if is_crdb(svcconn):
        pytest.skip(crdb_skip_message("composite"))
    cur = svcconn.cursor()
    cur.execute(
        """
        create schema if not exists testschema;

        drop type if exists testcomp cascade;
        drop type if exists testschema.testcomp cascade;

        create type testcomp as (foo text, bar int8, baz float8);
        create type testschema.testcomp as (foo text, bar int8, qux bool);
        """
    )
    return CompositeInfo.fetch(svcconn, "testcomp")


fetch_cases = [
    (
        "testcomp",
        [("foo", "text"), ("bar", "int8"), ("baz", "float8")],
    ),
    (
        "testschema.testcomp",
        [("foo", "text"), ("bar", "int8"), ("qux", "bool")],
    ),
    (
        sql.Identifier("testcomp"),
        [("foo", "text"), ("bar", "int8"), ("baz", "float8")],
    ),
    (
        sql.Identifier("testschema", "testcomp"),
        [("foo", "text"), ("bar", "int8"), ("qux", "bool")],
    ),
]


@pytest.mark.parametrize("name, fields", fetch_cases)
def test_fetch_info(conn, testcomp, name, fields):
    info = CompositeInfo.fetch(conn, name)
    assert info.name == "testcomp"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.field_names) == 3
    assert len(info.field_types) == 3
    for i, (name, t) in enumerate(fields):
        assert info.field_names[i] == name
        assert info.field_types[i] == builtins[t].oid


@pytest.mark.asyncio
@pytest.mark.parametrize("name, fields", fetch_cases)
async def test_fetch_info_async(aconn, testcomp, name, fields):
    info = await CompositeInfo.fetch(aconn, name)
    assert info.name == "testcomp"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.field_names) == 3
    assert len(info.field_types) == 3
    for i, (name, t) in enumerate(fields):
        assert info.field_names[i] == name
        assert info.field_types[i] == builtins[t].oid


@pytest.mark.parametrize("fmt_in", [PyFormat.AUTO, PyFormat.TEXT])
def test_dump_tuple_all_chars(conn, fmt_in, testcomp):
    cur = conn.cursor()
    for i in range(1, 256):
        (res,) = cur.execute(
            f"select row(chr(%s::int), 1, 1.0)::testcomp = %{fmt_in.value}::testcomp",
            (i, (chr(i), 1, 1.0)),
        ).fetchone()
        assert res is True


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_composite_all_chars(conn, fmt_in, testcomp):
    cur = conn.cursor()
    register_composite(testcomp, cur)
    factory = testcomp.python_type
    for i in range(1, 256):
        obj = factory(chr(i), 1, 1.0)
        (res,) = cur.execute(
            f"select row(chr(%s::int), 1, 1.0)::testcomp = %{fmt_in.value}", (i, obj)
        ).fetchone()
        assert res is True


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_composite_null(conn, fmt_in, testcomp):
    cur = conn.cursor()
    register_composite(testcomp, cur)
    factory = testcomp.python_type

    obj = factory("foo", 1, None)
    rec = cur.execute(
        f"""
        select row('foo', 1, NULL)::testcomp = %(obj){fmt_in.value},
            %(obj){fmt_in.value}::text
        """,
        {"obj": obj},
    ).fetchone()
    assert rec[0] is True, rec[1]


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_composite(conn, testcomp, fmt_out):
    info = CompositeInfo.fetch(conn, "testcomp")
    register_composite(info, conn)

    cur = conn.cursor(binary=fmt_out)
    res = cur.execute("select row('hello', 10, 20)::testcomp").fetchone()[0]
    assert res.foo == "hello"
    assert res.bar == 10
    assert res.baz == 20.0
    assert isinstance(res.baz, float)

    res = cur.execute("select array[row('hello', 10, 30)::testcomp]").fetchone()[0]
    assert len(res) == 1
    assert res[0].baz == 30.0
    assert isinstance(res[0].baz, float)


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_composite_factory(conn, testcomp, fmt_out):
    info = CompositeInfo.fetch(conn, "testcomp")

    class MyThing:
        def __init__(self, *args):
            self.foo, self.bar, self.baz = args

    register_composite(info, conn, factory=MyThing)
    assert info.python_type is MyThing

    cur = conn.cursor(binary=fmt_out)
    res = cur.execute("select row('hello', 10, 20)::testcomp").fetchone()[0]
    assert isinstance(res, MyThing)
    assert res.baz == 20.0
    assert isinstance(res.baz, float)

    res = cur.execute("select array[row('hello', 10, 30)::testcomp]").fetchone()[0]
    assert len(res) == 1
    assert res[0].baz == 30.0
    assert isinstance(res[0].baz, float)


def test_register_scope(conn, testcomp):
    info = CompositeInfo.fetch(conn, "testcomp")
    register_composite(info)
    for fmt in pq.Format:
        for oid in (info.oid, info.array_oid):
            assert postgres.adapters._loaders[fmt].pop(oid)

    for f in PyFormat:
        assert postgres.adapters._dumpers[f].pop(info.python_type)

    cur = conn.cursor()
    register_composite(info, cur)
    for fmt in pq.Format:
        for oid in (info.oid, info.array_oid):
            assert oid not in postgres.adapters._loaders[fmt]
            assert oid not in conn.adapters._loaders[fmt]
            assert oid in cur.adapters._loaders[fmt]

    register_composite(info, conn)
    for fmt in pq.Format:
        for oid in (info.oid, info.array_oid):
            assert oid not in postgres.adapters._loaders[fmt]
            assert oid in conn.adapters._loaders[fmt]


def test_type_dumper_registered(conn, testcomp):
    info = CompositeInfo.fetch(conn, "testcomp")
    register_composite(info, conn)
    assert issubclass(info.python_type, tuple)
    assert info.python_type.__name__ == "testcomp"
    d = conn.adapters.get_dumper(info.python_type, "s")
    assert issubclass(d, TupleDumper)
    assert d is not TupleDumper

    tc = info.python_type("foo", 42, 3.14)
    cur = conn.execute("select pg_typeof(%s)", [tc])
    assert cur.fetchone()[0] == "testcomp"


def test_type_dumper_registered_binary(conn, testcomp):
    info = CompositeInfo.fetch(conn, "testcomp")
    register_composite(info, conn)
    assert issubclass(info.python_type, tuple)
    assert info.python_type.__name__ == "testcomp"
    d = conn.adapters.get_dumper(info.python_type, "b")
    assert issubclass(d, TupleBinaryDumper)
    assert d is not TupleBinaryDumper

    tc = info.python_type("foo", 42, 3.14)
    cur = conn.execute("select pg_typeof(%b)", [tc])
    assert cur.fetchone()[0] == "testcomp"


def test_callable_dumper_not_registered(conn, testcomp):
    info = CompositeInfo.fetch(conn, "testcomp")

    def fac(*args):
        return args + (args[-1],)

    register_composite(info, conn, factory=fac)
    assert info.python_type is None

    # but the loader is registered
    cur = conn.execute("select '(foo,42,3.14)'::testcomp")
    assert cur.fetchone()[0] == ("foo", 42, 3.14, 3.14)


def test_no_info_error(conn):
    with pytest.raises(TypeError, match="composite"):
        register_composite(None, conn)  # type: ignore[arg-type]


def test_invalid_fields_names(conn):
    conn.execute("set client_encoding to utf8")
    conn.execute(
        f"""
        create type "a-b" as ("c-d" text, "{eur}" int);
        create type "-x-{eur}" as ("w-ww" "a-b", "0" int);
        """
    )
    ab = CompositeInfo.fetch(conn, '"a-b"')
    x = CompositeInfo.fetch(conn, f'"-x-{eur}"')
    register_composite(ab, conn)
    register_composite(x, conn)
    obj = x.python_type(ab.python_type("foo", 10), 20)
    conn.execute(f"""create table meh (wat "-x-{eur}")""")
    conn.execute("insert into meh values (%s)", [obj])
    got = conn.execute("select wat from meh").fetchone()[0]
    assert obj == got


@pytest.mark.parametrize("name", ["a-b", f"{eur}", "order", "1", "'"])
def test_literal_invalid_name(conn, name):
    conn.execute("set client_encoding to utf8")
    conn.execute(
        sql.SQL("create type {name} as (foo text)").format(name=sql.Identifier(name))
    )
    info = CompositeInfo.fetch(conn, sql.Identifier(name).as_string(conn))
    register_composite(info, conn)
    obj = info.python_type("hello")
    assert sql.Literal(obj).as_string(conn) == f"'(hello)'::\"{name}\""
    cur = conn.execute(sql.SQL("select {}").format(obj))
    got = cur.fetchone()[0]
    assert got == obj
    assert type(got) is type(obj)


@pytest.mark.parametrize(
    "name, attr",
    [
        ("a-b", "a_b"),
        (f"{eur}", "f_"),
        ("üåäö", "üåäö"),
        ("order", "order"),
        ("1", "f1"),
    ],
)
def test_literal_invalid_attr(conn, name, attr):
    conn.execute("set client_encoding to utf8")
    conn.execute(
        sql.SQL("create type test_attr as ({name} text)").format(
            name=sql.Identifier(name)
        )
    )
    info = CompositeInfo.fetch(conn, "test_attr")
    register_composite(info, conn)
    obj = info.python_type("hello")
    assert getattr(obj, attr) == "hello"
    cur = conn.execute(sql.SQL("select {}").format(obj))
    got = cur.fetchone()[0]
    assert got == obj
    assert type(got) is type(obj)
