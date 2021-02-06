import pytest

from psycopg3 import pq
from psycopg3.sql import Identifier
from psycopg3.oids import postgres_types as builtins
from psycopg3.adapt import Format, global_adapters
from psycopg3.types import CompositeInfo


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
    info.register(context=conn)

    res = conn.execute("select %s::tmptype", [obj]).fetchone()[0]
    assert res == obj


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_all_chars(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 256):
        res = cur.execute("select row(chr(%s::int))", (i,)).fetchone()[0]
        assert res == (chr(i),)

    cur.execute(
        "select row(%s)" % ",".join(f"chr({i}::int)" for i in range(1, 256))
    )
    res = cur.fetchone()[0]
    assert res == tuple(map(chr, range(1, 256)))

    s = "".join(map(chr, range(1, 256)))
    res = cur.execute("select row(%s::text)", [s]).fetchone()[0]
    assert res == (s,)


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
            "10::int, null::text, 20::float,"
            " null::text, 'foo'::text, 'bar'::bytea ",
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
        Identifier("testcomp"),
        [("foo", "text"), ("bar", "int8"), ("baz", "float8")],
    ),
    (
        Identifier("testschema", "testcomp"),
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
    info = await CompositeInfo.fetch_async(aconn, name)
    assert info.name == "testcomp"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.field_names) == 3
    assert len(info.field_types) == 3
    for i, (name, t) in enumerate(fields):
        assert info.field_names[i] == name
        assert info.field_types[i] == builtins[t].oid


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_composite_all_chars(conn, fmt_in, testcomp):
    if fmt_in == Format.BINARY:
        pytest.xfail("binary composite dumper not implemented")
    cur = conn.cursor()
    for i in range(1, 256):
        (res,) = cur.execute(
            f"select row(chr(%s::int), 1, 1.0)::testcomp = %{fmt_in}::testcomp",
            (i, (chr(i), 1, 1.0)),
        ).fetchone()
        assert res is True


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_composite(conn, testcomp, fmt_out):
    info = CompositeInfo.fetch(conn, "testcomp")
    info.register(conn)

    cur = conn.cursor(binary=fmt_out)
    res = cur.execute("select row('hello', 10, 20)::testcomp").fetchone()[0]
    assert res.foo == "hello"
    assert res.bar == 10
    assert res.baz == 20.0
    assert isinstance(res.baz, float)

    res = cur.execute(
        "select array[row('hello', 10, 30)::testcomp]"
    ).fetchone()[0]
    assert len(res) == 1
    assert res[0].baz == 30.0
    assert isinstance(res[0].baz, float)


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_composite_factory(conn, testcomp, fmt_out):
    info = CompositeInfo.fetch(conn, "testcomp")

    class MyThing:
        def __init__(self, *args):
            self.foo, self.bar, self.baz = args

    info.register(conn, factory=MyThing)

    cur = conn.cursor(binary=fmt_out)
    res = cur.execute("select row('hello', 10, 20)::testcomp").fetchone()[0]
    assert isinstance(res, MyThing)
    assert res.baz == 20.0
    assert isinstance(res.baz, float)

    res = cur.execute(
        "select array[row('hello', 10, 30)::testcomp]"
    ).fetchone()[0]
    assert len(res) == 1
    assert res[0].baz == 30.0
    assert isinstance(res[0].baz, float)


def test_register_scope(conn, testcomp):
    info = CompositeInfo.fetch(conn, "testcomp")
    info.register()
    for fmt in (pq.Format.TEXT, pq.Format.BINARY):
        for oid in (info.oid, info.array_oid):
            assert global_adapters._loaders[fmt].pop(oid)

    cur = conn.cursor()
    info.register(cur)
    for fmt in (pq.Format.TEXT, pq.Format.BINARY):
        for oid in (info.oid, info.array_oid):
            assert oid not in global_adapters._loaders[fmt]
            assert oid not in conn.adapters._loaders[fmt]
            assert oid in cur.adapters._loaders[fmt]

    info.register(conn)
    for fmt in (pq.Format.TEXT, pq.Format.BINARY):
        for oid in (info.oid, info.array_oid):
            assert oid not in global_adapters._loaders[fmt]
            assert oid in conn.adapters._loaders[fmt]
