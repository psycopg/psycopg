import pytest

from psycopg3.oids import builtins
from psycopg3.adapt import Format, Loader
from psycopg3.types import composite


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
    info = composite.fetch_info(conn, "tmptype")
    composite.register(info, context=conn)

    res = cur.execute("select %s::tmptype", [obj]).fetchone()[0]
    assert res == obj


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_all_chars(conn, fmt_out):
    cur = conn.cursor(format=fmt_out)
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
    cur = conn.cursor(format=1)
    res = cur.execute(f"select row({rec})").fetchone()[0]
    assert res == want
    for o1, o2 in zip(res, want):
        assert type(o1) is type(o2)


@pytest.fixture(scope="session")
def testcomp(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        drop type if exists testcomp cascade;
        create type testcomp as (foo text, bar int8, baz float8);
        """
    )


def test_fetch_info(conn, testcomp):
    info = composite.fetch_info(conn, "testcomp")
    assert info.name == "testcomp"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.fields) == 3
    for i, (name, t) in enumerate(
        [("foo", "text"), ("bar", "int8"), ("baz", "float8")]
    ):
        assert info.fields[i].name == name
        assert info.fields[i].type_oid == builtins[t].oid


@pytest.mark.asyncio
async def test_fetch_info_async(aconn, testcomp):
    info = await composite.fetch_info_async(aconn, "testcomp")
    assert info.name == "testcomp"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.fields) == 3
    for i, (name, t) in enumerate(
        [("foo", "text"), ("bar", "int8"), ("baz", "float8")]
    ):
        assert info.fields[i].name == name
        assert info.fields[i].type_oid == builtins[t].oid


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_composite(conn, testcomp, fmt_out):
    cur = conn.cursor(format=fmt_out)
    info = composite.fetch_info(conn, "testcomp")
    composite.register(info, conn)

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


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_composite_factory(conn, testcomp, fmt_out):
    cur = conn.cursor(format=fmt_out)
    info = composite.fetch_info(conn, "testcomp")

    class MyThing:
        def __init__(self, *args):
            self.foo, self.bar, self.baz = args

    composite.register(info, conn, factory=MyThing)

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


def test_register_scope(conn):
    info = composite.fetch_info(conn, "testcomp")

    composite.register(info)
    for fmt in (Format.TEXT, Format.BINARY):
        for oid in (info.oid, info.array_oid):
            assert Loader.globals.pop((oid, fmt))

    cur = conn.cursor()
    composite.register(info, cur)
    for fmt in (Format.TEXT, Format.BINARY):
        for oid in (info.oid, info.array_oid):
            key = oid, fmt
            assert key not in Loader.globals
            assert key not in conn.loaders
            assert key in cur.loaders

    composite.register(info, conn)
    for fmt in (Format.TEXT, Format.BINARY):
        for oid in (info.oid, info.array_oid):
            key = oid, fmt
            assert key not in Loader.globals
            assert key in conn.loaders
