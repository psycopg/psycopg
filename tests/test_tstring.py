from random import random

import pytest

import psycopg
from psycopg import sql
from psycopg.pq import Format

from .acompat import alist

vstr = "hello"
vint = 16


async def test_connection_no_params(aconn):
    with pytest.raises(TypeError):
        await aconn.execute(t"select 1", [])    # noqa: F542


async def test_cursor_no_params(aconn):
    cur = aconn.cursor()
    with pytest.raises(TypeError):
        await cur.execute(t"select 1", [])  # noqa: F542


async def test_connection_execute(aconn):
    cur = await aconn.execute(t"select {vstr}")
    assert await cur.fetchone() == ("hello",)
    assert cur._query.query == b"select $1"
    assert cur._query.params == [b"hello"]
    assert cur._query.types == (0,)


@pytest.mark.parametrize(
    "t", [t"select {vstr!a}", t"select {vstr!r}", t"select {vstr!s}"]
)
async def test_no_conversion(aconn, t):
    with pytest.raises(TypeError):
        await aconn.execute(t)


@pytest.mark.parametrize(
    "t, fmt",
    [
        (t"select {vint}", Format.BINARY),
        (t"select {vint:s}", Format.BINARY),
        (t"select {vint:t}", Format.TEXT),
        (t"select {vint:b}", Format.BINARY),
    ],
)
async def test_format(aconn, t, fmt):
    cur = await aconn.execute(t)
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b"select $1"
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,)
    assert cur._query.params == [b"\x00\x10" if fmt == Format.BINARY else b"16"]
    assert cur._query.formats == [fmt]


async def test_format_bad(aconn):
    with pytest.raises(psycopg.ProgrammingError, match="format 'x' not supported"):
        await aconn.execute(t"select {vint:x}")


async def test_expression(aconn):
    cur = await aconn.execute(t"select {vint * 2}")
    assert await cur.fetchone() == (32,)
    assert cur._query.query == b"select $1"
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,)
    assert cur._query.params == [b"\x00\x20"]
    assert cur._query.formats == [Format.BINARY]


async def test_format_identifier(aconn):
    f1 = "foo-bar"
    f2 = "baz"
    cur = await aconn.execute(t"select {vint} as {f1:i}, {vint * 2:t} as {f2:i}")
    assert await cur.fetchone() == (16, 32)
    assert cur._query.query == b'select $1 as "foo-bar", $2 as "baz"'
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,) * 2
    assert cur._query.params == [b"\x00\x10", b"32"]
    assert cur._query.formats == [Format.BINARY, Format.TEXT]


async def test_format_literal(aconn):
    f1 = "foo-bar"
    f2 = "baz"
    cur = await aconn.execute(t"select {vint * 2:l} as {f1:i}, {vint:t} as {f2:i}")
    assert await cur.fetchone() == (32, 16)
    assert cur._query.query == b'select 32 as "foo-bar", $1 as "baz"'
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,)
    assert cur._query.params == [b"16"]
    assert cur._query.formats == [Format.TEXT]


async def test_nested(aconn):
    part = t"{vint} as foo"
    cur = await aconn.execute(t"select {part:q}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b"select $1 as foo"
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,)
    assert cur._query.params == [b"\x00\x10"]
    assert cur._query.formats == [Format.BINARY]

    with pytest.raises(psycopg.ProgrammingError, match="Template.*':q'"):
        cur = await aconn.execute(t"select {part}")


async def test_scope(aconn):
    t = t"select "  # noqa: F542
    for i, name in enumerate(("foo", "bar", "baz")):
        if i:
            t += t", "  # noqa: F542
        t += t"{i} as {name:i}"

    cur = await aconn.execute(t)
    assert await cur.fetchone() == (0, 1, 2)
    assert cur.description[0].name == "foo"
    assert cur.description[2].name == "baz"


async def test_no_reuse(aconn):
    t = t"select {vint}, {vint}"
    cur = await aconn.execute(t)
    assert await cur.fetchone() == (vint, vint)
    assert b"$2" in cur._query.query


async def test_volatile(aconn):
    t = t"select {random()}, {random()}"
    cur = await aconn.execute(t)
    rec = await cur.fetchone()
    assert rec[0] != rec[1]
    assert b"$2" in cur._query.query


async def test_sql(aconn):
    part = sql.SQL("foo")
    cur = await aconn.execute(t"select {vint} as {part:q}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b"select $1 as foo"

    with pytest.raises(psycopg.ProgrammingError, match=r"sql\.SQL.*':q'"):
        await aconn.execute(t"select {vint} as {part:i}")


async def test_sql_composed(aconn):
    part = sql.SQL("{} as {}").format(vint, sql.Identifier("foo"))
    cur = await aconn.execute(t"select {part:q}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b'select 16 as "foo"'

    with pytest.raises(psycopg.ProgrammingError, match=r"sql\.Composed.*':q'"):
        await aconn.execute(t"select {part}")


async def test_sql_identifier(aconn):
    part = sql.Identifier("foo")
    cur = await aconn.execute(t"select {vint} as {part:i}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b'select $1 as "foo"'

    with pytest.raises(psycopg.ProgrammingError, match=r"sql\.Identifier.*':i'"):
        await aconn.execute(t"select {vint} as {part}")


async def test_sql_literal(aconn):
    lit = sql.Literal(42)
    cur = await aconn.execute(t"select {lit:l} as foo")
    assert await cur.fetchone() == (42,)
    assert cur._query.query == b'select 42 as foo'

    with pytest.raises(psycopg.ProgrammingError, match=r"sql\.Literal.*':l'"):
        await aconn.execute(t"select {lit} as foo")


async def test_sql_placeholder(aconn):
    part = sql.Placeholder("foo")
    with pytest.raises(psycopg.ProgrammingError, match="Placeholder not supported"):
        await aconn.execute(t"select {part}")


@pytest.mark.xfail(reason="Template.join() needed")
async def test_template_join(aconn):
    ts = [t"{i} as {name:i}" for i, name in enumerate(("foo", "bar", "baz"))]
    fields = t','.join(ts)  # noqa: F542
    cur = await aconn.execute(t"select {fields}")
    assert await cur.fetchone() == (0, 1, 2)
    assert cur.description[0].name == "foo"
    assert cur.description[2].name == "baz"


async def test_sql_join(aconn):
    ts = [t"{i} as {name:i}" for i, name in enumerate(("foo", "bar", "baz"))]
    fields = sql.SQL(',').join(ts)
    cur = await aconn.execute(t"select {fields:q}")
    assert await cur.fetchone() == (0, 1, 2)
    assert cur.description[0].name == "foo"
    assert cur.description[2].name == "baz"


async def test_copy(aconn):
    cur = aconn.cursor()
    async with cur.copy(
        t"copy (select * from generate_series(1, {3})) to stdout"
    ) as copy:
        data = await alist(copy.rows())
    assert data == [("1",), ("2",), ("3",)]


async def test_client_cursor(aconn):
    cur = psycopg.AsyncClientCursor(aconn)
    await cur.execute(t"select {vint}, {vstr} as {vstr:i}")
    assert await cur.fetchone() == (vint, vstr)
    assert cur.description[1].name == vstr
    assert str(vint) in cur._query.query.decode()
    assert str(vint) == cur._query.params[0].decode()
    assert f"'{vstr}'" in cur._query.query.decode()
    assert f"'{vstr}'" in cur._query.params[1].decode()


async def test_mogrify(aconn):
    cur = psycopg.AsyncClientCursor(aconn)
    res = cur.mogrify(t"select {vint}, {vstr} as {vstr:i}")
    assert res == "select 16, 'hello' as \"hello\""


async def test_raw_cursor(aconn):
    cur = psycopg.AsyncRawCursor(aconn)
    with pytest.raises(psycopg.NotSupportedError):
        await cur.execute(t"select {vint}, {vstr} as {vstr:i}")


async def test_server_cursor(aconn):
    async with psycopg.AsyncServerCursor(aconn, "test") as cur:
        await cur.execute(t"select {vint}, {vstr} as {vstr:i}")
        assert await cur.fetchone() == (vint, vstr)
        assert cur.description[1].name == vstr
        assert b"$2" in cur._query.query
        assert b"$3" not in cur._query.query
