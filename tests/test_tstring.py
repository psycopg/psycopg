import pytest

import psycopg
from psycopg import sql
from psycopg.pq import Format

vstr = "hello"
vint = 16

async def test_connection_no_params(aconn):
    with pytest.raises(TypeError):
        await aconn.execute(t"select 1", [])


async def test_cursor_no_params(aconn):
    cur = aconn.cursor()
    with pytest.raises(TypeError):
        await cur.execute(t"select 1", [])


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
        cur = await aconn.execute(t)


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


async def test_reuse(aconn):
    cur = await aconn.execute(t"select {vint}, {vstr}, {vint}")
    assert await cur.fetchone() == (16, "hello", 16)
    assert cur._query.query == b"select $1, $2, $1"

    cur = await aconn.execute(t"select {vint}, {vint:s}, {vint:l}")
    assert await cur.fetchone() == (16,) * 3
    assert cur._query.query == b"select $1, $1, 16"
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,)
    assert cur._query.params == [b"\x00\x10"]
    assert cur._query.formats == [Format.BINARY]

    with pytest.raises(psycopg.ProgrammingError, match="different formats"):
        await aconn.execute(t"select {vint:b}, {vint:t}")


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
    cur = await aconn.execute(t"select {part}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b"select $1 as foo"
    assert cur._query.types == (psycopg.adapters.types["smallint"].oid,)
    assert cur._query.params == [b"\x00\x10"]
    assert cur._query.formats == [Format.BINARY]

    with pytest.raises(
        psycopg.ProgrammingError, match="nested templates don't support format"
    ):
        cur = await aconn.execute(t"select {part:s}")


async def test_sql(aconn):
    part = sql.SQL("foo")
    cur = await aconn.execute(t"select {vint} as {part}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b"select $1 as foo"

    with pytest.raises(psycopg.ProgrammingError, match="cannot have a format"):
        await aconn.execute(t"select {vint} as {part:i}")


async def test_sql_identifier(aconn):
    part = sql.Identifier("foo")
    cur = await aconn.execute(t"select {vint} as {part}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b'select $1 as "foo"'

    with pytest.raises(psycopg.ProgrammingError, match="can only have 'i' format"):
        await aconn.execute(t"select {vint} as {part:s}")


async def test_sql_composed(aconn):
    part = sql.SQL("{} as {}").format(vint, sql.Identifier("foo"))
    cur = await aconn.execute(t"select {part}")
    assert await cur.fetchone() == (16,)
    assert cur._query.query == b'select 16 as "foo"'

    with pytest.raises(psycopg.ProgrammingError, match="cannot have a format"):
        await aconn.execute(t"select {part:i}")


async def test_sql_placeholder(aconn):
    part = sql.Placeholder("foo")
    with pytest.raises(psycopg.ProgrammingError, match="Placeholder not supported"):
        cur = await aconn.execute(t"select {part}")
