import pytest

pytestmark = pytest.mark.asyncio


async def test_description(aconn):
    cur = await aconn.cursor("foo")
    assert cur.name == "foo"
    await cur.execute("select generate_series(1, 10) as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0


async def test_close(aconn, recwarn):
    cur = await aconn.cursor("foo")
    await cur.execute("select generate_series(1, 10) as bar")
    await cur.close()
    assert cur.closed

    assert not await (
        await aconn.execute("select * from pg_cursors where name = 'foo'")
    ).fetchone()
    del cur
    assert not recwarn


async def test_context(aconn, recwarn):
    async with await aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, 10) as bar")

    assert cur.closed
    assert not await (
        await aconn.execute("select * from pg_cursors where name = 'foo'")
    ).fetchone()
    del cur
    assert not recwarn


async def test_warn_close(aconn, recwarn):
    cur = await aconn.cursor("foo")
    await cur.execute("select generate_series(1, 10) as bar")
    del cur
    assert ".close()" in str(recwarn.pop(ResourceWarning).message)
