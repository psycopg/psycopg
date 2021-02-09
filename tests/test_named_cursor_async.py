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
