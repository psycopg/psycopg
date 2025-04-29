import pytest


async def test_connection_no_params(aconn):
    with pytest.raises(TypeError):
        await aconn.execute(t"select 1", [])


async def test_cursor_no_params(aconn):
    cur = aconn.cursor()
    with pytest.raises(TypeError):
        await cur.execute(t"select 1", [])
