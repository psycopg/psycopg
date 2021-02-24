import pytest

from psycopg3 import errors as e
from psycopg3.rows import dict_row
from psycopg3.pq import Format

pytestmark = pytest.mark.asyncio


async def test_funny_name(aconn):
    cur = aconn.cursor("1-2-3")
    await cur.execute("select generate_series(1, 3) as bar")
    assert await cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.name == "1-2-3"


async def test_repr(aconn):
    cur = aconn.cursor("my-name")
    assert "AsyncServerCursor" in repr(cur)
    assert "my-name" in repr(cur)


async def test_connection(aconn):
    cur = aconn.cursor("foo")
    assert cur.connection is aconn


async def test_description(aconn):
    cur = aconn.cursor("foo")
    assert cur.name == "foo"
    await cur.execute("select generate_series(1, 10) as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0


async def test_format(aconn):
    cur = aconn.cursor("foo")
    assert cur.format == Format.TEXT

    cur = aconn.cursor("foo", binary=True)
    assert cur.format == Format.BINARY


async def test_query_params(aconn):
    async with aconn.cursor("foo") as cur:
        assert cur.query is None
        assert cur.params is None
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        assert b"declare" in cur.query.lower()
        assert b"(1, $1)" in cur.query.lower()
        assert cur.params == [bytes([0, 3])]  # 3 as binary int2


async def test_close(aconn, recwarn):
    cur = aconn.cursor("foo")
    await cur.execute("select generate_series(1, 10) as bar")
    await cur.close()
    assert cur.closed

    assert not await (
        await aconn.execute("select * from pg_cursors where name = 'foo'")
    ).fetchone()
    del cur
    assert not recwarn


async def test_close_noop(aconn, recwarn):
    cur = aconn.cursor("foo")
    await cur.close()
    assert not recwarn


async def test_context(aconn, recwarn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, 10) as bar")

    assert cur.closed
    assert not await (
        await aconn.execute("select * from pg_cursors where name = 'foo'")
    ).fetchone()
    del cur
    assert not recwarn


async def test_close_no_clobber(aconn):
    with pytest.raises(e.DivisionByZero):
        async with aconn.cursor("foo") as cur:
            await cur.execute("select 1 / %s", (0,))


async def test_warn_close(aconn, recwarn):
    cur = aconn.cursor("foo")
    await cur.execute("select generate_series(1, 10) as bar")
    del cur
    assert ".close()" in str(recwarn.pop(ResourceWarning).message)


async def test_execute_reuse(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as foo", (3,))
        assert await cur.fetchone() == (1,)

        await cur.execute(
            "select %s::text as bar, %s::text as baz", ("hello", "world")
        )
        assert await cur.fetchone() == ("hello", "world")
        assert cur.description[0].name == "bar"
        assert cur.description[0].type_code == cur.adapters.types["text"].oid
        assert cur.description[1].name == "baz"


async def test_executemany(aconn):
    cur = aconn.cursor("foo")
    with pytest.raises(e.NotSupportedError):
        await cur.executemany("select %s", [(1,), (2,)])


async def test_fetchone(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (2,))
        assert await cur.fetchone() == (1,)
        assert await cur.fetchone() == (2,)
        assert await cur.fetchone() is None


async def test_fetchmany(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (5,))
        assert await cur.fetchmany(3) == [(1,), (2,), (3,)]
        assert await cur.fetchone() == (4,)
        assert await cur.fetchmany(3) == [(5,)]
        assert await cur.fetchmany(3) == []


async def test_fetchall(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        assert await cur.fetchall() == [(1,), (2,), (3,)]
        assert await cur.fetchall() == []

    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        assert await cur.fetchone() == (1,)
        assert await cur.fetchall() == [(2,), (3,)]
        assert await cur.fetchall() == []


async def test_nextset(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        assert not cur.nextset()


async def test_row_factory(aconn):
    n = 0

    def my_row_factory(cur):
        nonlocal n
        n += 1
        return lambda values: [n] + [-v for v in values]

    cur = aconn.cursor("foo", row_factory=my_row_factory)
    await cur.execute("select generate_series(1, 3) as x", scrollable=True)
    rows = await cur.fetchall()
    await cur.scroll(0, "absolute")
    while 1:
        row = await cur.fetchone()
        if not row:
            break
        rows.append(row)
    assert rows == [[1, -1], [1, -2], [1, -3]] * 2

    await cur.scroll(0, "absolute")
    cur.row_factory = dict_row
    assert await cur.fetchone() == {"x": 1}


async def test_rownumber(aconn):
    cur = aconn.cursor("foo")
    assert cur.rownumber is None

    await cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rownumber == 0

    await cur.fetchone()
    assert cur.rownumber == 1
    await cur.fetchone()
    assert cur.rownumber == 2
    await cur.fetchmany(10)
    assert cur.rownumber == 12
    await cur.fetchall()
    assert cur.rownumber == 42


async def test_iter(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        recs = []
        async for rec in cur:
            recs.append(rec)
    assert recs == [(1,), (2,), (3,)]

    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        assert await cur.fetchone() == (1,)
        recs = []
        async for rec in cur:
            recs.append(rec)
    assert recs == [(2,), (3,)]


async def test_iter_rownumber(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        async for row in cur:
            assert cur.rownumber == row[0]


async def test_itersize(aconn, acommands):
    async with aconn.cursor("foo") as cur:
        assert cur.itersize == 100
        cur.itersize = 2
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        acommands.popall()  # flush begin and other noise

        async for rec in cur:
            pass
        cmds = acommands.popall()
        assert len(cmds) == 2
        for cmd in cmds:
            assert ("fetch forward 2") in cmd.lower()


async def test_scroll(aconn):
    cur = aconn.cursor("tmp")
    with pytest.raises(e.ProgrammingError):
        await cur.scroll(0)

    await cur.execute("select generate_series(0,9)", scrollable=True)
    await cur.scroll(2)
    assert await cur.fetchone() == (2,)
    await cur.scroll(2)
    assert await cur.fetchone() == (5,)
    await cur.scroll(2, mode="relative")
    assert await cur.fetchone() == (8,)
    await cur.scroll(9, mode="absolute")
    assert await cur.fetchone() == (9,)

    with pytest.raises(ValueError):
        await cur.scroll(9, mode="wat")


async def test_scrollable(aconn):
    curs = aconn.cursor("foo")
    await curs.execute("select generate_series(0, 5)", scrollable=True)
    await curs.scroll(5)
    for i in range(4, -1, -1):
        await curs.scroll(-1)
        assert i == (await curs.fetchone())[0]
        await curs.scroll(-1)


async def test_non_scrollable(aconn):
    curs = aconn.cursor("foo")
    await curs.execute("select generate_series(0, 5)", scrollable=False)
    await curs.scroll(5)
    with pytest.raises(e.OperationalError):
        await curs.scroll(-1)


@pytest.mark.parametrize("kwargs", [{}, {"hold": False}])
async def test_no_hold(aconn, kwargs):
    with pytest.raises(e.InvalidCursorName):
        async with aconn.cursor("foo") as curs:
            await curs.execute("select generate_series(0, 2)", **kwargs)
            assert await curs.fetchone() == (0,)
            await aconn.commit()
            await curs.fetchone()


async def test_hold(aconn):
    async with aconn.cursor("foo") as curs:
        await curs.execute("select generate_series(0, 5)", hold=True)
        assert await curs.fetchone() == (0,)
        await aconn.commit()
        assert await curs.fetchone() == (1,)


async def test_steal_cursor(aconn):
    cur1 = aconn.cursor()
    await cur1.execute(
        "declare test cursor without hold for select generate_series(1, 6)"
    )

    cur2 = aconn.cursor("test")
    # can call fetch without execute
    assert await cur2.fetchone() == (1,)
    assert await cur2.fetchmany(3) == [(2,), (3,), (4,)]
    assert await cur2.fetchall() == [(5,), (6,)]


async def test_stolen_cursor_close(aconn):
    cur1 = aconn.cursor()
    await cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = aconn.cursor("test")
    await cur2.close()

    await cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = aconn.cursor("test")
    await cur2.close()
