import pytest

import psycopg
from psycopg import rows, errors as e
from psycopg.pq import Format

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.crdb_skip("server-side cursor"),
]


async def test_init_row_factory(aconn):
    async with psycopg.AsyncServerCursor(aconn, "foo") as cur:
        assert cur.name == "foo"
        assert cur.connection is aconn
        assert cur.row_factory is aconn.row_factory

    aconn.row_factory = rows.dict_row

    async with psycopg.AsyncServerCursor(aconn, "bar") as cur:
        assert cur.name == "bar"
        assert cur.row_factory is rows.dict_row  # type: ignore

    async with psycopg.AsyncServerCursor(
        aconn, "baz", row_factory=rows.namedtuple_row
    ) as cur:
        assert cur.name == "baz"
        assert cur.row_factory is rows.namedtuple_row  # type: ignore


async def test_init_params(aconn):
    async with psycopg.AsyncServerCursor(aconn, "foo") as cur:
        assert cur.scrollable is None
        assert cur.withhold is False

    async with psycopg.AsyncServerCursor(
        aconn, "bar", withhold=True, scrollable=False
    ) as cur:
        assert cur.scrollable is False
        assert cur.withhold is True


@pytest.mark.crdb_skip("cursor invalid name")
async def test_funny_name(aconn):
    cur = aconn.cursor("1-2-3")
    await cur.execute("select generate_series(1, 3) as bar")
    assert await cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.name == "1-2-3"
    await cur.close()


async def test_repr(aconn):
    cur = aconn.cursor("my-name")
    assert "psycopg.AsyncServerCursor" in str(cur)
    assert "my-name" in repr(cur)
    await cur.close()


async def test_connection(aconn):
    cur = aconn.cursor("foo")
    assert cur.connection is aconn
    await cur.close()


async def test_description(aconn):
    cur = aconn.cursor("foo")
    assert cur.name == "foo"
    await cur.execute("select generate_series(1, 10)::int4 as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0
    await cur.close()


async def test_format(aconn):
    cur = aconn.cursor("foo")
    assert cur.format == Format.TEXT
    await cur.close()

    cur = aconn.cursor("foo", binary=True)
    assert cur.format == Format.BINARY
    await cur.close()


async def test_query_params(aconn):
    async with aconn.cursor("foo") as cur:
        assert cur._query is None
        await cur.execute("select generate_series(1, %s) as bar", (3,))
        assert cur._query is not None
        assert b"declare" in cur._query.query.lower()
        assert b"(1, $1)" in cur._query.query.lower()
        assert cur._query.params == [bytes([0, 3])]  # 3 as binary int2


async def test_binary_cursor_execute(aconn):
    cur = aconn.cursor("foo", binary=True)
    await cur.execute("select generate_series(1, 2)::int4")
    assert (await cur.fetchone()) == (1,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x01"
    assert (await cur.fetchone()) == (2,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x02"
    await cur.close()


async def test_execute_binary(aconn):
    cur = aconn.cursor("foo")
    await cur.execute("select generate_series(1, 2)::int4", binary=True)
    assert (await cur.fetchone()) == (1,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x01"
    assert (await cur.fetchone()) == (2,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x02"

    await cur.execute("select generate_series(1, 1)")
    assert (await cur.fetchone()) == (1,)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"
    await cur.close()


async def test_binary_cursor_text_override(aconn):
    cur = aconn.cursor("foo", binary=True)
    await cur.execute("select generate_series(1, 2)", binary=False)
    assert (await cur.fetchone()) == (1,)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert (await cur.fetchone()) == (2,)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"2"

    await cur.execute("select generate_series(1, 2)::int4")
    assert (await cur.fetchone()) == (1,)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x00\x00\x01"
    await cur.close()


async def test_close(aconn, recwarn):
    if aconn.info.transaction_status == aconn.TransactionStatus.INTRANS:
        # connection dirty from previous failure
        await aconn.execute("close foo")
    recwarn.clear()
    cur = aconn.cursor("foo")
    await cur.execute("select generate_series(1, 10) as bar")
    await cur.close()
    assert cur.closed

    assert not await (
        await aconn.execute("select * from pg_cursors where name = 'foo'")
    ).fetchone()
    del cur
    assert not recwarn, [str(w.message) for w in recwarn.list]


async def test_close_idempotent(aconn):
    cur = aconn.cursor("foo")
    await cur.execute("select 1")
    await cur.fetchall()
    await cur.close()
    await cur.close()


async def test_close_broken_conn(aconn):
    cur = aconn.cursor("foo")
    await aconn.close()
    await cur.close()
    assert cur.closed


async def test_cursor_close_fetchone(aconn):
    cur = aconn.cursor("foo")
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    await cur.execute(query)
    for _ in range(5):
        await cur.fetchone()

    await cur.close()
    assert cur.closed

    with pytest.raises(e.InterfaceError):
        await cur.fetchone()


async def test_cursor_close_fetchmany(aconn):
    cur = aconn.cursor("foo")
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    await cur.execute(query)
    assert len(await cur.fetchmany(2)) == 2

    await cur.close()
    assert cur.closed

    with pytest.raises(e.InterfaceError):
        await cur.fetchmany(2)


async def test_cursor_close_fetchall(aconn):
    cur = aconn.cursor("foo")
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    await cur.execute(query)
    assert len(await cur.fetchall()) == 10

    await cur.close()
    assert cur.closed

    with pytest.raises(e.InterfaceError):
        await cur.fetchall()


async def test_close_noop(aconn, recwarn):
    recwarn.clear()
    cur = aconn.cursor("foo")
    await cur.close()
    assert not recwarn, [str(w.message) for w in recwarn.list]


async def test_close_on_error(aconn):
    cur = aconn.cursor("foo")
    await cur.execute("select 1")
    with pytest.raises(e.ProgrammingError):
        await aconn.execute("wat")
    assert aconn.info.transaction_status == aconn.TransactionStatus.INERROR
    await cur.close()


async def test_pgresult(aconn):
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert cur.pgresult
    await cur.close()
    assert not cur.pgresult


async def test_context(aconn, recwarn):
    recwarn.clear()
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, 10) as bar")

    assert cur.closed
    assert not await (
        await aconn.execute("select * from pg_cursors where name = 'foo'")
    ).fetchone()
    del cur
    assert not recwarn, [str(w.message) for w in recwarn.list]


async def test_close_no_clobber(aconn):
    with pytest.raises(e.DivisionByZero):
        async with aconn.cursor("foo") as cur:
            await cur.execute("select 1 / %s", (0,))
            await cur.fetchall()


async def test_warn_close(aconn, recwarn):
    recwarn.clear()
    cur = aconn.cursor("foo")
    await cur.execute("select generate_series(1, 10) as bar")
    del cur
    assert ".close()" in str(recwarn.pop(ResourceWarning).message)


async def test_execute_reuse(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as foo", (3,))
        assert await cur.fetchone() == (1,)

        await cur.execute("select %s::text as bar, %s::text as baz", ("hello", "world"))
        assert await cur.fetchone() == ("hello", "world")
        assert cur.description[0].name == "bar"
        assert cur.description[0].type_code == cur.adapters.types["text"].oid
        assert cur.description[1].name == "baz"


@pytest.mark.parametrize(
    "stmt", ["", "wat", "create table ssc ()", "select 1; select 2"]
)
async def test_execute_error(aconn, stmt):
    cur = aconn.cursor("foo")
    with pytest.raises(e.ProgrammingError):
        await cur.execute(stmt)
    await cur.close()


async def test_executemany(aconn):
    cur = aconn.cursor("foo")
    with pytest.raises(e.NotSupportedError):
        await cur.executemany("select %s", [(1,), (2,)])
    await cur.close()


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


async def test_no_result(aconn):
    async with aconn.cursor("foo") as cur:
        await cur.execute("select generate_series(1, %s) as bar where false", (3,))
        assert len(cur.description) == 1
        assert (await cur.fetchall()) == []


@pytest.mark.crdb_skip("scroll cursor")
async def test_row_factory(aconn):
    n = 0

    def my_row_factory(cur):
        nonlocal n
        n += 1
        return lambda values: [n] + [-v for v in values]

    cur = aconn.cursor("foo", row_factory=my_row_factory, scrollable=True)
    await cur.execute("select generate_series(1, 3) as x")
    recs = await cur.fetchall()
    await cur.scroll(0, "absolute")
    while True:
        rec = await cur.fetchone()
        if not rec:
            break
        recs.append(rec)
    assert recs == [[1, -1], [1, -2], [1, -3]] * 2

    await cur.scroll(0, "absolute")
    cur.row_factory = rows.dict_row
    assert await cur.fetchone() == {"x": 1}
    await cur.close()


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
    await cur.close()


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
            assert "fetch forward 2" in cmd.lower()


async def test_cant_scroll_by_default(aconn):
    cur = aconn.cursor("tmp")
    assert cur.scrollable is None
    with pytest.raises(e.ProgrammingError):
        await cur.scroll(0)
    await cur.close()


@pytest.mark.crdb_skip("scroll cursor")
async def test_scroll(aconn):
    cur = aconn.cursor("tmp", scrollable=True)
    await cur.execute("select generate_series(0,9)")
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
    await cur.close()


@pytest.mark.crdb_skip("scroll cursor")
async def test_scrollable(aconn):
    curs = aconn.cursor("foo", scrollable=True)
    assert curs.scrollable is True
    await curs.execute("select generate_series(0, 5)")
    await curs.scroll(5)
    for i in range(4, -1, -1):
        await curs.scroll(-1)
        assert i == (await curs.fetchone())[0]
        await curs.scroll(-1)
    await curs.close()


async def test_non_scrollable(aconn):
    curs = aconn.cursor("foo", scrollable=False)
    assert curs.scrollable is False
    await curs.execute("select generate_series(0, 5)")
    await curs.scroll(5)
    with pytest.raises(e.OperationalError):
        await curs.scroll(-1)
    await curs.close()


@pytest.mark.parametrize("kwargs", [{}, {"withhold": False}])
async def test_no_hold(aconn, kwargs):
    async with aconn.cursor("foo", **kwargs) as curs:
        assert curs.withhold is False
        await curs.execute("select generate_series(0, 2)")
        assert await curs.fetchone() == (0,)
        await aconn.commit()
        with pytest.raises(e.InvalidCursorName):
            await curs.fetchone()


@pytest.mark.crdb_skip("cursor with hold")
async def test_hold(aconn):
    async with aconn.cursor("foo", withhold=True) as curs:
        assert curs.withhold is True
        await curs.execute("select generate_series(0, 5)")
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
    await cur2.close()


async def test_stolen_cursor_close(aconn):
    cur1 = aconn.cursor()
    await cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = aconn.cursor("test")
    await cur2.close()

    await cur1.execute("declare test cursor for select generate_series(1, 6)")
    cur2 = aconn.cursor("test")
    await cur2.close()
