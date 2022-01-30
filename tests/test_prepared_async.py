"""
Prepared statements tests on async connections
"""

import datetime as dt
from decimal import Decimal

import pytest

import psycopg

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("value", [None, 0, 3])
async def test_prepare_threshold_init(dsn, value):
    async with await psycopg.AsyncConnection.connect(
        dsn, prepare_threshold=value
    ) as conn:
        assert conn.prepare_threshold == value


async def test_dont_prepare(aconn):
    cur = aconn.cursor()
    for i in range(10):
        await cur.execute("select %s::int", [i], prepare=False)

    await cur.execute("select count(*) from pg_prepared_statements")
    assert await cur.fetchone() == (0,)


async def test_do_prepare(aconn):
    cur = aconn.cursor()
    await cur.execute("select %s::int", [10], prepare=True)
    await cur.execute("select count(*) from pg_prepared_statements")
    assert await cur.fetchone() == (1,)


async def test_auto_prepare(aconn):
    cur = aconn.cursor()
    res = []
    for i in range(10):
        await cur.execute("select count(*) from pg_prepared_statements")
        res.append((await cur.fetchone())[0])

    assert res == [0] * 5 + [1] * 5


async def test_dont_prepare_conn(aconn):
    for i in range(10):
        await aconn.execute("select %s::int", [i], prepare=False)

    cur = await aconn.execute("select count(*) from pg_prepared_statements")
    assert await cur.fetchone() == (0,)


async def test_do_prepare_conn(aconn):
    await aconn.execute("select %s::int", [10], prepare=True)
    cur = await aconn.execute("select count(*) from pg_prepared_statements")
    assert await cur.fetchone() == (1,)


async def test_auto_prepare_conn(aconn):
    res = []
    for i in range(10):
        cur = await aconn.execute("select count(*) from pg_prepared_statements")
        res.append((await cur.fetchone())[0])

    assert res == [0] * 5 + [1] * 5


async def test_prepare_disable(aconn):
    aconn.prepare_threshold = None
    res = []
    for i in range(10):
        cur = await aconn.execute("select count(*) from pg_prepared_statements")
        res.append((await cur.fetchone())[0])

    assert res == [0] * 10
    assert not aconn._prepared._names
    assert not aconn._prepared._counts


async def test_no_prepare_multi(aconn):
    res = []
    for i in range(10):
        cur = await aconn.execute(
            "select count(*) from pg_prepared_statements; select 1"
        )
        res.append((await cur.fetchone())[0])

    assert res == [0] * 10


async def test_no_prepare_error(aconn):
    await aconn.set_autocommit(True)
    for i in range(10):
        with pytest.raises(aconn.ProgrammingError):
            await aconn.execute("select wat")

    cur = await aconn.execute("select count(*) from pg_prepared_statements")
    assert await cur.fetchone() == (0,)


@pytest.mark.parametrize(
    "query",
    [
        "create table test_no_prepare ()",
        "notify foo, 'bar'",
        "set timezone = utc",
        "select num from prepared_test",
        "insert into prepared_test (num) values (1)",
        "update prepared_test set num = num * 2",
        "delete from prepared_test where num > 10",
    ],
)
async def test_misc_statement(aconn, query):
    await aconn.execute("create table prepared_test (num int)", prepare=False)
    aconn.prepare_threshold = 0
    await aconn.execute(query)
    cur = await aconn.execute(
        "select count(*) from pg_prepared_statements", prepare=False
    )
    assert await cur.fetchone() == (1,)


async def test_params_types(aconn):
    await aconn.execute(
        "select %s, %s, %s",
        [dt.date(2020, 12, 10), 42, Decimal(42)],
        prepare=True,
    )
    cur = await aconn.execute("select parameter_types from pg_prepared_statements")
    (rec,) = await cur.fetchall()
    assert rec[0] == ["date", "smallint", "numeric"]


async def test_evict_lru(aconn):
    aconn.prepared_max = 5
    for i in range(10):
        await aconn.execute("select 'a'")
        await aconn.execute(f"select {i}")

    assert len(aconn._prepared._names) == 1
    assert aconn._prepared._names[b"select 'a'", ()] == b"_pg3_0"
    for i in [9, 8, 7, 6]:
        assert aconn._prepared._counts[f"select {i}".encode(), ()] == 1

    cur = await aconn.execute("select statement from pg_prepared_statements")
    assert await cur.fetchall() == [("select 'a'",)]


async def test_evict_lru_deallocate(aconn):
    aconn.prepared_max = 5
    aconn.prepare_threshold = 0
    for i in range(10):
        await aconn.execute("select 'a'")
        await aconn.execute(f"select {i}")

    assert len(aconn._prepared._names) == 5
    for j in [9, 8, 7, 6, "'a'"]:
        name = aconn._prepared._names[f"select {j}".encode(), ()]
        assert name.startswith(b"_pg3_")

    cur = await aconn.execute(
        "select statement from pg_prepared_statements order by prepare_time",
        prepare=False,
    )
    assert await cur.fetchall() == [(f"select {i}",) for i in ["'a'", 6, 7, 8, 9]]


async def test_different_types(aconn):
    aconn.prepare_threshold = 0
    await aconn.execute("select %s", [None])
    await aconn.execute("select %s", [dt.date(2000, 1, 1)])
    await aconn.execute("select %s", [42])
    await aconn.execute("select %s", [41])
    await aconn.execute("select %s", [dt.date(2000, 1, 2)])
    cur = await aconn.execute(
        "select parameter_types from pg_prepared_statements order by prepare_time",
        prepare=False,
    )
    assert await cur.fetchall() == [(["text"],), (["date"],), (["smallint"],)]


async def test_untyped_json(aconn):
    aconn.prepare_threshold = 1
    await aconn.execute("create table testjson(data jsonb)")
    for i in range(2):
        await aconn.execute("insert into testjson (data) values (%s)", ["{}"])

    cur = await aconn.execute("select parameter_types from pg_prepared_statements")
    assert await cur.fetchall() == [(["jsonb"],)]
