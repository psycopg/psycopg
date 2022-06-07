"""
Prepared statements tests on async connections
"""

import datetime as dt
from decimal import Decimal

import pytest

from psycopg.rows import namedtuple_row

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("value", [None, 0, 3])
async def test_prepare_threshold_init(aconn_cls, dsn, value):
    async with await aconn_cls.connect(dsn, prepare_threshold=value) as conn:
        assert conn.prepare_threshold == value


async def test_dont_prepare(aconn):
    cur = aconn.cursor()
    for i in range(10):
        await cur.execute("select %s::int", [i], prepare=False)

    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 0


async def test_do_prepare(aconn):
    cur = aconn.cursor()
    await cur.execute("select %s::int", [10], prepare=True)
    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 1


async def test_auto_prepare(aconn):
    res = []
    for i in range(10):
        await aconn.execute("select %s::int", [0])
        stmts = await get_prepared_statements(aconn)
        res.append(len(stmts))

    assert res == [0] * 5 + [1] * 5


async def test_dont_prepare_conn(aconn):
    for i in range(10):
        await aconn.execute("select %s::int", [i], prepare=False)

    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 0


async def test_do_prepare_conn(aconn):
    await aconn.execute("select %s::int", [10], prepare=True)
    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 1


async def test_auto_prepare_conn(aconn):
    res = []
    for i in range(10):
        await aconn.execute("select %s", [0])
        stmts = await get_prepared_statements(aconn)
        res.append(len(stmts))

    assert res == [0] * 5 + [1] * 5


async def test_prepare_disable(aconn):
    aconn.prepare_threshold = None
    res = []
    for i in range(10):
        await aconn.execute("select %s", [0])
        stmts = await get_prepared_statements(aconn)
        res.append(len(stmts))

    assert res == [0] * 10
    assert not aconn._prepared._names
    assert not aconn._prepared._counts


async def test_no_prepare_multi(aconn):
    res = []
    for i in range(10):
        await aconn.execute("select 1; select 2")
        stmts = await get_prepared_statements(aconn)
        res.append(len(stmts))

    assert res == [0] * 10


async def test_no_prepare_error(aconn):
    await aconn.set_autocommit(True)
    for i in range(10):
        with pytest.raises(aconn.ProgrammingError):
            await aconn.execute("select wat")

    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 0


@pytest.mark.parametrize(
    "query",
    [
        "create table test_no_prepare ()",
        pytest.param("notify foo, 'bar'", marks=pytest.mark.crdb_skip("notify")),
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
    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 1


async def test_params_types(aconn):
    await aconn.execute(
        "select %s, %s, %s",
        [dt.date(2020, 12, 10), 42, Decimal(42)],
        prepare=True,
    )
    stmts = await get_prepared_statements(aconn)
    want = [stmt.parameter_types for stmt in stmts]
    assert want == [["date", "smallint", "numeric"]]


async def test_evict_lru(aconn):
    aconn.prepared_max = 5
    for i in range(10):
        await aconn.execute("select 'a'")
        await aconn.execute(f"select {i}")

    assert len(aconn._prepared._names) == 1
    assert aconn._prepared._names[b"select 'a'", ()] == b"_pg3_0"
    for i in [9, 8, 7, 6]:
        assert aconn._prepared._counts[f"select {i}".encode(), ()] == 1

    stmts = await get_prepared_statements(aconn)
    assert len(stmts) == 1
    assert stmts[0].statement == "select 'a'"


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

    stmts = await get_prepared_statements(aconn)
    stmts.sort(key=lambda rec: rec.prepare_time)
    got = [stmt.statement for stmt in stmts]
    assert got == [f"select {i}" for i in ["'a'", 6, 7, 8, 9]]


async def test_different_types(aconn):
    aconn.prepare_threshold = 0
    await aconn.execute("select %s", [None])
    await aconn.execute("select %s", [dt.date(2000, 1, 1)])
    await aconn.execute("select %s", [42])
    await aconn.execute("select %s", [41])
    await aconn.execute("select %s", [dt.date(2000, 1, 2)])

    stmts = await get_prepared_statements(aconn)
    stmts.sort(key=lambda rec: rec.prepare_time)
    got = [stmt.parameter_types for stmt in stmts]
    assert got == [["text"], ["date"], ["smallint"]]


async def test_untyped_json(aconn):
    aconn.prepare_threshold = 1
    await aconn.execute("create table testjson(data jsonb)")
    for i in range(2):
        await aconn.execute("insert into testjson (data) values (%s)", ["{}"])

    stmts = await get_prepared_statements(aconn)
    got = [stmt.parameter_types for stmt in stmts]
    assert got == [["jsonb"]]


async def get_prepared_statements(aconn):
    cur = aconn.cursor(row_factory=namedtuple_row)
    await cur.execute(
        r"""
select name,
    regexp_replace(statement, 'prepare _pg3_\d+ as ', '', 'i') as statement,
    prepare_time,
    parameter_types
from pg_prepared_statements
where name != ''
        """,
        prepare=False,
    )
    return await cur.fetchall()
