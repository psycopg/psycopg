import pytest
import psycopg
from psycopg import pq, rows, errors as e
from psycopg.adapt import PyFormat

from .test_cursor import ph
from .utils import gc_collect, gc_count


@pytest.fixture
async def aconn(aconn, anyio_backend):
    aconn.cursor_factory = psycopg.AsyncRawCursor
    return aconn


async def test_default_cursor(aconn):
    cur = aconn.cursor()
    assert type(cur) is psycopg.AsyncRawCursor


async def test_str(aconn):
    cur = aconn.cursor()
    assert "psycopg.AsyncRawCursor" in str(cur)


async def test_sequence_only(aconn):
    cur = aconn.cursor()
    await cur.execute("select 1", ())
    assert await cur.fetchone() == (1,)

    with pytest.raises(TypeError, match="sequence"):
        await cur.execute("select 1", {})


async def test_execute_many_results_param(aconn):
    cur = aconn.cursor()
    # Postgres raises SyntaxError, CRDB raises InvalidPreparedStatementDefinition
    with pytest.raises((e.SyntaxError, e.InvalidPreparedStatementDefinition)):
        await cur.execute("select $1; select generate_series(1, $2)", ("foo", 3))


async def test_query_params_execute(aconn):
    cur = aconn.cursor()
    assert cur._query is None

    await cur.execute("select $1, $2::text", [1, None])
    assert cur._query is not None
    assert cur._query.query == b"select $1, $2::text"
    assert cur._query.params == [b"\x00\x01", None]

    await cur.execute("select 1")
    assert cur._query.query == b"select 1"
    assert not cur._query.params

    with pytest.raises(psycopg.DataError):
        await cur.execute("select $1::int", ["wat"])

    assert cur._query.query == b"select $1::int"
    assert cur._query.params == [b"wat"]


async def test_query_params_executemany(aconn):
    cur = aconn.cursor()

    await cur.executemany("select $1, $2", [[1, 2], [3, 4]])
    assert cur._query.query == b"select $1, $2"
    assert cur._query.params == [b"\x00\x03", b"\x00\x04"]


@pytest.mark.slow
@pytest.mark.parametrize("fmt", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
@pytest.mark.parametrize("row_factory", ["tuple_row", "dict_row", "namedtuple_row"])
async def test_leak(aconn_cls, dsn, faker, fmt, fmt_out, fetch, row_factory):
    faker.format = fmt
    faker.choose_schema(ncols=5)
    faker.make_records(10)
    row_factory = getattr(rows, row_factory)

    async def work():
        async with await aconn_cls.connect(dsn) as aconn:
            async with aconn.transaction(force_rollback=True):
                async with aconn.cursor(binary=fmt_out, row_factory=row_factory) as cur:
                    await cur.execute(faker.drop_stmt)
                    await cur.execute(faker.create_stmt)
                    async with faker.find_insert_problem_async(aconn):
                        await cur.executemany(faker.insert_stmt, faker.records)
                    await cur.execute(ph(cur, faker.select_stmt))

                    if fetch == "one":
                        while True:
                            tmp = await cur.fetchone()
                            if tmp is None:
                                break
                    elif fetch == "many":
                        while True:
                            tmp = await cur.fetchmany(3)
                            if not tmp:
                                break
                    elif fetch == "all":
                        await cur.fetchall()
                    elif fetch == "iter":
                        async for rec in cur:
                            pass

    n = []
    gc_collect()
    for i in range(3):
        await work()
        gc_collect()
        n.append(gc_count())
    assert n[0] == n[1] == n[2], f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"
