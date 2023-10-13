import datetime as dt

import pytest
import psycopg
from psycopg import rows

from .utils import gc_collect, gc_count
from .fix_crdb import crdb_encoding


@pytest.fixture
async def aconn(aconn, anyio_backend):
    aconn.cursor_factory = psycopg.AsyncClientCursor
    return aconn


async def test_default_cursor(aconn):
    cur = aconn.cursor()
    assert type(cur) is psycopg.AsyncClientCursor


async def test_str(aconn):
    cur = aconn.cursor()
    assert "psycopg.%s" % psycopg.AsyncClientCursor.__name__ in str(cur)


async def test_from_cursor_factory(aconn_cls, dsn):
    async with await aconn_cls.connect(
        dsn, cursor_factory=psycopg.AsyncClientCursor
    ) as aconn:
        cur = aconn.cursor()
        assert type(cur) is psycopg.AsyncClientCursor


async def test_execute_many_results_param(aconn):
    cur = aconn.cursor()
    assert cur.nextset() is None

    rv = await cur.execute("select %s; select generate_series(1, %s)", ("foo", 3))
    assert rv is cur
    assert (await cur.fetchall()) == [("foo",)]
    assert cur.rowcount == 1
    assert cur.nextset()
    assert (await cur.fetchall()) == [(1,), (2,), (3,)]
    assert cur.nextset() is None

    await cur.close()
    assert cur.nextset() is None


async def test_query_params_execute(aconn):
    cur = aconn.cursor()
    assert cur._query is None

    await cur.execute("select %t, %s::text", [1, None])
    assert cur._query is not None
    assert cur._query.query == b"select 1, NULL::text"
    assert cur._query.params == (b"1", b"NULL")

    await cur.execute("select 1")
    assert cur._query.query == b"select 1"
    assert not cur._query.params

    with pytest.raises(psycopg.DataError):
        await cur.execute("select %t::int", ["wat"])

    assert cur._query.query == b"select 'wat'::int"
    assert cur._query.params == (b"'wat'",)


async def test_query_params_executemany(aconn):
    cur = aconn.cursor()

    await cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur._query.query == b"select 3, 4"
    assert cur._query.params == (b"3", b"4")


@pytest.mark.slow
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
@pytest.mark.parametrize("row_factory", ["tuple_row", "dict_row", "namedtuple_row"])
async def test_leak(aconn_cls, dsn, faker, fetch, row_factory):
    faker.choose_schema(ncols=5)
    faker.make_records(10)
    row_factory = getattr(rows, row_factory)

    async def work():
        async with await aconn_cls.connect(dsn) as conn, conn.transaction(
            force_rollback=True
        ):
            async with psycopg.AsyncClientCursor(conn, row_factory=row_factory) as cur:
                await cur.execute(faker.drop_stmt)
                await cur.execute(faker.create_stmt)
                async with faker.find_insert_problem_async(conn):
                    await cur.executemany(faker.insert_stmt, faker.records)
                await cur.execute(faker.select_stmt)

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


@pytest.mark.parametrize(
    "query, params, want",
    [
        ("select 'hello'", (), "select 'hello'"),
        ("select %s, %s", ([1, dt.date(2020, 1, 1)],), "select 1, '2020-01-01'::date"),
        ("select %(foo)s, %(foo)s", ({"foo": "x"},), "select 'x', 'x'"),
        ("select %%", (), "select %%"),
        ("select %%, %s", (["a"],), "select %, 'a'"),
        ("select %%, %(foo)s", ({"foo": "x"},), "select %, 'x'"),
        ("select %%s, %(foo)s", ({"foo": "x"},), "select %s, 'x'"),
    ],
)
async def test_mogrify(aconn, query, params, want):
    cur = aconn.cursor()
    got = cur.mogrify(query, *params)
    assert got == want


@pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
async def test_mogrify_encoding(aconn, encoding):
    await aconn.execute(f"set client_encoding to {encoding}")
    q = aconn.cursor().mogrify("select %(s)s", {"s": "\u20ac"})
    assert q == "select '\u20ac'"


@pytest.mark.parametrize("encoding", [crdb_encoding("latin1")])
async def test_mogrify_badenc(aconn, encoding):
    await aconn.execute(f"set client_encoding to {encoding}")
    with pytest.raises(UnicodeEncodeError):
        aconn.cursor().mogrify("select %(s)s", {"s": "\u20ac"})
