import gc
import pytest
import weakref
import datetime as dt

import psycopg3
from psycopg3 import sql
from psycopg3.adapt import Format

pytestmark = pytest.mark.asyncio


async def test_close(aconn):
    cur = aconn.cursor()
    assert not cur.closed
    await cur.close()
    assert cur.closed

    with pytest.raises(psycopg3.InterfaceError):
        await cur.execute("select 'foo'")

    await cur.close()
    assert cur.closed


async def test_context(aconn):
    async with aconn.cursor() as cur:
        assert not cur.closed

    assert cur.closed


async def test_weakref(aconn):
    cur = aconn.cursor()
    w = weakref.ref(cur)
    await cur.close()
    del cur
    gc.collect()
    assert w() is None


async def test_status(aconn):
    cur = aconn.cursor()
    assert cur.status is None
    await cur.execute("reset all")
    assert cur.status == cur.ExecStatus.COMMAND_OK
    await cur.execute("select 1")
    assert cur.status == cur.ExecStatus.TUPLES_OK
    await cur.close()
    assert cur.status is None


async def test_execute_many_results(aconn):
    cur = aconn.cursor()
    assert cur.nextset() is None

    rv = await cur.execute("select 'foo'; select generate_series(1,3)")
    assert rv is cur
    assert (await cur.fetchall()) == [("foo",)]
    assert cur.rowcount == 1
    assert cur.nextset()
    assert (await cur.fetchall()) == [(1,), (2,), (3,)]
    assert cur.rowcount == 3
    assert cur.nextset() is None

    await cur.close()
    assert cur.nextset() is None


async def test_execute_sequence(aconn):
    cur = aconn.cursor()
    rv = await cur.execute(
        "select %s::int, %s::text, %s::text", [1, "foo", None]
    )
    assert rv is cur
    assert len(cur._results) == 1
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.pgresult.get_value(0, 1) == b"foo"
    assert cur.pgresult.get_value(0, 2) is None
    assert cur.nextset() is None


@pytest.mark.parametrize("query", ["", " ", ";"])
async def test_execute_empty_query(aconn, query):
    cur = aconn.cursor()
    await cur.execute(query)
    assert cur.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg3.ProgrammingError):
        await cur.fetchone()


@pytest.mark.parametrize(
    "query", ["copy testcopy from stdin", "copy testcopy to stdout"]
)
async def test_execute_copy(aconn, query):
    cur = aconn.cursor()
    await cur.execute("create table testcopy (id int)")
    with pytest.raises(psycopg3.ProgrammingError):
        await cur.execute(query)


async def test_fetchone(aconn):
    cur = aconn.cursor()
    await cur.execute("select %s::int, %s::text, %s::text", [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = await cur.fetchone()
    assert row[0] == 1
    assert row[1] == "foo"
    assert row[2] is None
    row = await cur.fetchone()
    assert row is None


async def test_execute_binary_result(aconn):
    cur = aconn.cursor(binary=True)
    await cur.execute("select %s::text, %s::text", ["foo", None])
    assert cur.pgresult.fformat(0) == 1

    row = await cur.fetchone()
    assert row[0] == "foo"
    assert row[1] is None
    row = await cur.fetchone()
    assert row is None


@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
async def test_query_encode(aconn, encoding):
    await aconn.set_client_encoding(encoding)
    cur = aconn.cursor()
    await cur.execute("select '\u20ac'")
    (res,) = await cur.fetchone()
    assert res == "\u20ac"


async def test_query_badenc(aconn):
    await aconn.set_client_encoding("latin1")
    cur = aconn.cursor()
    with pytest.raises(UnicodeEncodeError):
        await cur.execute("select '\u20ac'")


@pytest.fixture(scope="function")
async def execmany(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        drop table if exists execmany;
        create table execmany (id serial primary key, num integer, data text)
        """
    )


async def test_executemany(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s)",
        [(10, "hello"), (20, "world")],
    )
    await cur.execute("select num, data from execmany order by 1")
    rv = await cur.fetchall()
    assert rv == [(10, "hello"), (20, "world")]


async def test_executemany_name(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%(num)s, %(data)s)",
        [{"num": 11, "data": "hello", "x": 1}, {"num": 21, "data": "world"}],
    )
    await cur.execute("select num, data from execmany order by 1")
    rv = await cur.fetchall()
    assert rv == [(11, "hello"), (21, "world")]


async def test_executemany_rowcount(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s)",
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2


async def test_executemany_returning_rowcount(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s) returning num",
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2


@pytest.mark.parametrize(
    "query",
    [
        "insert into nosuchtable values (%s, %s)",
        "copy (select %s, %s) to stdout",
        "wat (%s, %s)",
    ],
)
async def test_executemany_badquery(aconn, query):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        await cur.executemany(query, [(10, "hello"), (20, "world")])


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
async def test_executemany_null_first(aconn, fmt_in):
    cur = aconn.cursor()
    await cur.execute("create table testmany (a bigint, b bigint)")
    await cur.executemany(
        f"insert into testmany values (%{fmt_in}, %{fmt_in})",
        [[1, None], [3, 4]],
    )
    with pytest.raises((psycopg3.DataError, psycopg3.ProgrammingError)):
        await cur.executemany(
            f"insert into testmany values (%{fmt_in}, %{fmt_in})",
            [[1, ""], [3, 4]],
        )


async def test_rowcount(aconn):
    cur = aconn.cursor()

    await cur.execute("select 1 from generate_series(1, 0)")
    assert cur.rowcount == 0

    await cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rowcount == 42

    await cur.execute(
        "create table test_rowcount_notuples (id int primary key)"
    )
    assert cur.rowcount == -1

    await cur.execute(
        "insert into test_rowcount_notuples select generate_series(1, 42)"
    )
    assert cur.rowcount == 42

    await cur.close()
    assert cur.rowcount == -1


async def test_rownumber(aconn):
    cur = aconn.cursor()
    assert cur.rownumber is None

    await cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rownumber == 0

    await cur.fetchone()
    assert cur.rownumber == 1
    await cur.fetchone()
    assert cur.rownumber == 2
    await cur.fetchmany(10)
    assert cur.rownumber == 12
    rns = []
    async for i in cur:
        rns.append(cur.rownumber)
        if len(rns) >= 3:
            break
    assert rns == [13, 14, 15]
    assert len(await cur.fetchall()) == 42 - rns[-1]
    assert cur.rownumber == 42


async def test_iter(aconn):
    cur = aconn.cursor()
    await cur.execute("select generate_series(1, 3)")
    res = []
    async for rec in cur:
        res.append(rec)
    assert res == [(1,), (2,), (3,)]


async def test_iter_stop(aconn):
    cur = aconn.cursor()
    await cur.execute("select generate_series(1, 3)")
    async for rec in cur:
        assert rec == (1,)
        break

    async for rec in cur:
        assert rec == (2,)
        break

    assert (await cur.fetchone()) == (3,)
    async for rec in cur:
        assert False


async def test_scroll(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.ProgrammingError):
        await cur.scroll(0)

    await cur.execute("select generate_series(0,9)")
    await cur.scroll(2)
    assert await cur.fetchone() == (2,)
    await cur.scroll(2)
    assert await cur.fetchone() == (5,)
    await cur.scroll(2, mode="relative")
    assert await cur.fetchone() == (8,)
    await cur.scroll(-1)
    assert await cur.fetchone() == (8,)
    await cur.scroll(-2)
    assert await cur.fetchone() == (7,)
    await cur.scroll(2, mode="absolute")
    assert await cur.fetchone() == (2,)

    # on the boundary
    await cur.scroll(0, mode="absolute")
    assert await cur.fetchone() == (0,)
    with pytest.raises(IndexError):
        await cur.scroll(-1, mode="absolute")

    await cur.scroll(0, mode="absolute")
    with pytest.raises(IndexError):
        await cur.scroll(-1)

    await cur.scroll(9, mode="absolute")
    assert await cur.fetchone() == (9,)
    with pytest.raises(IndexError):
        await cur.scroll(10, mode="absolute")

    await cur.scroll(9, mode="absolute")
    with pytest.raises(IndexError):
        await cur.scroll(1)

    with pytest.raises(ValueError):
        await cur.scroll(1, "wat")


async def test_query_params_execute(aconn):
    cur = aconn.cursor()
    assert cur.query is None
    assert cur.params is None

    await cur.execute("select %t, %s::text", [1, None])
    assert cur.query == b"select $1, $2::text"
    assert cur.params == [b"1", None]

    await cur.execute("select 1")
    assert cur.query == b"select 1"
    assert cur.params is None

    with pytest.raises(psycopg3.DataError):
        await cur.execute("select %t::int", ["wat"])

    assert cur.query == b"select $1::int"
    assert cur.params == [b"wat"]


async def test_query_params_executemany(aconn):
    cur = aconn.cursor()

    await cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur.query == b"select $1, $2"
    assert cur.params == [b"3", b"4"]

    with pytest.raises((psycopg3.DataError, TypeError)):
        await cur.executemany("select %t::int", [[1], ["x"], [2]])
    assert cur.query == b"select $1::int"
    # TODO: cannot really check this: after introduced row_dumpers, this
    # fails dumping, not query passing.
    # assert cur.params == [b"x"]


async def test_stream(aconn):
    cur = aconn.cursor()
    recs = []
    async for rec in cur.stream(
        "select i, '2021-01-01'::date + i from generate_series(1, %s) as i",
        [2],
    ):
        recs.append(rec)

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


async def test_stream_sql(aconn):
    cur = aconn.cursor()
    recs = []
    async for rec in cur.stream(
        sql.SQL(
            "select i, '2021-01-01'::date + i from generate_series(1, {}) as i"
        ).format(2)
    ):
        recs.append(rec)

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


@pytest.mark.parametrize(
    "query",
    [
        "create table test_stream_badq ()",
        "copy (select 1) to stdout",
        "wat?",
    ],
)
async def test_stream_badquery(aconn, query):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.ProgrammingError):
        async for rec in cur.stream(query):
            pass


async def test_str(aconn):
    cur = aconn.cursor()
    assert "[IDLE]" in str(cur)
    assert "[closed]" not in str(cur)
    assert "[no result]" in str(cur)
    await cur.execute("select 1")
    assert "[INTRANS]" in str(cur)
    assert "[TUPLES_OK]" in str(cur)
    assert "[closed]" not in str(cur)
    assert "[no result]" not in str(cur)
    await cur.close()
    assert "[closed]" in str(cur)
    assert "[INTRANS]" in str(cur)


@pytest.mark.slow
@pytest.mark.parametrize("fmt", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
async def test_leak(dsn, faker, fmt, fetch):
    faker.format = fmt
    faker.choose_schema(ncols=5)
    faker.make_records(10)

    n = []
    for i in range(3):
        async with await psycopg3.AsyncConnection.connect(dsn) as conn:
            async with conn.cursor(binary=Format.as_pq(fmt)) as cur:
                await cur.execute(faker.drop_stmt)
                await cur.execute(faker.create_stmt)
                await cur.executemany(faker.insert_stmt, faker.records)
                await cur.execute(faker.select_stmt)

                if fetch == "one":
                    while 1:
                        tmp = await cur.fetchone()
                        if tmp is None:
                            break
                elif fetch == "many":
                    while 1:
                        tmp = await cur.fetchmany(3)
                        if not tmp:
                            break
                elif fetch == "all":
                    await cur.fetchall()
                elif fetch == "iter":
                    async for rec in cur:
                        pass

                tmp = None

        del cur, conn
        gc.collect()
        gc.collect()
        n.append(len(gc.get_objects()))

    assert (
        n[0] == n[1] == n[2]
    ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"
