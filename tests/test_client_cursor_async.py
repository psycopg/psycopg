import gc
import pytest
import weakref
import datetime as dt
from typing import List

import psycopg
from psycopg import sql, rows
from psycopg.adapt import PyFormat

from .utils import alist, gc_collect
from .test_cursor import my_row_factory
from .test_cursor import execmany, _execmany  # noqa: F401
from .fix_crdb import crdb_encoding

execmany = execmany  # avoid F811 underneath
pytestmark = pytest.mark.asyncio


@pytest.fixture
async def aconn(aconn):
    aconn.cursor_factory = psycopg.AsyncClientCursor
    return aconn


async def test_init(aconn):
    cur = psycopg.AsyncClientCursor(aconn)
    await cur.execute("select 1")
    assert (await cur.fetchone()) == (1,)

    aconn.row_factory = rows.dict_row
    cur = psycopg.AsyncClientCursor(aconn)
    await cur.execute("select 1 as a")
    assert (await cur.fetchone()) == {"a": 1}


async def test_init_factory(aconn):
    cur = psycopg.AsyncClientCursor(aconn, row_factory=rows.dict_row)
    await cur.execute("select 1 as a")
    assert (await cur.fetchone()) == {"a": 1}


async def test_from_cursor_factory(aconn_cls, dsn):
    async with await aconn_cls.connect(
        dsn, cursor_factory=psycopg.AsyncClientCursor
    ) as aconn:
        cur = aconn.cursor()
        assert type(cur) is psycopg.AsyncClientCursor

        await cur.execute("select %s", (1,))
        assert await cur.fetchone() == (1,)
        assert cur._query
        assert cur._query.query == b"select 1"


async def test_close(aconn):
    cur = aconn.cursor()
    assert not cur.closed
    await cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        await cur.execute("select 'foo'")

    await cur.close()
    assert cur.closed


async def test_cursor_close_fetchone(aconn):
    cur = aconn.cursor()
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    await cur.execute(query)
    for _ in range(5):
        await cur.fetchone()

    await cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        await cur.fetchone()


async def test_cursor_close_fetchmany(aconn):
    cur = aconn.cursor()
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    await cur.execute(query)
    assert len(await cur.fetchmany(2)) == 2

    await cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        await cur.fetchmany(2)


async def test_cursor_close_fetchall(aconn):
    cur = aconn.cursor()
    assert not cur.closed

    query = "select * from generate_series(1, 10)"
    await cur.execute(query)
    assert len(await cur.fetchall()) == 10

    await cur.close()
    assert cur.closed

    with pytest.raises(psycopg.InterfaceError):
        await cur.fetchall()


async def test_context(aconn):
    async with aconn.cursor() as cur:
        assert not cur.closed

    assert cur.closed


@pytest.mark.slow
async def test_weakref(aconn):
    cur = aconn.cursor()
    w = weakref.ref(cur)
    await cur.close()
    del cur
    gc_collect()
    assert w() is None


async def test_pgresult(aconn):
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert cur.pgresult
    await cur.close()
    assert not cur.pgresult


async def test_statusmessage(aconn):
    cur = aconn.cursor()
    assert cur.statusmessage is None

    await cur.execute("select generate_series(1, 10)")
    assert cur.statusmessage == "SELECT 10"

    await cur.execute("create table statusmessage ()")
    assert cur.statusmessage == "CREATE TABLE"

    with pytest.raises(psycopg.ProgrammingError):
        await cur.execute("wat")
    assert cur.statusmessage is None


async def test_execute_sql(aconn):
    cur = aconn.cursor()
    await cur.execute(sql.SQL("select {value}").format(value="hello"))
    assert await cur.fetchone() == ("hello",)


async def test_execute_many_results(aconn):
    cur = aconn.cursor()
    assert cur.nextset() is None

    rv = await cur.execute("select %s; select generate_series(1,%s)", ("foo", 3))
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
    rv = await cur.execute("select %s::int, %s::text, %s::text", [1, "foo", None])
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
    assert cur.pgresult.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()


async def test_execute_type_change(aconn):
    # issue #112
    await aconn.execute("create table bug_112 (num integer)")
    sql = "insert into bug_112 (num) values (%s)"
    cur = aconn.cursor()
    await cur.execute(sql, (1,))
    await cur.execute(sql, (100_000,))
    await cur.execute("select num from bug_112 order by num")
    assert (await cur.fetchall()) == [(1,), (100_000,)]


async def test_executemany_type_change(aconn):
    await aconn.execute("create table bug_112 (num integer)")
    sql = "insert into bug_112 (num) values (%s)"
    cur = aconn.cursor()
    await cur.executemany(sql, [(1,), (100_000,)])
    await cur.execute("select num from bug_112 order by num")
    assert (await cur.fetchall()) == [(1,), (100_000,)]


@pytest.mark.parametrize(
    "query", ["copy testcopy from stdin", "copy testcopy to stdout"]
)
async def test_execute_copy(aconn, query):
    cur = aconn.cursor()
    await cur.execute("create table testcopy (id int)")
    with pytest.raises(psycopg.ProgrammingError):
        await cur.execute(query)


async def test_fetchone(aconn):
    cur = aconn.cursor()
    await cur.execute("select %s::int, %s::text, %s::text", [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = await cur.fetchone()
    assert row == (1, "foo", None)
    row = await cur.fetchone()
    assert row is None


async def test_binary_cursor_execute(aconn):
    with pytest.raises(psycopg.NotSupportedError):
        cur = aconn.cursor(binary=True)
        await cur.execute("select %s, %s", [1, None])


async def test_execute_binary(aconn):
    with pytest.raises(psycopg.NotSupportedError):
        cur = aconn.cursor()
        await cur.execute("select %s, %s", [1, None], binary=True)


async def test_binary_cursor_text_override(aconn):
    cur = aconn.cursor(binary=True)
    await cur.execute("select %s, %s", [1, None], binary=False)
    assert (await cur.fetchone()) == (1, None)
    assert cur.pgresult.fformat(0) == 0
    assert cur.pgresult.get_value(0, 0) == b"1"


@pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
async def test_query_encode(aconn, encoding):
    await aconn.execute(f"set client_encoding to {encoding}")
    cur = aconn.cursor()
    await cur.execute("select '\u20ac'")
    (res,) = await cur.fetchone()
    assert res == "\u20ac"


@pytest.mark.parametrize("encoding", [crdb_encoding("latin1")])
async def test_query_badenc(aconn, encoding):
    await aconn.execute(f"set client_encoding to {encoding}")
    cur = aconn.cursor()
    with pytest.raises(UnicodeEncodeError):
        await cur.execute("select '\u20ac'")


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


async def test_executemany_no_data(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany("insert into execmany(num, data) values (%s, %s)", [])
    assert cur.rowcount == 0


async def test_executemany_rowcount(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s)",
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2


async def test_executemany_returning(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s) returning num",
        [(10, "hello"), (20, "world")],
        returning=True,
    )
    assert cur.rowcount == 2
    assert (await cur.fetchone()) == (10,)
    assert cur.nextset()
    assert (await cur.fetchone()) == (20,)
    assert cur.nextset() is None


async def test_executemany_returning_discard(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s) returning num",
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()
    assert cur.nextset() is None


async def test_executemany_no_result(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        "insert into execmany(num, data) values (%s, %s)",
        [(10, "hello"), (20, "world")],
        returning=True,
    )
    assert cur.rowcount == 2
    assert cur.statusmessage.startswith("INSERT")
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()
    pgresult = cur.pgresult
    assert cur.nextset()
    assert cur.statusmessage.startswith("INSERT")
    assert pgresult is not cur.pgresult
    assert cur.nextset() is None


async def test_executemany_rowcount_no_hit(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany("delete from execmany where id = %s", [(-1,), (-2,)])
    assert cur.rowcount == 0
    await cur.executemany("delete from execmany where id = %s", [])
    assert cur.rowcount == 0
    await cur.executemany(
        "delete from execmany where id = %s returning num", [(-1,), (-2,)]
    )
    assert cur.rowcount == 0


@pytest.mark.parametrize(
    "query",
    [
        "insert into nosuchtable values (%s, %s)",
        # This fails because we end up trying to copy in pipeline mode.
        # However, sometimes (and pretty regularly if we enable pgconn.trace())
        # something goes in a loop and only terminates by OOM. Strace shows
        # an allocation loop. I think it's in the libpq.
        # "copy (select %s, %s) to stdout",
        "wat (%s, %s)",
    ],
)
async def test_executemany_badquery(aconn, query):
    cur = aconn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        await cur.executemany(query, [(10, "hello"), (20, "world")])


@pytest.mark.parametrize("fmt_in", PyFormat)
async def test_executemany_null_first(aconn, fmt_in):
    cur = aconn.cursor()
    await cur.execute("create table testmany (a bigint, b bigint)")
    await cur.executemany(
        f"insert into testmany values (%{fmt_in.value}, %{fmt_in.value})",
        [[1, None], [3, 4]],
    )
    with pytest.raises((psycopg.DataError, psycopg.ProgrammingError)):
        await cur.executemany(
            f"insert into testmany values (%{fmt_in.value}, %{fmt_in.value})",
            [[1, ""], [3, 4]],
        )


async def test_rowcount(aconn):
    cur = aconn.cursor()

    await cur.execute("select 1 from generate_series(1, 0)")
    assert cur.rowcount == 0

    await cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rowcount == 42

    await cur.execute("create table test_rowcount_notuples (id int primary key)")
    assert cur.rowcount == -1

    await cur.execute(
        "insert into test_rowcount_notuples select generate_series(1, 42)"
    )
    assert cur.rowcount == 42


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
    rns: List[int] = []
    async for i in cur:
        assert cur.rownumber
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


async def test_row_factory(aconn):
    cur = aconn.cursor(row_factory=my_row_factory)
    await cur.execute("select 'foo' as bar")
    (r,) = await cur.fetchone()
    assert r == "FOObar"

    await cur.execute("select 'x' as x; select 'y' as y, 'z' as z")
    assert await cur.fetchall() == [["Xx"]]
    assert cur.nextset()
    assert await cur.fetchall() == [["Yy", "Zz"]]

    await cur.scroll(-1)
    cur.row_factory = rows.dict_row
    assert await cur.fetchone() == {"y": "y", "z": "z"}


async def test_row_factory_none(aconn):
    cur = aconn.cursor(row_factory=None)
    assert cur.row_factory is rows.tuple_row
    await cur.execute("select 1 as a, 2 as b")
    r = await cur.fetchone()
    assert type(r) is tuple
    assert r == (1, 2)


async def test_bad_row_factory(aconn):
    def broken_factory(cur):
        1 / 0

    cur = aconn.cursor(row_factory=broken_factory)
    with pytest.raises(ZeroDivisionError):
        await cur.execute("select 1")

    def broken_maker(cur):
        def make_row(seq):
            1 / 0

        return make_row

    cur = aconn.cursor(row_factory=broken_maker)
    await cur.execute("select 1")
    with pytest.raises(ZeroDivisionError):
        await cur.fetchone()


async def test_scroll(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
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


@pytest.mark.parametrize(
    "query, params, want",
    [
        ("select %(x)s", {"x": 1}, (1,)),
        ("select %(x)s, %(y)s", {"x": 1, "y": 2}, (1, 2)),
        ("select %(x)s, %(x)s", {"x": 1}, (1, 1)),
    ],
)
async def test_query_params_named(aconn, query, params, want):
    cur = aconn.cursor()
    await cur.execute(query, params)
    rec = await cur.fetchone()
    assert rec == want


async def test_query_params_executemany(aconn):
    cur = aconn.cursor()

    await cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur._query.query == b"select 3, 4"
    assert cur._query.params == (b"3", b"4")


@pytest.mark.crdb_skip("copy")
@pytest.mark.parametrize("ph, params", [("%s", (10,)), ("%(n)s", {"n": 10})])
async def test_copy_out_param(aconn, ph, params):
    cur = aconn.cursor()
    async with cur.copy(
        f"copy (select * from generate_series(1, {ph})) to stdout", params
    ) as copy:
        copy.set_types(["int4"])
        assert await alist(copy.rows()) == [(i + 1,) for i in range(10)]

    assert aconn.info.transaction_status == aconn.TransactionStatus.INTRANS


async def test_stream(aconn):
    cur = aconn.cursor()
    recs = []
    async for rec in cur.stream(
        "select i, '2021-01-01'::date + i from generate_series(1, %s) as i",
        [2],
    ):
        recs.append(rec)

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


async def test_str(aconn):
    cur = aconn.cursor()
    assert "psycopg.AsyncClientCursor" in str(cur)
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
        n.append(len(gc.get_objects()))

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


@pytest.mark.pipeline
async def test_message_0x33(aconn):
    # https://github.com/psycopg/psycopg/issues/314
    notices = []
    aconn.add_notice_handler(lambda diag: notices.append(diag.message_primary))

    await aconn.set_autocommit(True)
    async with aconn.pipeline():
        cur = await aconn.execute("select 'test'")
        assert (await cur.fetchone()) == ("test",)

    assert not notices
