import pytest
import weakref
import datetime as dt
from typing import List

import psycopg
from psycopg import sql, rows
from psycopg.adapt import PyFormat
from psycopg.types import TypeInfo

from .utils import alist, gc_collect, raiseif
from .test_cursor import my_row_factory, ph
from .test_cursor import execmany, _execmany  # noqa: F401
from .fix_crdb import crdb_encoding

execmany = execmany  # avoid F811 underneath


@pytest.fixture(
    params=[psycopg.AsyncCursor, psycopg.AsyncClientCursor, psycopg.AsyncRawCursor]
)
def aconn(aconn, request, anyio_backend):
    aconn.cursor_factory = request.param
    return aconn


async def test_init(aconn):
    cur = aconn.cursor_factory(aconn)
    await cur.execute("select 1")
    assert (await cur.fetchone()) == (1,)

    aconn.row_factory = rows.dict_row
    cur = aconn.cursor_factory(aconn)
    await cur.execute("select 1 as a")
    assert (await cur.fetchone()) == {"a": 1}


async def test_init_factory(aconn):
    cur = aconn.cursor_factory(aconn, row_factory=rows.dict_row)
    await cur.execute("select 1 as a")
    assert (await cur.fetchone()) == {"a": 1}


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
    assert (await cur.fetchone()) == ("hello",)


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
        ph(cur, "select %s::int, %s::text, %s::text"), [1, "foo", None]
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
    assert cur.pgresult.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()


async def test_execute_type_change(aconn):
    # issue #112
    await aconn.execute("create table bug_112 (num integer)")
    cur = aconn.cursor()
    sql = ph(cur, "insert into bug_112 (num) values (%s)")
    await cur.execute(sql, (1,))
    await cur.execute(sql, (100_000,))
    await cur.execute("select num from bug_112 order by num")
    assert (await cur.fetchall()) == [(1,), (100_000,)]


async def test_executemany_type_change(aconn):
    await aconn.execute("create table bug_112 (num integer)")
    cur = aconn.cursor()
    sql = ph(cur, "insert into bug_112 (num) values (%s)")
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
    await cur.execute(ph(cur, "select %s::int, %s::text, %s::text"), [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = await cur.fetchone()
    assert row == (1, "foo", None)
    row = await cur.fetchone()
    assert row is None


async def test_binary_cursor_execute(aconn):
    with raiseif(
        aconn.cursor_factory is psycopg.AsyncClientCursor, psycopg.NotSupportedError
    ) as ex:
        cur = aconn.cursor(binary=True)
        await cur.execute(ph(cur, "select %s, %s"), [1, None])
    if ex:
        return

    assert (await cur.fetchone()) == (1, None)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x01"


async def test_execute_binary(aconn):
    cur = aconn.cursor()
    with raiseif(
        aconn.cursor_factory is psycopg.AsyncClientCursor, psycopg.NotSupportedError
    ) as ex:
        await cur.execute(ph(cur, "select %s, %s"), [1, None], binary=True)
    if ex:
        return

    assert (await cur.fetchone()) == (1, None)
    assert cur.pgresult.fformat(0) == 1
    assert cur.pgresult.get_value(0, 0) == b"\x00\x01"


async def test_binary_cursor_text_override(aconn):
    cur = aconn.cursor(binary=True)
    await cur.execute(ph(cur, "select %s, %s"), [1, None], binary=False)
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
        ph(cur, "insert into execmany(num, data) values (%s, %s)"),
        [(10, "hello"), (20, "world")],
    )
    await cur.execute("select num, data from execmany order by 1")
    rv = await cur.fetchall()
    assert rv == [(10, "hello"), (20, "world")]


async def test_executemany_name(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%(num)s, %(data)s)"),
        [{"num": 11, "data": "hello", "x": 1}, {"num": 21, "data": "world"}],
    )
    await cur.execute("select num, data from execmany order by 1")
    rv = await cur.fetchall()
    assert rv == [(11, "hello"), (21, "world")]


async def test_executemany_no_data(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s)"), []
    )
    assert cur.rowcount == 0


async def test_executemany_rowcount(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s)"),
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2


async def test_executemany_returning(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s) returning num"),
        [(10, "hello"), (20, "world")],
        returning=True,
    )
    assert cur.rowcount == 1
    assert (await cur.fetchone()) == (10,)
    assert cur.nextset()
    assert cur.rowcount == 1
    assert (await cur.fetchone()) == (20,)
    assert cur.nextset() is None


async def test_executemany_returning_discard(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s) returning num"),
        [(10, "hello"), (20, "world")],
    )
    assert cur.rowcount == 2
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()
    assert cur.nextset() is None


async def test_executemany_no_result(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(
        ph(cur, "insert into execmany(num, data) values (%s, %s)"),
        [(10, "hello"), (20, "world")],
        returning=True,
    )
    assert cur.rowcount == 1
    assert cur.statusmessage.startswith("INSERT")
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()
    pgresult = cur.pgresult
    assert cur.nextset()
    assert cur.rowcount == 1
    assert cur.statusmessage.startswith("INSERT")
    assert pgresult is not cur.pgresult
    assert cur.nextset() is None


async def test_executemany_rowcount_no_hit(aconn, execmany):
    cur = aconn.cursor()
    await cur.executemany(ph(cur, "delete from execmany where id = %s"), [(-1,), (-2,)])
    assert cur.rowcount == 0
    await cur.executemany(ph(cur, "delete from execmany where id = %s"), [])
    assert cur.rowcount == 0
    await cur.executemany(
        ph(cur, "delete from execmany where id = %s returning num"), [(-1,), (-2,)]
    )
    assert cur.rowcount == 0


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
    with pytest.raises(psycopg.DatabaseError):
        await cur.executemany(ph(cur, query), [(10, "hello"), (20, "world")])


@pytest.mark.parametrize("fmt_in", PyFormat)
async def test_executemany_null_first(aconn, fmt_in):
    cur = aconn.cursor()
    await cur.execute("create table testmany (a bigint, b bigint)")
    await cur.executemany(
        ph(cur, f"insert into testmany values (%{fmt_in.value}, %{fmt_in.value})"),
        [[1, None], [3, 4]],
    )
    with pytest.raises((psycopg.DataError, psycopg.ProgrammingError)):
        await cur.executemany(
            ph(cur, f"insert into testmany values (%{fmt_in.value}, %{fmt_in.value})"),
            [[1, ""], [3, 4]],
        )


async def test_rowcount(aconn):
    cur = aconn.cursor()

    await cur.execute("select 1 from generate_series(1, 0)")
    assert cur.rowcount == 0

    await cur.execute("select 1 from generate_series(1, 42)")
    assert cur.rowcount == 42

    await cur.execute("show timezone")
    assert cur.rowcount == 1

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


@pytest.mark.parametrize("query", ["", "set timezone to utc"])
async def test_rownumber_none(aconn, query):
    cur = aconn.cursor()
    await cur.execute(query)
    assert cur.rownumber is None


async def test_rownumber_mixed(aconn):
    cur = aconn.cursor()
    await cur.execute(
        """
select x from generate_series(1, 3) x;
set timezone to utc;
select x from generate_series(4, 6) x;
"""
    )
    assert cur.rownumber == 0
    assert await cur.fetchone() == (1,)
    assert cur.rownumber == 1
    assert await cur.fetchone() == (2,)
    assert cur.rownumber == 2
    cur.nextset()
    assert cur.rownumber is None
    cur.nextset()
    assert cur.rownumber == 0
    assert await cur.fetchone() == (4,)
    assert cur.rownumber == 1


async def test_iter(aconn):
    cur = aconn.cursor()
    await cur.execute("select generate_series(1, 3)")
    assert await alist(cur) == [(1,), (2,), (3,)]


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

    await cur.execute("reset search_path")
    with pytest.raises(psycopg.ProgrammingError):
        await cur.fetchone()

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


@pytest.mark.parametrize(
    "query, params, want",
    [
        ("select %(x)s", {"x": 1}, (1,)),
        ("select %(x)s, %(y)s", {"x": 1, "y": 2}, (1, 2)),
        ("select %(x)s, %(x)s", {"x": 1}, (1, 1)),
    ],
)
async def test_execute_params_named(aconn, query, params, want):
    cur = aconn.cursor()
    await cur.execute(ph(cur, query), params)
    rec = await cur.fetchone()
    assert rec == want


async def test_stream(aconn):
    cur = aconn.cursor()
    recs = []
    async for rec in cur.stream(
        ph(cur, "select i, '2021-01-01'::date + i from generate_series(1, %s) as i"),
        [2],
    ):
        recs.append(rec)

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


async def test_stream_sql(aconn):
    cur = aconn.cursor()
    recs = await alist(
        cur.stream(
            sql.SQL(
                "select i, '2021-01-01'::date + i from generate_series(1, {}) as i"
            ).format(2)
        )
    )

    assert recs == [(1, dt.date(2021, 1, 2)), (2, dt.date(2021, 1, 3))]


async def test_stream_row_factory(aconn):
    cur = aconn.cursor(row_factory=rows.dict_row)
    ait = cur.stream("select generate_series(1,2) as a")
    assert (await ait.__anext__())["a"] == 1
    cur.row_factory = rows.namedtuple_row
    assert (await ait.__anext__()).a == 2


async def test_stream_no_row(aconn):
    cur = aconn.cursor()
    recs = [rec async for rec in cur.stream("select generate_series(2,1) as a")]
    assert recs == []


@pytest.mark.crdb_skip("no col query")
async def test_stream_no_col(aconn):
    cur = aconn.cursor()
    recs = [rec async for rec in cur.stream("select")]
    assert recs == [()]


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
    with pytest.raises(psycopg.ProgrammingError):
        async for rec in cur.stream(query):
            pass


async def test_stream_error_tx(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        async for rec in cur.stream("wat"):
            pass
    assert aconn.info.transaction_status == aconn.TransactionStatus.INERROR


async def test_stream_error_notx(aconn):
    await aconn.set_autocommit(True)
    cur = aconn.cursor()
    with pytest.raises(psycopg.ProgrammingError):
        async for rec in cur.stream("wat"):
            pass
    assert aconn.info.transaction_status == aconn.TransactionStatus.IDLE


async def test_stream_error_python_to_consume(aconn):
    cur = aconn.cursor()
    with pytest.raises(ZeroDivisionError):
        gen = cur.stream("select generate_series(1, 10000)")
        async for rec in gen:
            1 / 0

    await gen.aclose()
    assert aconn.info.transaction_status in (
        aconn.TransactionStatus.INTRANS,
        aconn.TransactionStatus.INERROR,
    )


async def test_stream_error_python_consumed(aconn):
    cur = aconn.cursor()
    with pytest.raises(ZeroDivisionError):
        gen = cur.stream("select 1")
        async for rec in gen:
            1 / 0

    await gen.aclose()
    assert aconn.info.transaction_status == aconn.TransactionStatus.INTRANS


@pytest.mark.parametrize("autocommit", [False, True])
async def test_stream_close(aconn, autocommit):
    await aconn.set_autocommit(autocommit)
    cur = aconn.cursor()
    with pytest.raises(psycopg.OperationalError):
        async for rec in cur.stream("select generate_series(1, 3)"):
            if rec[0] == 1:
                await aconn.close()
            else:
                assert False

    assert aconn.closed


async def test_stream_binary_cursor(aconn):
    with raiseif(
        aconn.cursor_factory is psycopg.AsyncClientCursor, psycopg.NotSupportedError
    ):
        cur = aconn.cursor(binary=True)
        recs = []
        async for rec in cur.stream("select x::int4 from generate_series(1, 2) x"):
            recs.append(rec)
            assert cur.pgresult.fformat(0) == 1
            assert cur.pgresult.get_value(0, 0) == bytes([0, 0, 0, rec[0]])

        assert recs == [(1,), (2,)]


async def test_stream_execute_binary(aconn):
    cur = aconn.cursor()
    recs = []
    with raiseif(
        aconn.cursor_factory is psycopg.AsyncClientCursor, psycopg.NotSupportedError
    ):
        async for rec in cur.stream(
            "select x::int4 from generate_series(1, 2) x", binary=True
        ):
            recs.append(rec)
            assert cur.pgresult.fformat(0) == 1
            assert cur.pgresult.get_value(0, 0) == bytes([0, 0, 0, rec[0]])

        assert recs == [(1,), (2,)]


async def test_stream_binary_cursor_text_override(aconn):
    cur = aconn.cursor(binary=True)
    recs = []
    async for rec in cur.stream("select generate_series(1, 2)", binary=False):
        recs.append(rec)
        assert cur.pgresult.fformat(0) == 0
        assert cur.pgresult.get_value(0, 0) == str(rec[0]).encode()

    assert recs == [(1,), (2,)]


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


async def test_typeinfo(aconn):
    info = await TypeInfo.fetch(aconn, "jsonb")
    assert info is not None
