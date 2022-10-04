import asyncio
import logging
from typing import Any
from operator import attrgetter
from itertools import groupby

import pytest

import psycopg
from psycopg import pq
from psycopg import errors as e

from .test_pipeline import pipeline_aborted

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.pipeline,
    pytest.mark.skipif("not psycopg.AsyncPipeline.is_supported()"),
]


async def test_repr(aconn):
    async with aconn.pipeline() as p:
        assert "psycopg.AsyncPipeline" in repr(p)
        assert "[IDLE, pipeline=ON]" in repr(p)

    await aconn.close()
    assert "[BAD]" in repr(p)


async def test_connection_closed(aconn):
    await aconn.close()
    with pytest.raises(e.OperationalError):
        async with aconn.pipeline():
            pass


async def test_pipeline_status(aconn: psycopg.AsyncConnection[Any]) -> None:
    assert aconn._pipeline is None
    async with aconn.pipeline() as p:
        assert aconn._pipeline is p
        assert p.status == pq.PipelineStatus.ON
    assert p.status == pq.PipelineStatus.OFF
    assert not aconn._pipeline


async def test_pipeline_reenter(aconn: psycopg.AsyncConnection[Any]) -> None:
    async with aconn.pipeline() as p1:
        async with aconn.pipeline() as p2:
            assert p2 is p1
            assert p1.status == pq.PipelineStatus.ON
        assert p2 is p1
        assert p2.status == pq.PipelineStatus.ON
    assert aconn._pipeline is None
    assert p1.status == pq.PipelineStatus.OFF


async def test_pipeline_broken_conn_exit(aconn: psycopg.AsyncConnection[Any]) -> None:
    with pytest.raises(e.OperationalError):
        async with aconn.pipeline():
            await aconn.execute("select 1")
            await aconn.close()
            closed = True

    assert closed


async def test_pipeline_exit_error_noclobber(aconn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    with pytest.raises(ZeroDivisionError):
        async with aconn.pipeline():
            await aconn.close()
            1 / 0

    assert len(caplog.records) == 1


async def test_pipeline_exit_error_noclobber_nested(aconn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")
    with pytest.raises(ZeroDivisionError):
        async with aconn.pipeline():
            async with aconn.pipeline():
                await aconn.close()
                1 / 0

    assert len(caplog.records) == 2


async def test_pipeline_exit_sync_trace(aconn, trace):
    t = trace.trace(aconn)
    async with aconn.pipeline():
        pass
    await aconn.close()
    assert len([i for i in t if i.type == "Sync"]) == 1


async def test_pipeline_nested_sync_trace(aconn, trace):
    t = trace.trace(aconn)
    async with aconn.pipeline():
        async with aconn.pipeline():
            pass
    await aconn.close()
    assert len([i for i in t if i.type == "Sync"]) == 2


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()


async def test_server_cursor(aconn):
    async with aconn.cursor(name="pipeline") as cur, aconn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            await cur.execute("select 1")


async def test_cannot_insert_multiple_commands(aconn):
    with pytest.raises((e.SyntaxError, e.InvalidPreparedStatementDefinition)):
        async with aconn.pipeline():
            await aconn.execute("select 1; select 2")


async def test_copy(aconn):
    async with aconn.pipeline():
        cur = aconn.cursor()
        with pytest.raises(e.NotSupportedError):
            async with cur.copy("copy (select 1) to stdout") as copy:
                await copy.read()


async def test_pipeline_processed_at_exit(aconn):
    async with aconn.cursor() as cur:
        async with aconn.pipeline() as p:
            await cur.execute("select 1")

            assert len(p.result_queue) == 1

        assert await cur.fetchone() == (1,)


async def test_pipeline_errors_processed_at_exit(aconn):
    await aconn.set_autocommit(True)
    with pytest.raises(e.UndefinedTable):
        async with aconn.pipeline():
            await aconn.execute("select * from nosuchtable")
            await aconn.execute("create table voila ()")
    cur = await aconn.execute(
        "select count(*) from pg_tables where tablename = %s", ("voila",)
    )
    (count,) = await cur.fetchone()
    assert count == 0


async def test_pipeline(aconn):
    async with aconn.pipeline() as p:
        c1 = aconn.cursor()
        c2 = aconn.cursor()
        await c1.execute("select 1")
        await c2.execute("select 2")

        assert len(p.result_queue) == 2

        (r1,) = await c1.fetchone()
        assert r1 == 1

    (r2,) = await c2.fetchone()
    assert r2 == 2


async def test_autocommit(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline(), aconn.cursor() as c:
        await c.execute("select 1")

        (r,) = await c.fetchone()
        assert r == 1


async def test_pipeline_aborted(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as p:
        c1 = await aconn.execute("select 1")
        with pytest.raises(e.UndefinedTable):
            await (await aconn.execute("select * from doesnotexist")).fetchone()
        with pytest.raises(e.PipelineAborted):
            await (await aconn.execute("select 'aborted'")).fetchone()
        # Sync restore the connection in usable state.
        await p.sync()
        c2 = await aconn.execute("select 2")

    (r,) = await c1.fetchone()
    assert r == 1

    (r,) = await c2.fetchone()
    assert r == 2


async def test_pipeline_commit_aborted(aconn):
    with pytest.raises((e.UndefinedColumn, e.OperationalError)):
        async with aconn.pipeline():
            await aconn.execute("select error")
            await aconn.execute("create table voila ()")
            await aconn.commit()


async def test_sync_syncs_results(aconn):
    async with aconn.pipeline() as p:
        cur = await aconn.execute("select 1")
        assert cur.statusmessage is None
        await p.sync()
        assert cur.statusmessage == "SELECT 1"


async def test_sync_syncs_errors(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as p:
        await aconn.execute("select 1 from nosuchtable")
        with pytest.raises(e.UndefinedTable):
            await p.sync()


@pipeline_aborted
async def test_errors_raised_on_commit(aconn):
    async with aconn.pipeline():
        await aconn.execute("select 1 from nosuchtable")
        with pytest.raises(e.UndefinedTable):
            await aconn.commit()
        await aconn.rollback()
        cur1 = await aconn.execute("select 1")
    cur2 = await aconn.execute("select 2")

    assert await cur1.fetchone() == (1,)
    assert await cur2.fetchone() == (2,)


async def test_errors_raised_on_transaction_exit(aconn):
    here = False
    async with aconn.pipeline():
        with pytest.raises(e.UndefinedTable):
            async with aconn.transaction():
                await aconn.execute("select 1 from nosuchtable")
                here = True
        cur1 = await aconn.execute("select 1")
    assert here
    cur2 = await aconn.execute("select 2")

    assert await cur1.fetchone() == (1,)
    assert await cur2.fetchone() == (2,)


async def test_errors_raised_on_nested_transaction_exit(aconn):
    here = False
    async with aconn.pipeline():
        async with aconn.transaction():
            with pytest.raises(e.UndefinedTable):
                async with aconn.transaction():
                    await aconn.execute("select 1 from nosuchtable")
                    here = True
            cur1 = await aconn.execute("select 1")
    assert here
    cur2 = await aconn.execute("select 2")

    assert await cur1.fetchone() == (1,)
    assert await cur2.fetchone() == (2,)


async def test_implicit_transaction(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline():
        assert aconn.pgconn.transaction_status == pq.TransactionStatus.IDLE
        await aconn.execute("select 'before'")
        # Transaction is ACTIVE because previous command is not completed
        # since we have not fetched its results.
        assert aconn.pgconn.transaction_status == pq.TransactionStatus.ACTIVE
        # Upon entering the nested pipeline through "with transaction():", a
        # sync() is emitted to restore the transaction state to IDLE, as
        # expected to emit a BEGIN.
        async with aconn.transaction():
            await aconn.execute("select 'tx'")
        cur = await aconn.execute("select 'after'")
    assert await cur.fetchone() == ("after",)


@pytest.mark.crdb_skip("deferrable")
async def test_error_on_commit(aconn):
    await aconn.execute(
        """
        drop table if exists selfref;
        create table selfref (
            x serial primary key,
            y int references selfref (x) deferrable initially deferred)
        """
    )
    await aconn.commit()

    async with aconn.pipeline():
        await aconn.execute("insert into selfref (y) values (-1)")
        with pytest.raises(e.ForeignKeyViolation):
            await aconn.commit()
        cur1 = await aconn.execute("select 1")
    cur2 = await aconn.execute("select 2")

    assert (await cur1.fetchone()) == (1,)
    assert (await cur2.fetchone()) == (2,)


async def test_fetch_no_result(aconn):
    async with aconn.pipeline():
        cur = aconn.cursor()
        with pytest.raises(e.ProgrammingError):
            await cur.fetchone()


async def test_executemany(aconn):
    await aconn.set_autocommit(True)
    await aconn.execute("drop table if exists execmanypipeline")
    await aconn.execute(
        "create unlogged table execmanypipeline ("
        " id serial primary key, num integer)"
    )
    async with aconn.pipeline(), aconn.cursor() as cur:
        await cur.executemany(
            "insert into execmanypipeline(num) values (%s) returning num",
            [(10,), (20,)],
            returning=True,
        )
        assert cur.rowcount == 2
        assert (await cur.fetchone()) == (10,)
        assert cur.nextset()
        assert (await cur.fetchone()) == (20,)
        assert cur.nextset() is None


async def test_executemany_no_returning(aconn):
    await aconn.set_autocommit(True)
    await aconn.execute("drop table if exists execmanypipelinenoreturning")
    await aconn.execute(
        "create unlogged table execmanypipelinenoreturning ("
        " id serial primary key, num integer)"
    )
    async with aconn.pipeline(), aconn.cursor() as cur:
        await cur.executemany(
            "insert into execmanypipelinenoreturning(num) values (%s)",
            [(10,), (20,)],
            returning=False,
        )
        with pytest.raises(e.ProgrammingError, match="no result available"):
            await cur.fetchone()
        assert cur.nextset() is None
        with pytest.raises(e.ProgrammingError, match="no result available"):
            await cur.fetchone()
        assert cur.nextset() is None


@pytest.mark.crdb("skip", reason="temp tables")
async def test_executemany_trace(aconn, trace):
    await aconn.set_autocommit(True)
    cur = aconn.cursor()
    await cur.execute("create temp table trace (id int)")
    t = trace.trace(aconn)
    async with aconn.pipeline():
        await cur.executemany("insert into trace (id) values (%s)", [(10,), (20,)])
        await cur.executemany("insert into trace (id) values (%s)", [(10,), (20,)])
    await aconn.close()
    items = list(t)
    assert items[-1].type == "Terminate"
    del items[-1]
    roundtrips = [k for k, g in groupby(items, key=attrgetter("direction"))]
    assert roundtrips == ["F", "B"]
    assert len([i for i in items if i.type == "Sync"]) == 1


@pytest.mark.crdb("skip", reason="temp tables")
async def test_executemany_trace_returning(aconn, trace):
    await aconn.set_autocommit(True)
    cur = aconn.cursor()
    await cur.execute("create temp table trace (id int)")
    t = trace.trace(aconn)
    async with aconn.pipeline():
        await cur.executemany(
            "insert into trace (id) values (%s)", [(10,), (20,)], returning=True
        )
        await cur.executemany(
            "insert into trace (id) values (%s)", [(10,), (20,)], returning=True
        )
    await aconn.close()
    items = list(t)
    assert items[-1].type == "Terminate"
    del items[-1]
    roundtrips = [k for k, g in groupby(items, key=attrgetter("direction"))]
    assert roundtrips == ["F", "B"] * 3
    assert items[-2].direction == "F"  # last 2 items are F B
    assert len([i for i in items if i.type == "Sync"]) == 1


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline():
        c1 = await aconn.execute("select %s::int", [10], prepare=True)
        c2 = await aconn.execute(
            "select count(*) from pg_prepared_statements where name != ''"
        )

        (r,) = await c1.fetchone()
        assert r == 10

        (r,) = await c2.fetchone()
        assert r == 1


async def test_auto_prepare(aconn):
    aconn.prepared_threshold = 5
    async with aconn.pipeline():
        cursors = [
            await aconn.execute(
                "select count(*) from pg_prepared_statements where name != ''"
            )
            for i in range(10)
        ]

        assert len(aconn._prepared._names) == 1

    res = [(await c.fetchone())[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


async def test_transaction(aconn):
    notices = []
    aconn.add_notice_handler(lambda diag: notices.append(diag.message_primary))

    async with aconn.pipeline():
        async with aconn.transaction():
            cur = await aconn.execute("select 'tx'")

        (r,) = await cur.fetchone()
        assert r == "tx"

        async with aconn.transaction():
            cur = await aconn.execute("select 'rb'")
            raise psycopg.Rollback()

        (r,) = await cur.fetchone()
        assert r == "rb"

    assert not notices


async def test_transaction_nested(aconn):
    async with aconn.pipeline():
        async with aconn.transaction():
            outer = await aconn.execute("select 'outer'")
            with pytest.raises(ZeroDivisionError):
                async with aconn.transaction():
                    inner = await aconn.execute("select 'inner'")
                    1 / 0

        (r,) = await outer.fetchone()
        assert r == "outer"
        (r,) = await inner.fetchone()
        assert r == "inner"


async def test_transaction_nested_no_statement(aconn):
    async with aconn.pipeline():
        async with aconn.transaction():
            async with aconn.transaction():
                cur = await aconn.execute("select 1")

        (r,) = await cur.fetchone()
        assert r == 1


async def test_outer_transaction(aconn):
    async with aconn.transaction():
        await aconn.execute("drop table if exists outertx")
    async with aconn.transaction():
        async with aconn.pipeline():
            await aconn.execute("create table outertx as (select 1)")
            cur = await aconn.execute("select * from outertx")
    (r,) = await cur.fetchone()
    assert r == 1
    cur = await aconn.execute(
        "select count(*) from pg_tables where tablename = 'outertx'"
    )
    assert (await cur.fetchone())[0] == 1


async def test_outer_transaction_error(aconn):
    async with aconn.transaction():
        with pytest.raises((e.UndefinedColumn, e.OperationalError)):
            async with aconn.pipeline():
                await aconn.execute("select error")
                await aconn.execute("create table voila ()")


async def test_rollback_explicit(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline():
        with pytest.raises(e.DivisionByZero):
            cur = await aconn.execute("select 1 / %s", [0])
            await cur.fetchone()
        await aconn.rollback()
        await aconn.execute("select 1")


async def test_rollback_transaction(aconn):
    await aconn.set_autocommit(True)
    with pytest.raises(e.DivisionByZero):
        async with aconn.pipeline():
            async with aconn.transaction():
                cur = await aconn.execute("select 1 / %s", [0])
                await cur.fetchone()
    await aconn.execute("select 1")


async def test_message_0x33(aconn):
    # https://github.com/psycopg/psycopg/issues/314
    notices = []
    aconn.add_notice_handler(lambda diag: notices.append(diag.message_primary))

    await aconn.set_autocommit(True)
    async with aconn.pipeline():
        cur = await aconn.execute("select 'test'")
        assert (await cur.fetchone()) == ("test",)

    assert not notices


async def test_transaction_state_implicit_begin(aconn, trace):
    # Regression test to ensure that the transaction state is correct after
    # the implicit BEGIN statement (in non-autocommit mode).
    notices = []
    aconn.add_notice_handler(lambda diag: notices.append(diag.message_primary))
    t = trace.trace(aconn)
    async with aconn.pipeline():
        await (await aconn.execute("select 'x'")).fetchone()
        await aconn.execute("select 'y'")
    assert not notices
    assert [
        e.content[0] for e in t if e.type == "Parse" and b"BEGIN" in e.content[0]
    ] == [b' "" "BEGIN" 0']


async def test_concurrency(aconn):
    async with aconn.transaction():
        await aconn.execute("drop table if exists pipeline_concurrency")
        await aconn.execute("drop table if exists accessed")
    async with aconn.transaction():
        await aconn.execute(
            "create unlogged table pipeline_concurrency ("
            " id serial primary key,"
            " value integer"
            ")"
        )
        await aconn.execute("create unlogged table accessed as (select now() as value)")

    async def update(value):
        cur = await aconn.execute(
            "insert into pipeline_concurrency(value) values (%s) returning value",
            (value,),
        )
        await aconn.execute("update accessed set value = now()")
        return cur

    await aconn.set_autocommit(True)

    (before,) = await (await aconn.execute("select value from accessed")).fetchone()

    values = range(1, 10)
    async with aconn.pipeline():
        cursors = await asyncio.wait_for(
            asyncio.gather(*[update(value) for value in values]),
            timeout=len(values),
        )

    assert sum([(await cur.fetchone())[0] for cur in cursors]) == sum(values)

    (s,) = await (
        await aconn.execute("select sum(value) from pipeline_concurrency")
    ).fetchone()
    assert s == sum(values)
    (after,) = await (await aconn.execute("select value from accessed")).fetchone()
    assert after > before
