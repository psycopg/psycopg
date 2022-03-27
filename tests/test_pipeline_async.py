import asyncio
import logging
from typing import Any

import pytest

import psycopg
from psycopg import pq
from psycopg import errors as e

pytestmark = [
    pytest.mark.libpq(">= 14"),
    pytest.mark.asyncio,
]


async def test_repr(aconn):
    async with aconn.pipeline() as p:
        assert "psycopg.AsyncPipeline" in repr(p)
        assert "[IDLE]" in repr(p)

    await aconn.close()
    assert "[BAD]" in repr(p)


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


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()


async def test_server_cursor(aconn):
    async with aconn.cursor(name="pipeline") as cur, aconn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            await cur.execute("select 1")


async def test_cannot_insert_multiple_commands(aconn):
    with pytest.raises(psycopg.errors.SyntaxError) as cm:
        async with aconn.pipeline():
            await aconn.execute("select 1; select 2")
    assert cm.value.sqlstate == "42601"


async def test_pipeline_processed_at_exit(aconn):
    async with aconn.cursor() as cur:
        async with aconn.pipeline() as p:
            await cur.execute("select 1")

            # PQsendQuery[BEGIN], PQsendQuery
            assert len(p.result_queue) == 2

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

        # PQsendQuery[BEGIN], PQsendQuery(2)
        assert len(p.result_queue) == 3

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
        with pytest.raises(e.OperationalError, match="pipeline aborted"):
            await (await aconn.execute("select 'aborted'")).fetchone()
        # Sync restore the connection in usable state.
        p.sync()
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


async def test_executemany(aconn):
    await aconn.set_autocommit(True)
    await aconn.execute("drop table if exists execmanypipeline")
    await aconn.execute(
        "create unlogged table execmanypipeline ("
        " id serial primary key, num integer)"
    )
    async with aconn.pipeline(), aconn.cursor() as cur:
        await cur.executemany(
            "insert into execmanypipeline(num) values (%s) returning id",
            [(10,), (20,)],
        )
        assert (await cur.fetchone()) == (1,)
        assert cur.nextset()
        assert (await cur.fetchone()) == (2,)
        assert cur.nextset() is None


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline():
        c1 = await aconn.execute("select %s::int", [10], prepare=True)
        c2 = await aconn.execute("select count(*) from pg_prepared_statements")

        (r,) = await c1.fetchone()
        assert r == 10

        (r,) = await c2.fetchone()
        assert r == 1


async def test_auto_prepare(aconn):
    aconn.prepared_threshold = 5
    async with aconn.pipeline():
        cursors = [
            await aconn.execute("select count(*) from pg_prepared_statements")
            for i in range(10)
        ]

        assert len(aconn._prepared._names) == 1

    res = [(await c.fetchone())[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


async def test_transaction(aconn):
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


async def test_outer_transaction(aconn):
    async with aconn.transaction():
        async with aconn.pipeline():
            await aconn.execute("drop table if exists outertx")
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


async def test_concurrency(aconn):
    async with aconn.transaction():
        await aconn.execute("drop table if exists pipeline_concurrency")
        await aconn.execute(
            "create unlogged table pipeline_concurrency ("
            " id serial primary key,"
            " value integer"
            ")"
        )
        await aconn.execute("drop table if exists accessed")
        await aconn.execute("create unlogged table accessed as (select now() as value)")

    async def update(value):
        cur = await aconn.execute(
            "insert into pipeline_concurrency(value) values (%s) returning id",
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
