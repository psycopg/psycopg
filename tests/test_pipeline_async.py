import asyncio
import logging

import pytest

import psycopg
from psycopg import pq
from psycopg.errors import UndefinedTable

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.libpq(">=14"),
]


@pytest.fixture(autouse=True)
def debug_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="psycopg")
    caplog.set_level(logging.DEBUG, logger="asyncio")


async def test_pipeline_status(aconn):
    assert not aconn.pgconn.pipeline_status
    async with aconn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        assert aconn.pgconn.pipeline_status
        await p.sync()

        # PQpipelineSync
        assert len(p) == 1

    assert p.status == pq.PipelineStatus.OFF
    assert not aconn.pgconn.pipeline_status


async def test_pipeline_processed_at_exit(aconn):
    async with aconn.cursor() as cur, aconn.pipeline() as pipeline:
        await cur.execute("select 1")
        await pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery, PQpipelineSync
        assert len(pipeline) == 3

    assert len(pipeline) == 0
    assert await cur.fetchone() == (1,)


async def test_pipeline(aconn):
    async with aconn.pipeline() as pipeline:
        c1 = await aconn.execute("select 1")
        await pipeline.sync()
        c2 = await aconn.execute("select 2")
        await pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(2), PQpipelineSync(2)
        assert len(pipeline) == 5

        (r1,) = await c1.fetchone()
        assert r1 == 1
        assert len(pipeline) == 3  # -COMMAND_OK, -TUPLES_OK

        (r2,) = await c2.fetchone()
        assert r2 == 2
        assert len(pipeline) == 1  # -PIPELINE_SYNC, -TUPLES_OK

        await c1.execute("select 11")
        await pipeline.sync()
        assert len(pipeline) == 3  # PQsendQuery, PQpipelineSync

        (r11,) = await c1.fetchone()
        assert r11 == 11
        assert len(pipeline) == 1  # -TUPLES_OK, -PIPELINE_SYNC


async def test_pipeline_execute_wait(aconn):
    cur = aconn.cursor()

    async def fetchone(pipeline):
        await asyncio.sleep(0.1)
        await pipeline.sync()
        return await cur.fetchone()

    async with aconn.pipeline() as pipeline:
        await cur.execute("select 1")
        t = asyncio.create_task(fetchone(pipeline))
        # This execute() blocks until cur.fetch*() is called.
        await cur.execute("select generate_series(1, 3)")
        await pipeline.sync()

        assert await cur.fetchall() == [(1,), (2,), (3,)]
        assert await t == (1,)


async def test_autocommit(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select 1")
        await pipeline.sync()

        # PQsendQuery, PQpipelineSync
        assert len(pipeline) == 2

        (r,) = await c.fetchone()
        assert r == 1


async def test_pipeline_aborted(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        c1 = await aconn.execute("select 1")
        await pipeline.sync()
        c2 = await aconn.execute("select * from doesnotexist")
        c3 = await aconn.execute("select 'aborted'")
        await pipeline.sync()
        c4 = await aconn.execute("select 2")
        await pipeline.sync()

        # PQsendQuery(4), PQpipelineSync(3)
        assert len(pipeline) == 7

        (r,) = await c1.fetchone()
        assert r == 1
        assert len(pipeline) == 6

        with pytest.raises(UndefinedTable):
            await c2.fetchone()
        assert pipeline.status == pq.PipelineStatus.ABORTED
        assert len(pipeline) == 4  # -PIPELINE_SYNC, -TUPLES_OK

        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            await c3.fetchone()
        assert pipeline.status == pq.PipelineStatus.ABORTED
        assert len(pipeline) == 3  # -TUPLES_OK

        (r,) = await c4.fetchone()
        assert r == 2

        assert len(pipeline) == 1  # -PIPELINE_SYNC, -TUPLES_OK
        assert pipeline.status == pq.PipelineStatus.ON


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        c1 = await aconn.execute("select %s::int", [10], prepare=True)
        c2 = await aconn.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

        # PQsendPrepare, PQsendQuery(2), PQpipelineSync
        assert len(pipeline) == 4

        (r,) = await c1.fetchone()
        assert r == 10
        assert len(pipeline) == 2  # -COMMAND_OK, -TUPLES_OK

        (r,) = await c2.fetchone()
        assert r == 1
        assert len(pipeline) == 1  # -TUPLES_OK


@pytest.mark.xfail
async def test_auto_prepare(aconn):
    # Auto prepare does not work because cache maintainance requires access to
    # results at the moment.
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        cursors = []
        for i in range(10):
            cursors.append(
                await aconn.execute(
                    "select count(*) from pg_prepared_statements"
                )
            )
        await pipeline.sync()

        for cur, v in zip(cursors, [0] * 5 + [1] * 5):
            (r,) = await cur.fetchone()
            assert r == v


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()


async def test_server_cursor(aconn):
    cur = aconn.cursor(name="pipeline")
    async with aconn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            await cur.execute("select 1")


async def test_transaction(aconn):
    async with aconn.pipeline() as pipeline:
        async with aconn.transaction():
            cur = await aconn.execute("select 'tx'")
        await pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery, PQsendQuery[COMMIT], PQpipelineSync
        assert len(pipeline) == 4

        (r,) = await cur.fetchone()
        assert r == "tx"
        assert len(pipeline) == 2  # -COMMAND_OK, -TUPLES_OK
