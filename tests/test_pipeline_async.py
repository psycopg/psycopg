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


async def test_pipeline_busy(aconn):
    with pytest.raises(
        psycopg.ProgrammingError, match="has unfetched results in the pipeline"
    ):
        async with aconn.cursor() as cur, aconn.pipeline() as pipeline:
            await cur.execute("select 1")
            await pipeline.sync()

            # PQsendQuery[BEGIN], PQsendQuery, PQpipelineSync
            assert len(pipeline) == 3


async def test_pipeline(aconn):
    async with aconn.pipeline() as pipeline:
        c1 = aconn.cursor()
        c2 = aconn.cursor()
        await c1.execute("select 1")
        await pipeline.sync()
        await c1.execute("select 11")
        await c2.execute("select 2")
        await pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(3), PQpipelineSync(2)
        assert len(pipeline) == 6

        (r1,) = await c1.fetchone()
        assert r1 == 1
        assert len(pipeline) == 4  # -COMMAND_OK, -TUPLES_OK

        (r2,) = await c2.fetchone()
        assert r2 == 2
        assert len(pipeline) == 1  # -PIPELINE_SYNC, -TUPLES_OK(2)

        (r11,) = await c1.fetchone()
        assert r11 == 11
        # Same as before, since results have already been fetched.
        assert len(pipeline) == 1


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
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select 1")
        await pipeline.sync()
        await c.execute("select * from doesnotexist")
        await c.execute("select 'aborted'")
        await pipeline.sync()
        await c.execute("select 2")
        await pipeline.sync()

        # PQsendQuery(4), PQpipelineSync(3)
        assert len(pipeline) == 7

        (r,) = await c.fetchone()
        assert r == 1
        assert len(pipeline) == 6

        with pytest.raises(UndefinedTable):
            await c.fetchone()
        assert pipeline.status == pq.PipelineStatus.ABORTED
        assert len(pipeline) == 4  # -PIPELINE_SYNC, -TUPLES_OK

        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            await c.fetchone()
        assert pipeline.status == pq.PipelineStatus.ABORTED
        assert len(pipeline) == 3  # -TUPLES_OK

        (r,) = await c.fetchone()
        assert r == 2

        assert len(pipeline) == 1  # -PIPELINE_SYNC, -TUPLES_OK
        assert pipeline.status == pq.PipelineStatus.ON


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select %s::int", [10], prepare=True)
        await c.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

        # PQsendPrepare, PQsendQuery(2), PQpipelineSync
        assert len(pipeline) == 4

        (r,) = await c.fetchone()
        assert r == 10
        assert len(pipeline) == 2  # -COMMAND_OK, -TUPLES_OK

        (r,) = await c.fetchone()
        assert r == 1
        assert len(pipeline) == 1  # -TUPLES_OK


@pytest.mark.xfail
async def test_auto_prepare(aconn):
    # Auto prepare does not work because cache maintainance requires access to
    # results at the moment.
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline, aconn.cursor() as cur:
        for i in range(10):
            await cur.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

        for i, v in zip(range(10), [0] * 5 + [1] * 5):
            (r,) = await cur.fetchone()
            assert r == v


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()


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
