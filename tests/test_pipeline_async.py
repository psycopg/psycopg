import pytest

import psycopg
from psycopg import pq

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.libpq(">=14"),
]


async def test_pipeline_status(aconn):
    assert not aconn._pipeline_mode
    async with aconn.pipeline() as p:
        assert await p.status() == pq.PipelineStatus.ON
        assert aconn._pipeline_mode
        await p.sync()
        r = aconn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = aconn.pgconn.get_result()
        assert r is None
    assert await p.status() == pq.PipelineStatus.OFF
    assert not aconn._pipeline_mode


async def test_pipeline_busy(aconn):
    with pytest.raises(
        psycopg.OperationalError, match="cannot exit pipeline mode while busy"
    ):
        async with aconn.cursor() as cur, aconn.pipeline():
            await cur.execute("select 1")


async def test_pipeline(aconn):
    pgconn = aconn.pgconn
    async with aconn.pipeline() as pipeline:
        c1 = aconn.cursor()
        c2 = aconn.cursor()
        await c1.execute("select 1")
        await pipeline.sync()
        await c2.execute("select 2")
        await pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.COMMAND_OK  # BEGIN
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"2"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


async def test_autocommit(aconn):
    await aconn.set_autocommit(True)
    pgconn = aconn.pgconn
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select 1")
        await pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


async def test_pipeline_aborted(aconn):
    await aconn.set_autocommit(True)
    pgconn = aconn.pgconn
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select 1")
        await pipeline.sync()
        await c.execute("select * from doesnotexist")
        await c.execute("select 'aborted'")
        await pipeline.sync()
        await c.execute("select 2")
        await pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.FATAL_ERROR
        assert pgconn.get_result() is None

        assert await pipeline.status() == pq.PipelineStatus.ABORTED

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_ABORTED
        assert pgconn.get_result() is None

        assert await pipeline.status() == pq.PipelineStatus.ABORTED

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        assert await pipeline.status() == pq.PipelineStatus.ON

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"2"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    pgconn = aconn.pgconn
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select %s::int", [10], prepare=True)
        await c.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.COMMAND_OK  # PREPARE
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"10"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


@pytest.mark.xfail
async def test_auto_prepare(aconn):
    # Auto prepare does not work because cache maintainance requires access to
    # results at the moment.
    await aconn.set_autocommit(True)
    pgconn = aconn.pgconn
    async with aconn.pipeline() as pipeline:
        for i in range(10):
            await aconn.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

        for i, v in zip(range(10), [0] * 5 + [1] * 5):
            r = pgconn.get_result()
            assert r.status == pq.ExecStatus.TUPLES_OK
            rv = int(r.get_value(0, 0).decode())
            assert rv == v
            assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


async def test_transaction(aconn):
    pgconn = aconn.pgconn
    async with aconn.pipeline() as pipeline:
        async with aconn.transaction():
            await aconn.execute("select 'tx'")
        await pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.COMMAND_OK  # BEGIN
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"tx"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.COMMAND_OK  # COMMIT
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
