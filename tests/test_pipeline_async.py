import asyncio
import logging

import pytest

import psycopg
from psycopg import pq, waiting
from psycopg.errors import UndefinedTable

pytestmark = [
    pytest.mark.libpq(">= 14"),
    pytest.mark.asyncio,
]


@pytest.fixture(autouse=True)
def debug_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="psycopg")
    caplog.set_level(logging.DEBUG, logger="asyncio")


@pytest.mark.slow
async def test_pipeline_communicate_async(pgconn, demo_pipeline, generators):

    socket = pgconn.socket
    wait = waiting.wait_async
    loop = asyncio.get_event_loop()

    with demo_pipeline:
        while demo_pipeline.queue:
            gen = generators.pipeline_communicate(pgconn)
            fetched = await wait(gen, socket)
            demo_pipeline.process_results(fetched)
            fut = loop.create_future()
            loop.add_writer(socket, fut.set_result, True)
            fut.add_done_callback(lambda f: loop.remove_writer(socket))
            try:
                await asyncio.wait_for(fut, timeout=0.1)
            except asyncio.TimeoutError:
                continue
            else:
                next(demo_pipeline, None)


@pytest.mark.slow
async def test_pipeline_demo(aconn):
    # This reproduces libpq_pipeline::pipelined_insert PostgreSQL test at
    # src/test/modules/libpq_pipeline/libpq_pipeline.c::test_pipelined_insert()
    # using plain psycopg API.
    #
    # We do not fetch results explicitly (using cursor.fetch*()), this is
    # handled by execute() calls when pgconn socket is read-ready, which
    # happens when the output buffer is full.
    #
    # Run with --log-file=<path> to see what happens.
    rows_to_send = 10_000
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        async with aconn.transaction():
            await aconn.execute("DROP TABLE IF EXISTS pq_pipeline_demo")
            await aconn.execute(
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            )
            for r in range(rows_to_send, 0, -1):
                await aconn.execute(
                    "INSERT INTO pq_pipeline_demo(itemno, int8filler)"
                    " VALUES (%s, %s)",
                    (r, 1 << 62),
                )
        await pipeline.sync()


async def test_pipeline_status(aconn):
    async with aconn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        await p.sync()

        # PQpipelineSync
        assert len(p) == 1

    assert p.status == pq.PipelineStatus.OFF


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()


async def test_server_cursor(aconn):
    cur = aconn.cursor(name="pipeline")
    async with aconn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            await cur.execute("select 1")


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
        c1 = aconn.cursor()
        c2 = aconn.cursor()
        await c1.execute("select 1")
        await c2.execute("select 2")
        await pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(2), PQpipelineSync
        assert len(pipeline) == 4

        (r1,) = await c1.fetchone()
        assert r1 == 1
        assert len(pipeline) == 0

    (r2,) = await c2.fetchone()
    assert r2 == 2


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
        # Here we can sometimes see that pipeline.status is ABORTED, but this
        # depends on whether results got fetched by previous execute() and
        # pipeline communication.
        await pipeline.sync()
        c4 = await aconn.execute("select 2")
        await pipeline.sync()

        (r,) = await c1.fetchone()
        assert r == 1

        with pytest.raises(UndefinedTable):
            await c2.fetchone()

        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            await c3.fetchone()

        (r,) = await c4.fetchone()
        assert r == 2


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        c1 = await aconn.execute("select %s::int", [10], prepare=True)
        c2 = await aconn.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

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

        assert len(aconn._prepared._prepared) == 1

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
