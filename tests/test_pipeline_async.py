import asyncio

import pytest

import psycopg
from psycopg import pq, waiting

pytestmark = [
    pytest.mark.libpq(">= 14"),
    pytest.mark.asyncio,
]


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


async def test_pipeline_status(aconn):
    async with aconn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        await p.sync()
        r = aconn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = aconn.pgconn.get_result()
        assert r is None
    assert p.status == pq.PipelineStatus.OFF


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()
