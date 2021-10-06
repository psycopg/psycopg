from select import select

import pytest

import psycopg
from psycopg import pq, waiting

pytestmark = pytest.mark.libpq(">= 14")


@pytest.mark.slow
def test_pipeline_communicate(pgconn, demo_pipeline, generators):
    # This reproduces libpq_pipeline::pipelined_insert PostgreSQL test at
    # src/test/modules/libpq_pipeline/libpq_pipeline.c::test_pipelined_insert()
    #
    # This sends enough data as to fill the output buffer and the
    # pipeline_communicate() generator will consume input while we send more
    # output. Note that the pipeline is NOT synced before we process the
    # results.

    socket = pgconn.socket
    wait = waiting.wait

    with demo_pipeline:
        while demo_pipeline.queue:
            gen = generators.pipeline_communicate(pgconn)
            fetched = wait(gen, socket)
            demo_pipeline.process_results(fetched)
            rl, wl, xl = select([], [socket], [], 0.1)
            if wl:
                next(demo_pipeline, None)


def test_pipeline_status(conn):
    with conn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        p.sync()
        r = conn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = conn.pgconn.get_result()
        assert r is None
    assert p.status == pq.PipelineStatus.OFF


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()
