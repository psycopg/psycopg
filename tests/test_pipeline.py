import logging
from select import select

import pytest

import psycopg
from psycopg import pq, waiting
from psycopg.errors import UndefinedTable

pytestmark = pytest.mark.libpq(">= 14")


@pytest.fixture(autouse=True)
def debug_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="psycopg")


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


@pytest.mark.slow
def test_pipeline_demo(conn):
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
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        with conn.transaction():
            conn.execute("DROP TABLE IF EXISTS pq_pipeline_demo")
            conn.execute(
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            )
            for r in range(rows_to_send, 0, -1):
                conn.execute(
                    "INSERT INTO pq_pipeline_demo(itemno, int8filler)"
                    " VALUES (%s, %s)",
                    (r, 1 << 62),
                )
        pipeline.sync()


def test_pipeline_status(conn):
    with conn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        p.sync()

        # PQpipelineSync
        assert len(p) == 1

    assert p.status == pq.PipelineStatus.OFF


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_server_cursor(conn):
    cur = conn.cursor(name="pipeline")
    with conn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 1")


def test_pipeline_processed_at_exit(conn):
    with conn.cursor() as cur, conn.pipeline() as pipeline:
        cur.execute("select 1")
        pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery, PQpipelineSync
        assert len(pipeline) == 3

    assert len(pipeline) == 0


def test_pipeline(conn):
    with conn.pipeline() as pipeline:
        c1 = conn.cursor()
        c2 = conn.cursor()
        c1.execute("select 1")
        c2.execute("select 2")
        pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(2), PQpipelineSync
        assert len(pipeline) == 4

        (r1,) = c1.fetchone()
        assert r1 == 1
        assert len(pipeline) == 0

    (r2,) = c2.fetchone()
    assert r2 == 2


def test_autocommit(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select 1")
        pipeline.sync()

        # PQsendQuery, PQpipelineSync
        assert len(pipeline) == 2

        (r,) = c.fetchone()
        assert r == 1


def test_pipeline_aborted(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        c1 = conn.execute("select 1")
        pipeline.sync()
        c2 = conn.execute("select * from doesnotexist")
        c3 = conn.execute("select 'aborted'")
        # Here we can sometimes see that pipeline.status is ABORTED, but this
        # depends on whether results got fetched by previous execute() and
        # pipeline communication.
        pipeline.sync()
        c4 = conn.execute("select 2")
        pipeline.sync()

        (r,) = c1.fetchone()
        assert r == 1

        with pytest.raises(UndefinedTable):
            c2.fetchone()

        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            c3.fetchone()

        (r,) = c4.fetchone()
        assert r == 2


def test_prepared(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        c1 = conn.execute("select %s::int", [10], prepare=True)
        c2 = conn.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        (r,) = c1.fetchone()
        assert r == 10

        (r,) = c2.fetchone()
        assert r == 1


def test_auto_prepare(conn):
    conn.autocommit = True
    conn.prepared_threshold = 5
    with conn.pipeline():
        cursors = [
            conn.execute("select count(*) from pg_prepared_statements")
            for i in range(10)
        ]

        assert len(conn._prepared._names) == 1

    res = [c.fetchone()[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


def test_transaction(conn):
    with conn.pipeline():
        with conn.transaction():
            cur = conn.execute("select 'tx'")

        (r,) = cur.fetchone()
        assert r == "tx"

        with conn.transaction():
            cur = conn.execute("select 'rb'")
            raise psycopg.Rollback()

        (r,) = cur.fetchone()
        assert r == "rb"
