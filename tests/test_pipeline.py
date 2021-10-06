import logging

import pytest

import psycopg
from psycopg import pq
from psycopg.errors import UndefinedTable


@pytest.fixture(autouse=True)
def debug_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="psycopg")


pytestmark = pytest.mark.libpq(">=14")


def test_pipeline_status(conn):
    assert not conn._pipeline_mode
    with conn.pipeline() as p:
        assert p.status() == pq.PipelineStatus.ON
        assert conn._pipeline_mode
        p.sync()

        # PQpipelineSync
        assert len(p) == 1

    assert p.status() == pq.PipelineStatus.OFF
    assert not conn._pipeline_mode


def test_pipeline_busy(conn):
    with pytest.raises(
        psycopg.ProgrammingError, match="has unfetched results in the pipeline"
    ):
        with conn.cursor() as cur, conn.pipeline() as pipeline:
            cur.execute("select 1")
            pipeline.sync()

            # PQsendQuery[BEGIN], PQsendQuery, PQpipelineSync
            assert len(pipeline) == 3


def test_pipeline(conn):
    with conn.pipeline() as pipeline:
        c1 = conn.cursor()
        c2 = conn.cursor()
        c1.execute("select 1")
        pipeline.sync()
        c1.execute("select 11")
        c2.execute("select 2")
        pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(3), PQpipelineSync(2)
        assert len(pipeline) == 6

        (r1,) = c1.fetchone()
        assert r1 == 1
        assert len(pipeline) == 4  # -COMMAND_OK, -TUPLES_OK

        (r2,) = c2.fetchone()
        assert r2 == 2
        assert len(pipeline) == 1  # -PIPELINE_SYNC, -TUPLES_OK(2)

        (r11,) = c1.fetchone()
        assert r11 == 11
        # Same as before, since results have already been fetched.
        assert len(pipeline) == 1


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
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select 1")
        pipeline.sync()
        c.execute("select * from doesnotexist")
        c.execute("select 'aborted'")
        pipeline.sync()
        c.execute("select 2")
        pipeline.sync()

        # PQsendQuery(4), PQpipelineSync(3)
        assert len(pipeline) == 7

        (r,) = c.fetchone()
        assert r == 1
        assert len(pipeline) == 6

        with pytest.raises(UndefinedTable):
            c.fetchone()
        assert pipeline.status() == pq.PipelineStatus.ABORTED
        assert len(pipeline) == 4  # -PIPELINE_SYNC, -TUPLES_OK

        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            c.fetchone()
        assert pipeline.status() == pq.PipelineStatus.ABORTED
        assert len(pipeline) == 3  # -TUPLES_OK

        (r,) = c.fetchone()
        assert r == 2

        assert len(pipeline) == 1  # -PIPELINE_SYNC, -TUPLES_OK
        assert pipeline.status() == pq.PipelineStatus.ON


def test_prepared(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select %s::int", [10], prepare=True)
        c.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        # PQsendPrepare, PQsendQuery(2), PQpipelineSync
        assert len(pipeline) == 4

        (r,) = c.fetchone()
        assert r == 10
        assert len(pipeline) == 2  # -COMMAND_OK, -TUPLES_OK

        (r,) = c.fetchone()
        assert r == 1
        assert len(pipeline) == 1  # -TUPLES_OK


@pytest.mark.xfail
def test_auto_prepare(conn):
    # Auto prepare does not work because cache maintainance requires access to
    # results at the moment.
    conn.autocommit = True
    with conn.pipeline() as pipeline, conn.cursor() as cur:
        for i in range(10):
            cur.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        for v in [0] * 5 + [1] * 5:
            (r,) = cur.fetchone()
            assert r == v


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_transaction(conn):
    with conn.pipeline() as pipeline:
        with conn.transaction():
            cur = conn.execute("select 'tx'")
        pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery, PQsendQuery[COMMIT], PQpipelineSync
        assert len(pipeline) == 4

        (r,) = cur.fetchone()
        assert r == "tx"
        assert len(pipeline) == 2  # -COMMAND_OK, -TUPLES_OK
