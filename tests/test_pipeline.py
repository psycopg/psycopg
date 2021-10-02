import pytest

import psycopg
from psycopg import pq

pytestmark = pytest.mark.libpq(">=14")


def test_pipeline_status(conn):
    assert not conn._pipeline_mode
    with conn.pipeline() as p:
        assert p.status() == pq.PipelineStatus.ON
        assert conn._pipeline_mode
        p.sync()
        r = conn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = conn.pgconn.get_result()
        assert r is None
    assert p.status() == pq.PipelineStatus.OFF
    assert not conn._pipeline_mode


def test_pipeline_busy(conn):
    with pytest.raises(
        psycopg.OperationalError, match="cannot exit pipeline mode while busy"
    ):
        with conn.cursor() as cur, conn.pipeline():
            cur.execute("select 1")


def test_pipeline(conn):
    pgconn = conn.pgconn
    with conn.pipeline() as pipeline:
        c1 = conn.cursor()
        c2 = conn.cursor()
        c1.execute("select 1")
        pipeline.sync()
        c2.execute("select 2")
        pipeline.sync()

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


def test_autocommit(conn):
    conn.autocommit = True
    pgconn = conn.pgconn
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select 1")
        pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


def test_pipeline_aborted(conn):
    conn.autocommit = True
    pgconn = conn.pgconn
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select 1")
        pipeline.sync()
        c.execute("select * from doesnotexist")
        c.execute("select 'aborted'")
        pipeline.sync()
        c.execute("select 2")
        pipeline.sync()

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.FATAL_ERROR
        assert pgconn.get_result() is None

        assert pipeline.status() == pq.PipelineStatus.ABORTED

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_ABORTED
        assert pgconn.get_result() is None

        assert pipeline.status() == pq.PipelineStatus.ABORTED

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        assert pipeline.status() == pq.PipelineStatus.ON

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"2"
        assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


def test_prepared(conn):
    conn.autocommit = True
    pgconn = conn.pgconn
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select %s::int", [10], prepare=True)
        c.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

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
def test_auto_prepare(conn):
    # Auto prepare does not work because cache maintainance requires access to
    # results at the moment.
    conn.autocommit = True
    pgconn = conn.pgconn
    with conn.pipeline() as pipeline:
        for i in range(10):
            conn.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        for i, v in zip(range(10), [0] * 5 + [1] * 5):
            r = pgconn.get_result()
            assert r.status == pq.ExecStatus.TUPLES_OK
            rv = int(r.get_value(0, 0).decode())
            assert rv == v
            assert pgconn.get_result() is None

        r = pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


def test_transaction(conn):
    pgconn = conn.pgconn
    with conn.pipeline() as pipeline:
        with conn.transaction():
            conn.execute("select 'tx'")
        pipeline.sync()

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
