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

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

    assert p.status() == pq.PipelineStatus.OFF
    assert not conn._pipeline_mode


def test_pipeline_busy(conn):
    with pytest.raises(
        psycopg.OperationalError, match="cannot exit pipeline mode while busy"
    ):
        with conn.cursor() as cur, conn.pipeline():
            cur.execute("select 1")


def test_pipeline(conn):
    with conn.pipeline() as pipeline:
        c1 = conn.cursor()
        c2 = conn.cursor()
        c1.execute("select 1")
        pipeline.sync()
        c2.execute("select 2")
        pipeline.sync()

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.COMMAND_OK  # BEGIN

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"2"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


def test_autocommit(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select 1")
        pipeline.sync()

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


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

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.FATAL_ERROR

        assert pipeline.status() == pq.PipelineStatus.ABORTED

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_ABORTED

        assert pipeline.status() == pq.PipelineStatus.ABORTED

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC

        (r,) = conn.wait(conn._fetch_many_gen())
        assert pipeline.status() == pq.PipelineStatus.ON
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"2"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


def test_prepared(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select %s::int", [10], prepare=True)
        c.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.COMMAND_OK  # PREPARE

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"10"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"1"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


@pytest.mark.xfail
def test_auto_prepare(conn):
    # Auto prepare does not work because cache maintainance requires access to
    # results at the moment.
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        for i in range(10):
            conn.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        for v in [0] * 5 + [1] * 5:
            (r,) = conn.wait(conn._fetch_many_gen())
            assert r.status == pq.ExecStatus.TUPLES_OK
            rv = int(r.get_value(0, 0).decode())
            assert rv == v

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC


def test_transaction(conn):
    with conn.pipeline() as pipeline:
        with conn.transaction():
            conn.execute("select 'tx'")
        pipeline.sync()

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.COMMAND_OK  # BEGIN

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.TUPLES_OK
        assert r.get_value(0, 0) == b"tx"

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.COMMAND_OK  # COMMIT

        (r,) = conn.wait(conn._fetch_many_gen())
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
