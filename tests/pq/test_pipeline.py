import pytest

import psycopg
from psycopg import pq


@pytest.mark.libpq("< 14")
def test_old_libpq(pgconn):
    assert pgconn.pipeline_status is False
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.pipeline_status = True
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.pipeline_status = False
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.pipeline_sync()


@pytest.mark.libpq(">= 14")
def test_pipeline_status(pgconn):
    assert not pgconn.pipeline_status
    pgconn.pipeline_status = False
    pgconn.pipeline_status = True

    pgconn.pipeline_status = pq.PipelineStatus.OFF
    assert not pgconn.pipeline_status

    with pytest.raises(ValueError):
        pgconn.pipeline_status = pq.PipelineStatus.ABORTED

    pgconn.pipeline_status = pq.PipelineStatus.ON
    assert pgconn.pipeline_status

    with pytest.raises(
        psycopg.ProgrammingError,
        match="cannot enter pipeline mode, pipeline is ON",
    ):
        pgconn.pipeline_status = True

    pgconn.send_query(b"select * from doesnotexist")
    pgconn.get_result()
    assert pgconn.pipeline_status == pq.PipelineStatus.ABORTED
    with pytest.raises(
        psycopg.ProgrammingError,
        match="cannot enter pipeline mode, pipeline is ABORTED",
    ):
        pgconn.pipeline_status = True
    pgconn.pipeline_status = False
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF


@pytest.mark.libpq(">= 14")
def test_work_in_progress(pgconn):
    assert not pgconn.nonblocking
    assert not pgconn.pipeline_status
    pgconn.pipeline_status = True
    pgconn.send_query_params(b"select $1", [b"1"])
    with pytest.raises(
        psycopg.OperationalError, match="cannot exit pipeline mode"
    ):
        pgconn.pipeline_status = False


@pytest.mark.libpq(">= 14")
def test_multi_pipelines(pgconn):
    assert not pgconn.pipeline_status
    pgconn.pipeline_status = True
    pgconn.send_query_params(b"select $1", [b"1"])
    pgconn.pipeline_sync()
    pgconn.send_query_params(b"select $1", [b"2"])
    pgconn.pipeline_sync()

    # result from first query
    result1 = pgconn.get_result()
    assert result1 is not None
    assert result1.status == pq.ExecStatus.TUPLES_OK

    # NULL signals end of result
    assert pgconn.get_result() is None

    # first sync result
    sync_result = pgconn.get_result()
    assert sync_result is not None
    assert sync_result.status == pq.ExecStatus.PIPELINE_SYNC

    # result from second query
    result2 = pgconn.get_result()
    assert result2 is not None
    assert result2.status == pq.ExecStatus.TUPLES_OK

    # NULL signals end of result
    assert pgconn.get_result() is None

    # second sync result
    sync_result = pgconn.get_result()
    assert sync_result is not None
    assert sync_result.status == pq.ExecStatus.PIPELINE_SYNC

    # pipeline still ON
    assert pgconn.pipeline_status == pq.PipelineStatus.ON

    pgconn.pipeline_status = False
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF

    assert result1.get_value(0, 0) == b"1"
    assert result2.get_value(0, 0) == b"2"


@pytest.fixture
def table(pgconn):
    tablename = "pipeline"
    pgconn.exec_(f"create table {tablename} (s text)".encode("ascii"))
    yield tablename
    pgconn.exec_(f"drop table if exists {tablename}".encode("ascii"))


@pytest.mark.libpq(">= 14")
def test_pipeline_abort(pgconn, table):
    assert not pgconn.pipeline_status
    pgconn.pipeline_status = True
    pgconn.send_query_params(b"insert into pipeline values ($1)", [b"1"])
    pgconn.send_query_params(b"select no_such_function($1)", [b"1"])
    pgconn.send_query_params(b"insert into pipeline values ($1)", [b"2"])
    pgconn.pipeline_sync()
    pgconn.send_query_params(b"insert into pipeline values ($1)", [b"3"])
    pgconn.pipeline_sync()

    # result from first INSERT
    r = pgconn.get_result()
    assert r is not None
    assert r.status == pq.ExecStatus.COMMAND_OK

    # NULL signals end of result
    assert pgconn.get_result() is None

    # error result from second query (SELECT)
    r = pgconn.get_result()
    assert r is not None
    assert r.status == pq.ExecStatus.FATAL_ERROR

    # NULL signals end of result
    assert pgconn.get_result() is None

    # pipeline should be aborted, due to previous error
    assert pgconn.pipeline_status == pq.PipelineStatus.ABORTED

    # result from second INSERT, aborted due to previous error
    r = pgconn.get_result()
    assert r is not None
    assert r.status == pq.ExecStatus.PIPELINE_ABORTED

    # NULL signals end of result
    assert pgconn.get_result() is None

    # pipeline is still aborted
    assert pgconn.pipeline_status == pq.PipelineStatus.ABORTED

    # sync result
    r = pgconn.get_result()
    assert r is not None
    assert r.status == pq.ExecStatus.PIPELINE_SYNC

    # aborted flag is clear, pipeline is on again
    assert pgconn.pipeline_status == pq.PipelineStatus.ON

    # result from the third INSERT
    r = pgconn.get_result()
    assert r is not None
    assert r.status == pq.ExecStatus.COMMAND_OK

    # NULL signals end of result
    assert pgconn.get_result() is None

    # second sync result
    r = pgconn.get_result()
    assert r is not None
    assert r.status == pq.ExecStatus.PIPELINE_SYNC

    # NULL signals end of result
    assert pgconn.get_result() is None

    pgconn.pipeline_status = False
