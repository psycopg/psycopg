import pytest

import psycopg
from psycopg import pq


@pytest.mark.libpq("< 14")
def test_old_libpq(pgconn):
    assert pgconn.pipeline_status == 0
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.enter_pipeline_mode()
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.exit_pipeline_mode()
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.pipeline_sync()
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.send_flush_request()


@pytest.mark.libpq(">= 14")
def test_work_in_progress(pgconn):
    assert not pgconn.nonblocking
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF
    pgconn.enter_pipeline_mode()
    pgconn.send_query_params(b"select $1", [b"1"])
    with pytest.raises(psycopg.OperationalError, match="cannot exit pipeline mode"):
        pgconn.exit_pipeline_mode()


@pytest.mark.libpq(">= 14")
def test_multi_pipelines(pgconn):
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF
    pgconn.enter_pipeline_mode()
    pgconn.send_query_params(b"select $1", [b"1"], param_types=[25])
    pgconn.pipeline_sync()
    pgconn.send_query_params(b"select $1", [b"2"], param_types=[25])
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

    pgconn.exit_pipeline_mode()

    assert pgconn.pipeline_status == pq.PipelineStatus.OFF

    assert result1.get_value(0, 0) == b"1"
    assert result2.get_value(0, 0) == b"2"


@pytest.mark.libpq(">= 14")
def test_flush_request(pgconn):
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF
    pgconn.enter_pipeline_mode()
    pgconn.send_query_params(b"select $1", [b"1"], param_types=[25])
    pgconn.send_flush_request()
    r = pgconn.get_result()
    assert r.status == pq.ExecStatus.TUPLES_OK
    assert r.get_value(0, 0) == b"1"
    pgconn.exit_pipeline_mode()


@pytest.fixture
def table(pgconn):
    tablename = "pipeline"
    pgconn.exec_(f"create table {tablename} (s text)".encode("ascii"))
    yield tablename
    pgconn.exec_(f"drop table if exists {tablename}".encode("ascii"))


@pytest.mark.libpq(">= 14")
def test_pipeline_abort(pgconn, table):
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF
    pgconn.enter_pipeline_mode()
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

    pgconn.exit_pipeline_mode()
