from collections import deque
from functools import partial
from typing import List

import pytest

import psycopg
from psycopg import waiting
from psycopg import pq
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def test_connect_operationalerror_pgconn(generators, dsn, monkeypatch):
    """Check that when generators.connect() fails, the resulting
    OperationalError has a pgconn attribute set with needs_password.
    """
    gen = generators.connect(dsn)
    pgconn = waiting.wait_conn(gen)
    if not pgconn.used_password:
        pytest.skip("test connection needs no password")

    with monkeypatch.context() as m:
        try:
            m.delenv("PGPASSWORD", raising=True)
        except KeyError:
            info = conninfo_to_dict(dsn)
            del info["password"]  # should not raise per check above.
            dsn = make_conninfo(**info)

        gen = generators.connect(dsn)
        with pytest.raises(
            psycopg.OperationalError, match="connection failed:"
        ) as excinfo:
            waiting.wait_conn(gen)

    pgconn = excinfo.value.pgconn
    assert pgconn is not None
    assert pgconn.needs_password
    assert b"fe_sendauth: no password supplied" in pgconn.error_message
    assert pgconn.status == pq.ConnStatus.BAD.value
    assert pgconn.transaction_status == pq.TransactionStatus.UNKNOWN.value
    assert pgconn.pipeline_status == pq.PipelineStatus.OFF.value
    with pytest.raises(psycopg.OperationalError, match="connection is closed"):
        pgconn.exec_(b"select 1")


@pytest.fixture
def pipeline(pgconn):
    nb, pgconn.nonblocking = pgconn.nonblocking, True
    assert pgconn.nonblocking
    pgconn.enter_pipeline_mode()
    yield
    if pgconn.pipeline_status:
        pgconn.exit_pipeline_mode()
    pgconn.nonblocking = nb


def _run_pipeline_communicate(pgconn, generators, commands, expected_statuses):
    actual_statuses: List[pq.ExecStatus] = []
    while len(actual_statuses) != len(expected_statuses):
        if commands:
            gen = generators.pipeline_communicate(pgconn, commands)
            results = waiting.wait(gen, pgconn.socket)
            for (result,) in results:
                actual_statuses.append(result.status)
        else:
            gen = generators.fetch_many(pgconn)
            results = waiting.wait(gen, pgconn.socket)
            for result in results:
                actual_statuses.append(result.status)

    assert actual_statuses == expected_statuses


@pytest.mark.pipeline
def test_pipeline_communicate_multi_pipeline(pgconn, pipeline, generators):
    commands = deque(
        [
            partial(pgconn.send_query_params, b"select 1", None),
            pgconn.pipeline_sync,
            partial(pgconn.send_query_params, b"select 2", None),
            pgconn.pipeline_sync,
        ]
    )
    expected_statuses = [
        pq.ExecStatus.TUPLES_OK,
        pq.ExecStatus.PIPELINE_SYNC,
        pq.ExecStatus.TUPLES_OK,
        pq.ExecStatus.PIPELINE_SYNC,
    ]
    _run_pipeline_communicate(pgconn, generators, commands, expected_statuses)


@pytest.mark.pipeline
def test_pipeline_communicate_no_sync(pgconn, pipeline, generators):
    numqueries = 10
    commands = deque(
        [partial(pgconn.send_query_params, b"select repeat('xyzxz', 12)", None)]
        * numqueries
        + [pgconn.send_flush_request]
    )
    expected_statuses = [pq.ExecStatus.TUPLES_OK] * numqueries
    _run_pipeline_communicate(pgconn, generators, commands, expected_statuses)


@pytest.fixture
def pipeline_demo(pgconn):
    assert pgconn.pipeline_status == 0
    res = pgconn.exec_(b"DROP TABLE IF EXISTS pg_pipeline")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    res = pgconn.exec_(
        b"CREATE UNLOGGED TABLE pg_pipeline(" b" id serial primary key, itemno integer)"
    )
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    yield "pg_pipeline"
    res = pgconn.exec_(b"DROP TABLE IF EXISTS pg_pipeline")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message


# TODOCRDB: 1 doesn't get rolled back. Open a ticket?
@pytest.mark.pipeline
@pytest.mark.crdb("skip", reason="pipeline aborted")
def test_pipeline_communicate_abort(pgconn, pipeline_demo, pipeline, generators):
    insert_sql = b"insert into pg_pipeline(itemno) values ($1)"
    commands = deque(
        [
            partial(pgconn.send_query_params, insert_sql, [b"1"]),
            partial(pgconn.send_query_params, b"select no_such_function(1)", None),
            partial(pgconn.send_query_params, insert_sql, [b"2"]),
            pgconn.pipeline_sync,
            partial(pgconn.send_query_params, insert_sql, [b"3"]),
            pgconn.pipeline_sync,
        ]
    )
    expected_statuses = [
        pq.ExecStatus.COMMAND_OK,
        pq.ExecStatus.FATAL_ERROR,
        pq.ExecStatus.PIPELINE_ABORTED,
        pq.ExecStatus.PIPELINE_SYNC,
        pq.ExecStatus.COMMAND_OK,
        pq.ExecStatus.PIPELINE_SYNC,
    ]
    _run_pipeline_communicate(pgconn, generators, commands, expected_statuses)
    pgconn.exit_pipeline_mode()
    res = pgconn.exec_(b"select itemno from pg_pipeline order by itemno")
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"3"


@pytest.fixture
def pipeline_uniqviol(pgconn):
    if not psycopg.Pipeline.is_supported():
        pytest.skip(psycopg.Pipeline._not_supported_reason())
    assert pgconn.pipeline_status == 0
    res = pgconn.exec_(b"DROP TABLE IF EXISTS pg_pipeline_uniqviol")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    res = pgconn.exec_(
        b"CREATE UNLOGGED TABLE pg_pipeline_uniqviol("
        b" id bigint primary key, idata bigint)"
    )
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    res = pgconn.exec_(b"BEGIN")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    res = pgconn.prepare(
        b"insertion",
        b"insert into pg_pipeline_uniqviol values ($1, $2) returning id",
    )
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    return "pg_pipeline_uniqviol"


def test_pipeline_communicate_uniqviol(pgconn, pipeline_uniqviol, pipeline, generators):
    commands = deque(
        [
            partial(pgconn.send_query_prepared, b"insertion", [b"1", b"2"]),
            partial(pgconn.send_query_prepared, b"insertion", [b"2", b"2"]),
            partial(pgconn.send_query_prepared, b"insertion", [b"1", b"2"]),
            partial(pgconn.send_query_prepared, b"insertion", [b"3", b"2"]),
            partial(pgconn.send_query_prepared, b"insertion", [b"4", b"2"]),
            partial(pgconn.send_query_params, b"commit", None),
        ]
    )
    expected_statuses = [
        pq.ExecStatus.TUPLES_OK,
        pq.ExecStatus.TUPLES_OK,
        pq.ExecStatus.FATAL_ERROR,
        pq.ExecStatus.PIPELINE_ABORTED,
        pq.ExecStatus.PIPELINE_ABORTED,
        pq.ExecStatus.PIPELINE_ABORTED,
    ]
    _run_pipeline_communicate(pgconn, generators, commands, expected_statuses)
