from select import select

import pytest

import psycopg
from psycopg import pq
from psycopg.generators import execute


def execute_wait(pgconn):
    return psycopg.waiting.wait(execute(pgconn), pgconn.socket)


def test_send_query(pgconn):
    # This test shows how to process an async query in all its glory
    pgconn.nonblocking = 1

    # Long query to make sure we have to wait on send
    pgconn.send_query(
        b"/* %s */ select 'x' as f from pg_sleep(0.01); select 1 as foo;"
        % (b"x" * 1_000_000)
    )

    # send loop
    waited_on_send = 0
    while True:
        f = pgconn.flush()
        if f == 0:
            break

        waited_on_send += 1

        rl, wl, xl = select([pgconn.socket], [pgconn.socket], [])
        assert not (rl and wl)
        if wl:
            continue  # call flush again()
        if rl:
            pgconn.consume_input()
            continue

    # TODO: this check is not reliable, it fails on travis sometimes
    # assert waited_on_send

    # read loop
    results = []
    while True:
        pgconn.consume_input()
        if pgconn.is_busy():
            select([pgconn.socket], [], [])
            continue
        res = pgconn.get_result()
        if res is None:
            break
        assert res.status == pq.ExecStatus.TUPLES_OK
        results.append(res)

    assert len(results) == 2
    assert results[0].nfields == 1
    assert results[0].fname(0) == b"f"
    assert results[0].get_value(0, 0) == b"x"
    assert results[1].nfields == 1
    assert results[1].fname(0) == b"foo"
    assert results[1].get_value(0, 0) == b"1"


def test_send_query_compact_test(pgconn):
    # Like the above test but use psycopg facilities for compactness
    pgconn.send_query(
        b"/* %s */ select 'x' as f from pg_sleep(0.01); select 1 as foo;"
        % (b"x" * 1_000_000)
    )
    results = execute_wait(pgconn)

    assert len(results) == 2
    assert results[0].nfields == 1
    assert results[0].fname(0) == b"f"
    assert results[0].get_value(0, 0) == b"x"
    assert results[1].nfields == 1
    assert results[1].fname(0) == b"foo"
    assert results[1].get_value(0, 0) == b"1"

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.send_query(b"select 1")


def test_single_row_mode(pgconn):
    pgconn.send_query(b"select generate_series(1,2)")
    pgconn.set_single_row_mode()

    results = execute_wait(pgconn)
    assert len(results) == 3

    res = results[0]
    assert res.status == pq.ExecStatus.SINGLE_TUPLE
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"1"

    res = results[1]
    assert res.status == pq.ExecStatus.SINGLE_TUPLE
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"2"

    res = results[2]
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.ntuples == 0


def test_send_query_params(pgconn):
    pgconn.send_query_params(b"select $1::int + $2", [b"5", b"3"])
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"8"

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.send_query_params(b"select $1", [b"1"])


def test_send_prepare(pgconn):
    pgconn.send_prepare(b"prep", b"select $1::int + $2::int")
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"prep", [b"3", b"5"])
    (res,) = execute_wait(pgconn)
    assert res.get_value(0, 0) == b"8"

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.send_prepare(b"prep", b"select $1::int + $2::int")
    with pytest.raises(psycopg.OperationalError):
        pgconn.send_query_prepared(b"prep", [b"3", b"5"])


def test_send_prepare_types(pgconn):
    pgconn.send_prepare(b"prep", b"select $1 + $2", [23, 23])
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"prep", [b"3", b"5"])
    (res,) = execute_wait(pgconn)
    assert res.get_value(0, 0) == b"8"


def test_send_prepared_binary_in(pgconn):
    val = b"foo\00bar"
    pgconn.send_prepare(b"", b"select length($1::bytea), length($2::bytea)")
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"", [val, val], param_formats=[0, 1])
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"3"
    assert res.get_value(0, 1) == b"7"

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1::bytea", [val], param_formats=[1, 1])


@pytest.mark.parametrize("fmt, out", [(0, b"\\x666f6f00626172"), (1, b"foo\00bar")])
def test_send_prepared_binary_out(pgconn, fmt, out):
    val = b"foo\00bar"
    pgconn.send_prepare(b"", b"select $1::bytea")
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"", [val], param_formats=[1], result_format=fmt)
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == out


def test_send_describe_prepared(pgconn):
    pgconn.send_prepare(b"prep", b"select $1::int8 + $2::int8 as fld")
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_describe_prepared(b"prep")
    (res,) = execute_wait(pgconn)
    assert res.nfields == 1
    assert res.ntuples == 0
    assert res.fname(0) == b"fld"
    assert res.ftype(0) == 20

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.send_describe_prepared(b"prep")


@pytest.mark.crdb_skip("server-side cursor")
def test_send_describe_portal(pgconn):
    res = pgconn.exec_(
        b"""
        begin;
        declare cur cursor for select * from generate_series(1,10) foo;
        """
    )
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_describe_portal(b"cur")
    (res,) = execute_wait(pgconn)
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    assert res.nfields == 1
    assert res.fname(0) == b"foo"

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.send_describe_portal(b"cur")
