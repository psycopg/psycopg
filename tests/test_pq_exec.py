#!/usr/bin/env python3

import pytest


def test_exec_none(pq, pgconn):
    with pytest.raises(TypeError):
        pgconn.exec_(None)


def test_exec_empty(pq, pgconn):
    res = pgconn.exec_(b"")
    assert res.status == pq.ExecStatus.PGRES_EMPTY_QUERY


def test_exec_command(pq, pgconn):
    res = pgconn.exec_(b"set timezone to utc")
    assert res.status == pq.ExecStatus.PGRES_COMMAND_OK


def test_exec_error(pq, pgconn):
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.PGRES_FATAL_ERROR


def test_exec_params(pq, pgconn):
    res = pgconn.exec_params(b"select $1::int + $2", [b"5", b"3"])
    assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
    assert res.nfields == 1
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"8"


def test_exec_params_empty(pq, pgconn):
    res = pgconn.exec_params(b"select 8::int", [])
    assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
    assert res.nfields == 1
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"8"


def test_exec_params_types(pq, pgconn):
    res = pgconn.exec_params(b"select $1, $2", [b"8", b"8"], [1700, 23])
    assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
    assert res.nfields == 2
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"8"
    assert res.ftype(0) == 1700
    assert res.get_value(0, 1) == b"8"
    assert res.ftype(1) == 23

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1, $2", [b"8", b"8"], [1700])


def test_exec_params_nulls(pq, pgconn):
    res = pgconn.exec_params(b"select $1, $2, $3", [b"hi", b"", None])
    assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
    assert res.nfields == 3
    assert res.ntuples == 1
    assert res.get_value(0, 0) == b"hi"
    assert res.get_value(0, 1) == b""
    assert res.get_value(0, 2) is None


def test_exec_params_binary_in(pq, pgconn):
    val = b"foo\00bar"
    res = pgconn.exec_params(
        b"select length($1::bytea), length($2::bytea)",
        [val, val],
        param_formats=[0, 1],
    )
    assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
    assert res.get_value(0, 0) == b"3"
    assert res.get_value(0, 1) == b"7"

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1::bytea", [val], param_formats=[1, 1])


@pytest.mark.parametrize(
    "fmt, out", [(0, b"\\x666f6f00626172"), (1, b"foo\00bar")]
)
def test_exec_params_binary_out(pq, pgconn, fmt, out):
    val = b"foo\00bar"
    res = pgconn.exec_params(
        b"select $1::bytea", [val], param_formats=[1], result_format=fmt
    )
    assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
    assert res.get_value(0, 0) == out
