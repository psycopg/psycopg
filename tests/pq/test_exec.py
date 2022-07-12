#!/usr/bin/env python3

import pytest

import psycopg
from psycopg import pq


def test_exec_none(pgconn):
    with pytest.raises(TypeError):
        pgconn.exec_(None)


def test_exec(pgconn):
    res = pgconn.exec_(b"select 'hel' || 'lo'")
    assert res.get_value(0, 0) == b"hello"
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.exec_(b"select 'hello'")


def test_exec_params(pgconn):
    res = pgconn.exec_params(b"select $1::int + $2", [b"5", b"3"])
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"8"
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.exec_params(b"select $1::int + $2", [b"5", b"3"])


def test_exec_params_empty(pgconn):
    res = pgconn.exec_params(b"select 8::int", [])
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"8"


def test_exec_params_types(pgconn):
    res = pgconn.exec_params(b"select $1, $2", [b"8", b"8"], [1700, 23])
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"8"
    assert res.ftype(0) == 1700
    assert res.get_value(0, 1) == b"8"
    assert res.ftype(1) == 23

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1, $2", [b"8", b"8"], [1700])


def test_exec_params_nulls(pgconn):
    res = pgconn.exec_params(b"select $1::text, $2::text, $3::text", [b"hi", b"", None])
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"hi"
    assert res.get_value(0, 1) == b""
    assert res.get_value(0, 2) is None


def test_exec_params_binary_in(pgconn):
    val = b"foo\00bar"
    res = pgconn.exec_params(
        b"select length($1::bytea), length($2::bytea)",
        [val, val],
        param_formats=[0, 1],
    )
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"3"
    assert res.get_value(0, 1) == b"7"

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1::bytea", [val], param_formats=[1, 1])


@pytest.mark.parametrize("fmt, out", [(0, b"\\x666f6f00626172"), (1, b"foo\00bar")])
def test_exec_params_binary_out(pgconn, fmt, out):
    val = b"foo\00bar"
    res = pgconn.exec_params(
        b"select $1::bytea", [val], param_formats=[1], result_format=fmt
    )
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == out


def test_prepare(pgconn):
    res = pgconn.prepare(b"prep", b"select $1::int + $2::int")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.exec_prepared(b"prep", [b"3", b"5"])
    assert res.get_value(0, 0) == b"8"

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.prepare(b"prep", b"select $1::int + $2::int")
    with pytest.raises(psycopg.OperationalError):
        pgconn.exec_prepared(b"prep", [b"3", b"5"])


def test_prepare_types(pgconn):
    res = pgconn.prepare(b"prep", b"select $1 + $2", [23, 23])
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.exec_prepared(b"prep", [b"3", b"5"])
    assert res.get_value(0, 0) == b"8"


def test_exec_prepared_binary_in(pgconn):
    val = b"foo\00bar"
    res = pgconn.prepare(b"", b"select length($1::bytea), length($2::bytea)")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.exec_prepared(b"", [val, val], param_formats=[0, 1])
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"3"
    assert res.get_value(0, 1) == b"7"

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1::bytea", [val], param_formats=[1, 1])


@pytest.mark.parametrize("fmt, out", [(0, b"\\x666f6f00626172"), (1, b"foo\00bar")])
def test_exec_prepared_binary_out(pgconn, fmt, out):
    val = b"foo\00bar"
    res = pgconn.prepare(b"", b"select $1::bytea")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.exec_prepared(b"", [val], param_formats=[1], result_format=fmt)
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == out


@pytest.mark.crdb_skip("server-side cursor")
def test_describe_portal(pgconn):
    res = pgconn.exec_(
        b"""
        begin;
        declare cur cursor for select * from generate_series(1,10) foo;
        """
    )
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.describe_portal(b"cur")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message
    assert res.nfields == 1
    assert res.fname(0) == b"foo"

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.describe_portal(b"cur")
