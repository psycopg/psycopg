import ctypes
import pytest

from psycopg3 import pq


@pytest.mark.parametrize(
    "command, status",
    [
        (b"", "EMPTY_QUERY"),
        (b"select 1", "TUPLES_OK"),
        (b"set timezone to utc", "COMMAND_OK"),
        (b"wat", "FATAL_ERROR"),
    ],
)
def test_status(pgconn, command, status):
    res = pgconn.exec_(command)
    assert res.status == getattr(pq.ExecStatus, status)
    assert status in repr(res)


def test_clear(pgconn):
    res = pgconn.exec_(b"select 1")
    assert res.status == pq.ExecStatus.TUPLES_OK
    res.clear()
    assert res.status == pq.ExecStatus.FATAL_ERROR
    res.clear()
    assert res.status == pq.ExecStatus.FATAL_ERROR


def test_pgresult_ptr(pgconn, libpq):
    res = pgconn.exec_(b"select 1")
    assert res.pgresult_ptr is not None

    f = libpq.PQcmdStatus
    f.argtypes = [ctypes.c_void_p]
    f.restype = ctypes.c_char_p
    assert f(res.pgresult_ptr) == b"SELECT 1"

    res.clear()
    assert res.pgresult_ptr is None


def test_error_message(pgconn):
    res = pgconn.exec_(b"select 1")
    assert res.error_message == b""
    res = pgconn.exec_(b"select wat")
    assert b"wat" in res.error_message
    res.clear()
    assert res.error_message == b""


def test_error_field(pgconn):
    res = pgconn.exec_(b"select wat")
    assert res.error_field(pq.DiagnosticField.SEVERITY) == b"ERROR"
    assert res.error_field(pq.DiagnosticField.SQLSTATE) == b"42703"
    assert b"wat" in res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY)
    res.clear()
    assert res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) is None


@pytest.mark.parametrize("n", range(4))
def test_ntuples(pgconn, n):
    res = pgconn.exec_params(
        b"select generate_series(1, $1)", [str(n).encode("ascii")]
    )
    assert res.ntuples == n
    res.clear()
    assert res.ntuples == 0


def test_nfields(pgconn):
    res = pgconn.exec_(b"select wat")
    assert res.nfields == 0
    res = pgconn.exec_(b"select 1, 2, 3")
    assert res.nfields == 3
    res.clear()
    assert res.nfields == 0


def test_fname(pgconn):
    res = pgconn.exec_(b'select 1 as foo, 2 as "BAR"')
    assert res.fname(0) == b"foo"
    assert res.fname(1) == b"BAR"
    assert res.fname(2) is None
    assert res.fname(-1) is None
    res.clear()
    assert res.fname(0) is None


def test_ftable_and_col(pgconn):
    res = pgconn.exec_(
        b"""
        drop table if exists t1, t2;
        create table t1 as select 1 as f1;
        create table t2 as select 2 as f2, 3 as f3;
        """
    )
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.exec_(
        b"select f1, f3, 't1'::regclass::oid, 't2'::regclass::oid from t1, t2"
    )
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message

    assert res.ftable(0) == int(res.get_value(0, 2).decode("ascii"))
    assert res.ftable(1) == int(res.get_value(0, 3).decode("ascii"))
    assert res.ftablecol(0) == 1
    assert res.ftablecol(1) == 2
    res.clear()
    assert res.ftable(0) == 0
    assert res.ftablecol(0) == 0


@pytest.mark.parametrize("fmt", (0, 1))
def test_fformat(pgconn, fmt):
    res = pgconn.exec_params(b"select 1", [], result_format=fmt)
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.fformat(0) == fmt
    assert res.binary_tuples == fmt
    res.clear()
    assert res.fformat(0) == 0
    assert res.binary_tuples == 0


def test_ftype(pgconn):
    res = pgconn.exec_(b"select 1::int, 1::numeric, 1::text")
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.ftype(0) == 23
    assert res.ftype(1) == 1700
    assert res.ftype(2) == 25
    res.clear()
    assert res.ftype(0) == 0


def test_fmod(pgconn):
    res = pgconn.exec_(b"select 1::int, 1::numeric(10), 1::numeric(10,2)")
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.fmod(0) == -1
    assert res.fmod(1) == 0xA0004
    assert res.fmod(2) == 0xA0006
    res.clear()
    assert res.fmod(0) == 0


def test_fsize(pgconn):
    res = pgconn.exec_(b"select 1::int, 1::bigint, 1::text")
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.fsize(0) == 4
    assert res.fsize(1) == 8
    assert res.fsize(2) == -1
    res.clear()
    assert res.fsize(0) == 0


def test_get_value(pgconn):
    res = pgconn.exec_(b"select 'a', '', NULL")
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.get_value(0, 0) == b"a"
    assert res.get_value(0, 1) == b""
    assert res.get_value(0, 2) is None
    res.clear()
    assert res.get_value(0, 0) is None


def test_nparams_types(pgconn):
    res = pgconn.prepare(b"", b"select $1::int, $2::text")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.describe_prepared(b"")
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    assert res.nparams == 2
    assert res.param_type(0) == 23
    assert res.param_type(1) == 25

    res.clear()
    assert res.nparams == 0
    assert res.param_type(0) == 0


def test_command_status(pgconn):
    res = pgconn.exec_(b"select 1")
    assert res.command_status == b"SELECT 1"
    res = pgconn.exec_(b"set timezone to utf8")
    assert res.command_status == b"SET"
    res.clear()
    assert res.command_status is None


def test_command_tuples(pgconn):
    res = pgconn.exec_(b"set timezone to utf8")
    assert res.command_tuples is None
    res = pgconn.exec_(b"select * from generate_series(1, 10)")
    assert res.command_tuples == 10
    res.clear()
    assert res.command_tuples is None


def test_oid_value(pgconn):
    res = pgconn.exec_(b"select 1")
    assert res.oid_value == 0
    res.clear()
    assert res.oid_value == 0
