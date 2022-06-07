import pytest

import psycopg
from psycopg import pq


def test_error_message(pgconn):
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.FATAL_ERROR
    msg = pq.error_message(pgconn)
    assert "wat" in msg
    assert msg == pq.error_message(res)
    primary = res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY)
    assert primary.decode("ascii") in msg

    with pytest.raises(TypeError):
        pq.error_message(None)  # type: ignore[arg-type]

    res.clear()
    assert pq.error_message(res) == "no details available"
    pgconn.finish()
    assert "NULL" in pq.error_message(pgconn)


@pytest.mark.crdb_skip("encoding")
def test_error_message_encoding(pgconn):
    res = pgconn.exec_(b"set client_encoding to latin9")
    assert res.status == pq.ExecStatus.COMMAND_OK

    res = pgconn.exec_('select 1 from "foo\u20acbar"'.encode("latin9"))
    assert res.status == pq.ExecStatus.FATAL_ERROR

    msg = pq.error_message(pgconn)
    assert "foo\u20acbar" in msg

    msg = pq.error_message(res)
    assert "foo\ufffdbar" in msg

    msg = pq.error_message(res, encoding="latin9")
    assert "foo\u20acbar" in msg

    msg = pq.error_message(res, encoding="ascii")
    assert "foo\ufffdbar" in msg


def test_make_empty_result(pgconn):
    pgconn.exec_(b"wat")
    res = pgconn.make_empty_result(pq.ExecStatus.FATAL_ERROR)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert b"wat" in res.error_message

    pgconn.finish()
    res = pgconn.make_empty_result(pq.ExecStatus.FATAL_ERROR)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert res.error_message == b""


def test_result_set_attrs(pgconn):
    res = pgconn.make_empty_result(pq.ExecStatus.COPY_OUT)
    assert res.status == pq.ExecStatus.COPY_OUT

    attrs = [
        pq.PGresAttDesc(b"an_int", 0, 0, 0, 23, 0, 0),
        pq.PGresAttDesc(b"a_num", 0, 0, 0, 1700, 0, 0),
        pq.PGresAttDesc(b"a_bin_text", 0, 0, 1, 25, 0, 0),
    ]
    res.set_attributes(attrs)
    assert res.nfields == 3

    assert res.fname(0) == b"an_int"
    assert res.fname(1) == b"a_num"
    assert res.fname(2) == b"a_bin_text"

    assert res.fformat(0) == 0
    assert res.fformat(1) == 0
    assert res.fformat(2) == 1

    assert res.ftype(0) == 23
    assert res.ftype(1) == 1700
    assert res.ftype(2) == 25

    with pytest.raises(psycopg.OperationalError):
        res.set_attributes(attrs)
