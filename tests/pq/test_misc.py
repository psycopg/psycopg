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
    res.clear()
    assert pq.error_message(res) == "no error details available"
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


def test_result_set_value(pgconn):
    res = pgconn.make_empty_result(pq.ExecStatus.TUPLES_OK)
    res.set_attributes(
        [
            pq.PGresAttDesc(b"an_int", 0, 0, 0, 23, 4, -1),
            pq.PGresAttDesc(b"a_text", 0, 0, 0, 25, -1, -1),
        ]
    )
    assert res.ntuples == 0

    res.set_value(0, 0, b"42")
    res.set_value(0, 1, None)
    res.set_value(1, 0, b"")
    res.set_value(1, 1, b"a\x00b")
    assert res.ntuples == 2
    assert res.get_value(0, 0) == b"42"
    assert res.get_value(0, 1) is None
    assert res.get_value(1, 0) == b""
    assert res.get_value(1, 1) == b"a\x00b"

    res.set_value(0, 0, b"43")
    assert res.get_value(0, 0) == b"43"
    assert res.ntuples == 2

    for row, col in [(3, 0), (-1, 0), (0, 2), (0, -1)]:
        with pytest.raises(psycopg.OperationalError):
            res.set_value(row, col, b"x")
    assert res.ntuples == 2

    res.clear()
    with pytest.raises(psycopg.OperationalError):
        res.set_value(0, 0, b"x")


def test_result_copy(pgconn):
    res = pgconn.exec_(b"select 1 as an_int, null::text as a_text, 'x' as c")
    assert res.status == pq.ExecStatus.TUPLES_OK

    copy = res.copy(pq.CopyResultFlag.ATTRS | pq.CopyResultFlag.TUPLES)
    assert copy.status == pq.ExecStatus.TUPLES_OK
    assert copy.command_status == b"SELECT 1"
    assert copy.nfields == 3
    assert [copy.fname(i) for i in range(3)] == [b"an_int", b"a_text", b"c"]
    assert [copy.ftype(i) for i in range(3)] == [res.ftype(i) for i in range(3)]
    assert copy.ntuples == 1
    assert [copy.get_value(0, i) for i in range(3)] == [b"1", None, b"x"]

    copy.set_value(0, 0, b"2")
    copy.set_value(1, 2, b"y")
    assert copy.ntuples == 2
    assert res.get_value(0, 0) == b"1"
    assert res.ntuples == 1

    attrs_only = res.copy(pq.CopyResultFlag.ATTRS)
    assert attrs_only.nfields == 3
    assert attrs_only.ntuples == 0

    # TUPLES implies ATTRS
    tuples = res.copy(pq.CopyResultFlag.TUPLES)
    assert tuples.nfields == 3
    assert tuples.get_value(0, 2) == b"x"

    # a plain int works too
    same = res.copy(3)
    assert same.nfields == 3
    assert same.get_value(0, 0) == b"1"

    nothing = res.copy(0)
    assert nothing.nfields == 0
    assert nothing.ntuples == 0

    res.clear()
    assert copy.get_value(0, 2) == b"x"
    with pytest.raises(psycopg.OperationalError):
        res.copy(pq.CopyResultFlag.TUPLES)


@pytest.mark.parametrize(
    "intv, strv",
    [(91020, "9.10.20"), (100004, "10.4"), (101112, "10.11.12")],
)
def test_version_pretty(intv, strv):
    assert pq.version_pretty(intv) == strv
