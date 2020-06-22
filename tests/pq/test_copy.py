import pytest

from psycopg3 import pq

sample_tabledef = "col1 int primary key, col2 int, data text"


def test_put_data_no_copy(pgconn):
    with pytest.raises(pq.PQerror):
        pgconn.put_copy_data(b"wat")

    pgconn.finish()
    with pytest.raises(pq.PQerror):
        pgconn.put_copy_data(b"wat")


def test_put_end_no_copy(pgconn):
    with pytest.raises(pq.PQerror):
        pgconn.put_copy_end()

    pgconn.finish()
    with pytest.raises(pq.PQerror):
        pgconn.put_copy_end()


def test_copy_out(pgconn):
    ensure_table(pgconn, sample_tabledef)
    res = pgconn.exec_(b"copy copy_in from stdin")
    assert res.status == pq.ExecStatus.COPY_IN

    for i in range(10):
        data = []
        for j in range(20):
            data.append(
                f"""\
{i * 20 + j}\t{j}\t{'X' * (i * 20 + j)}
"""
            )
        rv = pgconn.put_copy_data("".join(data).encode("ascii"))
        assert rv > 0

    rv = pgconn.put_copy_end()
    assert rv > 0

    res = pgconn.get_result()
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    res = pgconn.exec_(
        b"select min(col1), max(col1), count(*), max(length(data)) from copy_in"
    )
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.get_value(0, 0) == b"0"
    assert res.get_value(0, 1) == b"199"
    assert res.get_value(0, 2) == b"200"
    assert res.get_value(0, 3) == b"199"


def test_copy_out_err(pgconn):
    ensure_table(pgconn, sample_tabledef)
    res = pgconn.exec_(b"copy copy_in from stdin")
    assert res.status == pq.ExecStatus.COPY_IN

    for i in range(10):
        data = []
        for j in range(20):
            data.append(
                f"""\
{i * 20 + j}\thardly a number\tnope
"""
            )
        rv = pgconn.put_copy_data("".join(data).encode("ascii"))
        assert rv > 0

    rv = pgconn.put_copy_end()
    assert rv > 0

    res = pgconn.get_result()
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert b"hardly a number" in res.error_message

    res = pgconn.exec_(b"select count(*) from copy_in")
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.get_value(0, 0) == b"0"


def test_copy_out_error_end(pgconn):
    ensure_table(pgconn, sample_tabledef)
    res = pgconn.exec_(b"copy copy_in from stdin")
    assert res.status == pq.ExecStatus.COPY_IN

    for i in range(10):
        data = []
        for j in range(20):
            data.append(
                f"""\
{i * 20 + j}\t{j}\t{'X' * (i * 20 + j)}
"""
            )
        rv = pgconn.put_copy_data("".join(data).encode("ascii"))
        assert rv > 0

    rv = pgconn.put_copy_end(b"nuttengoggenio")
    assert rv > 0

    res = pgconn.get_result()
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert b"nuttengoggenio" in res.error_message

    res = pgconn.exec_(b"select count(*) from copy_in")
    assert res.status == pq.ExecStatus.TUPLES_OK, res.error_message
    assert res.get_value(0, 0) == b"0"


def ensure_table(pgconn, tabledef, name="copy_in"):
    pgconn.exec_(f"drop table if exists {name}".encode("ascii"))
    pgconn.exec_(f"create table {name} ({tabledef})".encode("ascii"))
