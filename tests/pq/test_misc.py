import pytest

from psycopg3 import pq


def test_error_message(pgconn):
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.FATAL_ERROR
    msg = pq.error_message(pgconn)
    assert msg == 'syntax error at or near "wat"'
    assert msg == pq.error_message(res)
    assert msg == res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(
        "ascii"
    )

    with pytest.raises(TypeError):
        pq.error_message(None)

    res.clear()
    assert pq.error_message(res) == "no details available"
    pgconn.finish()
    assert "NULL" in pq.error_message(pgconn)
