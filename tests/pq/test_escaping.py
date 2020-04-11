import pytest

import psycopg3


@pytest.mark.parametrize(
    "data", [(b"hello\00world"), (b"\00\00\00\00")],
)
def test_escape_bytea(pq, pgconn, data):
    exp = br"\x" + b"".join(b"%02x" % c for c in data)
    esc = pq.Escaping(pgconn)
    rv = esc.escape_bytea(data)
    assert rv == exp

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        esc.escape_bytea(data)


def test_escape_noconn(pq, pgconn):
    data = bytes(range(256))
    esc = pq.Escaping()
    escdata = esc.escape_bytea(data)
    res = pgconn.exec_params(
        b"select '%s'::bytea" % escdata, [], result_format=1
    )
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == data


def test_escape_1char(pq, pgconn):
    esc = pq.Escaping(pgconn)
    for c in range(256):
        rv = esc.escape_bytea(bytes([c]))
        exp = br"\x%02x" % c
        assert rv == exp


@pytest.mark.parametrize(
    "data", [(b"hello\00world"), (b"\00\00\00\00")],
)
def test_unescape_bytea(pq, pgconn, data):
    enc = br"\x" + b"".join(b"%02x" % c for c in data)
    esc = pq.Escaping(pgconn)
    rv = esc.unescape_bytea(enc)
    assert rv == data

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        esc.unescape_bytea(data)
