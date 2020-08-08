import pytest

import psycopg3
from psycopg3 import pq


@pytest.mark.parametrize(
    "data, want",
    [
        (b"", b"''"),
        (b"hello", b"'hello'"),
        (b"foo'bar", b"'foo''bar'"),
        (b"foo\\bar", b" E'foo\\\\bar'"),
    ],
)
def test_escape_literal(pgconn, data, want):
    esc = pq.Escaping(pgconn)
    out = esc.escape_literal(data)
    assert out == want


def test_escape_literal_1char(pgconn):
    esc = pq.Escaping(pgconn)
    special = {b"'": b"''''", b"\\": b" E'\\\\'"}
    for c in range(1, 128):
        data = bytes([c])
        rv = esc.escape_literal(data)
        exp = special.get(data) or b"'%s'" % data
        assert rv == exp


def test_escape_literal_noconn(pgconn):
    esc = pq.Escaping()
    with pytest.raises(psycopg3.OperationalError):
        esc.escape_literal(b"hi")

    esc = pq.Escaping(pgconn)
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        esc.escape_literal(b"hi")


@pytest.mark.parametrize(
    "data, want",
    [
        (b"", b'""'),
        (b"hello", b'"hello"'),
        (b'foo"bar', b'"foo""bar"'),
        (b"foo\\bar", b'"foo\\bar"'),
    ],
)
def test_escape_identifier(pgconn, data, want):
    esc = pq.Escaping(pgconn)
    out = esc.escape_identifier(data)
    assert out == want


def test_escape_identifier_1char(pgconn):
    esc = pq.Escaping(pgconn)
    special = {b'"': b'""""', b"\\": b'"\\"'}
    for c in range(1, 128):
        data = bytes([c])
        rv = esc.escape_identifier(data)
        exp = special.get(data) or b'"%s"' % data
        assert rv == exp


def test_escape_identifier_noconn(pgconn):
    esc = pq.Escaping()
    with pytest.raises(psycopg3.OperationalError):
        esc.escape_identifier(b"hi")

    esc = pq.Escaping(pgconn)
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        esc.escape_identifier(b"hi")


@pytest.mark.parametrize(
    "data", [(b"hello\00world"), (b"\00\00\00\00")],
)
def test_escape_bytea(pgconn, data):
    exp = br"\x" + b"".join(b"%02x" % c for c in data)
    esc = pq.Escaping(pgconn)
    rv = esc.escape_bytea(data)
    assert rv == exp

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        esc.escape_bytea(data)


def test_escape_noconn(pgconn):
    data = bytes(range(256))
    esc = pq.Escaping()
    escdata = esc.escape_bytea(data)
    res = pgconn.exec_params(
        b"select '%s'::bytea" % escdata, [], result_format=1
    )
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == data


def test_escape_1char(pgconn):
    esc = pq.Escaping(pgconn)
    for c in range(256):
        rv = esc.escape_bytea(bytes([c]))
        exp = br"\x%02x" % c
        assert rv == exp


@pytest.mark.parametrize(
    "data", [(b"hello\00world"), (b"\00\00\00\00")],
)
def test_unescape_bytea(pgconn, data):
    enc = br"\x" + b"".join(b"%02x" % c for c in data)
    esc = pq.Escaping(pgconn)
    rv = esc.unescape_bytea(enc)
    assert rv == data

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        esc.unescape_bytea(data)
