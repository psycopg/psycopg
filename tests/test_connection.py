import pytest

import psycopg3
from psycopg3 import Connection


def test_connect(dsn):
    conn = Connection.connect(dsn)
    assert conn.status == conn.ConnStatus.OK


def test_connect_bad():
    with pytest.raises(psycopg3.OperationalError):
        Connection.connect("dbname=nosuchdb")


def test_close(conn):
    assert not conn.closed
    conn.close()
    assert conn.closed
    assert conn.status == conn.ConnStatus.BAD
    conn.close()
    assert conn.closed
    assert conn.status == conn.ConnStatus.BAD


def test_commit(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    res = conn.pgconn.exec_(b"insert into foo values (1)")
    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    conn.close()
    with pytest.raises(psycopg3.OperationalError):
        conn.commit()


def test_rollback(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    res = conn.pgconn.exec_(b"insert into foo values (1)")
    conn.rollback()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) is None

    conn.close()
    with pytest.raises(psycopg3.OperationalError):
        conn.rollback()


def test_get_encoding(conn):
    (enc,) = conn.cursor().execute("show client_encoding").fetchone()
    assert enc == conn.encoding


def test_set_encoding(conn):
    newenc = "LATIN1" if conn.encoding != "LATIN1" else "UTF8"
    assert conn.encoding != newenc
    conn.set_client_encoding(newenc)
    assert conn.encoding == newenc
    (enc,) = conn.cursor().execute("show client_encoding").fetchone()
    assert enc == newenc


def test_set_encoding_unsupported(conn):
    conn.set_client_encoding("EUC_TW")
    with pytest.raises(psycopg3.NotSupportedError):
        conn.cursor().execute("select 1")


def test_set_encoding_bad(conn):
    with pytest.raises(psycopg3.DatabaseError):
        conn.set_client_encoding("WAT")
