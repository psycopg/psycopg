import pytest

import psycopg3
from psycopg3 import Connection


def test_connect(pq, dsn):
    conn = Connection.connect(dsn)
    assert conn.pgconn.status == pq.ConnStatus.CONNECTION_OK


def test_connect_bad():
    with pytest.raises(psycopg3.OperationalError):
        Connection.connect("dbname=nosuchdb")


def test_commit(pq, conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert (
        conn.pgconn.transaction_status == pq.TransactionStatus.PQTRANS_INTRANS
    )
    res = conn.pgconn.exec_(b"insert into foo values (1)")
    conn.commit()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.PQTRANS_IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"


def test_rollback(pq, conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")
    conn.pgconn.exec_(b"begin")
    assert (
        conn.pgconn.transaction_status == pq.TransactionStatus.PQTRANS_INTRANS
    )
    res = conn.pgconn.exec_(b"insert into foo values (1)")
    conn.rollback()
    assert conn.pgconn.transaction_status == pq.TransactionStatus.PQTRANS_IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) is None
