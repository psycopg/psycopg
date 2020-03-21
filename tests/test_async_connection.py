import pytest

import psycopg3
from psycopg3 import AsyncConnection


def test_connect(pq, dsn, loop):
    conn = loop.run_until_complete(AsyncConnection.connect(dsn))
    assert conn.pgconn.status == pq.ConnStatus.OK


def test_connect_bad(loop):
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(AsyncConnection.connect("dbname=nosuchdb"))


def test_commit(loop, pq, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == pq.TransactionStatus.INTRANS
    res = aconn.pgconn.exec_(b"insert into foo values (1)")
    loop.run_until_complete(aconn.commit())
    assert aconn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"


def test_rollback(loop, pq, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == pq.TransactionStatus.INTRANS
    res = aconn.pgconn.exec_(b"insert into foo values (1)")
    loop.run_until_complete(aconn.rollback())
    assert aconn.pgconn.transaction_status == pq.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) is None
