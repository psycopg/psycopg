import pytest

import psycopg3
from psycopg3 import AsyncConnection


def test_connect(dsn, loop):
    conn = loop.run_until_complete(AsyncConnection.connect(dsn))
    assert conn.status == conn.ConnStatus.OK


def test_connect_bad(loop):
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(AsyncConnection.connect("dbname=nosuchdb"))


def test_close(aconn):
    assert not aconn.closed
    aconn.close()
    assert aconn.closed
    assert aconn.status == aconn.ConnStatus.BAD
    aconn.close()
    assert aconn.closed
    assert aconn.status == aconn.ConnStatus.BAD


def test_commit(loop, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    res = aconn.pgconn.exec_(b"insert into foo values (1)")
    loop.run_until_complete(aconn.commit())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    aconn.close()
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(aconn.commit())


def test_rollback(loop, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    res = aconn.pgconn.exec_(b"insert into foo values (1)")
    loop.run_until_complete(aconn.rollback())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) is None

    aconn.close()
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(aconn.rollback())


def test_get_encoding(aconn, loop):
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("show client_encoding"))
    (enc,) = loop.run_until_complete(cur.fetchone())
    assert enc == aconn.encoding


def test_set_encoding(aconn, loop):
    newenc = "LATIN1" if aconn.encoding != "LATIN1" else "UTF8"
    assert aconn.encoding != newenc
    loop.run_until_complete(aconn.set_client_encoding(newenc))
    assert aconn.encoding == newenc
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("show client_encoding"))
    (enc,) = loop.run_until_complete(cur.fetchone())
    assert enc == newenc


def test_set_encoding_bad(aconn, loop):
    with pytest.raises(psycopg3.DatabaseError):
        loop.run_until_complete(aconn.set_client_encoding("WAT"))
