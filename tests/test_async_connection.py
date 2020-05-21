import pytest

import psycopg3
from psycopg3 import AsyncConnection
from psycopg3.conninfo import conninfo_to_dict


def test_connect(dsn, loop):
    conn = loop.run_until_complete(AsyncConnection.connect(dsn))
    assert conn.status == conn.ConnStatus.OK


def test_connect_bad(loop):
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(AsyncConnection.connect("dbname=nosuchdb"))


def test_close(aconn, loop):
    assert not aconn.closed
    loop.run_until_complete(aconn.close())
    assert aconn.closed
    assert aconn.status == aconn.ConnStatus.BAD
    loop.run_until_complete(aconn.close())
    assert aconn.closed
    assert aconn.status == aconn.ConnStatus.BAD


def test_commit(loop, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    aconn.pgconn.exec_(b"insert into foo values (1)")
    loop.run_until_complete(aconn.commit())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    loop.run_until_complete(aconn.close())
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(aconn.commit())


def test_rollback(loop, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    aconn.pgconn.exec_(b"insert into foo values (1)")
    loop.run_until_complete(aconn.rollback())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.ntuples == 0

    loop.run_until_complete(aconn.close())
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(aconn.rollback())


def test_auto_transaction(loop, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = aconn.cursor()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    loop.run_until_complete(cur.execute("insert into foo values (1)"))
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS

    loop.run_until_complete(aconn.commit())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    loop.run_until_complete(cur.execute("select * from foo"))
    assert loop.run_until_complete(cur.fetchone()) == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


def test_auto_transaction_fail(loop, aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = aconn.cursor()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    loop.run_until_complete(cur.execute("insert into foo values (1)"))
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS

    with pytest.raises(psycopg3.DatabaseError):
        loop.run_until_complete(cur.execute("meh"))
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR

    loop.run_until_complete(aconn.commit())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    loop.run_until_complete(cur.execute("select * from foo"))
    assert loop.run_until_complete(cur.fetchone()) is None
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


def test_autocommit(loop, aconn):
    assert aconn.autocommit is False
    aconn.autocommit = True
    assert aconn.autocommit
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("select 1"))
    assert loop.run_until_complete(cur.fetchone()) == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE


def test_autocommit_connect(loop, dsn):
    aconn = loop.run_until_complete(
        psycopg3.AsyncConnection.connect(dsn, autocommit=True)
    )
    assert aconn.autocommit


def test_autocommit_intrans(loop, aconn):
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("select 1"))
    assert loop.run_until_complete(cur.fetchone()) == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    with pytest.raises(psycopg3.ProgrammingError):
        aconn.autocommit = True
    assert not aconn.autocommit


def test_autocommit_inerror(loop, aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        loop.run_until_complete(cur.execute("meh"))
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR
    with pytest.raises(psycopg3.ProgrammingError):
        aconn.autocommit = True
    assert not aconn.autocommit


def test_autocommit_unknown(loop, aconn):
    loop.run_until_complete(aconn.close())
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg3.ProgrammingError):
        aconn.autocommit = True
    assert not aconn.autocommit


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


@pytest.mark.parametrize(
    "testdsn, kwargs, want",
    [
        ("", {}, ""),
        ("host=foo user=bar", {}, "host=foo user=bar"),
        ("host=foo", {"user": "baz"}, "host=foo user=baz"),
        (
            "host=foo port=5432",
            {"host": "qux", "user": "joe"},
            "host=qux user=joe port=5432",
        ),
        ("host=foo", {"user": None}, "host=foo"),
    ],
)
def test_connect_args(monkeypatch, pgconn, loop, testdsn, kwargs, want):
    the_conninfo = None

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    loop.run_until_complete(
        psycopg3.AsyncConnection.connect(testdsn, **kwargs)
    )
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)


@pytest.mark.parametrize(
    "args, kwargs", [((), {}), (("", ""), {}), ((), {"nosuchparam": 42})],
)
def test_connect_badargs(monkeypatch, pgconn, loop, args, kwargs):
    def fake_connect(conninfo):
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    with pytest.raises((TypeError, psycopg3.ProgrammingError)):
        loop.run_until_complete(
            psycopg3.AsyncConnection.connect(*args, **kwargs)
        )


def test_broken_connection(aconn, loop):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        loop.run_until_complete(
            cur.execute("select pg_terminate_backend(pg_backend_pid())")
        )
    assert aconn.closed
