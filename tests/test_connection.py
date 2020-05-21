import pytest
import logging

import psycopg3
from psycopg3 import Connection
from psycopg3.conninfo import conninfo_to_dict


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
    conn.pgconn.exec_(b"insert into foo values (1)")
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
    conn.pgconn.exec_(b"insert into foo values (1)")
    conn.rollback()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.ntuples == 0

    conn.close()
    with pytest.raises(psycopg3.OperationalError):
        conn.rollback()


def test_auto_transaction(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = conn.cursor()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    cur.execute("insert into foo values (1)")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS

    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from foo").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


def test_auto_transaction_fail(conn):
    conn.pgconn.exec_(b"drop table if exists foo")
    conn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = conn.cursor()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    cur.execute("insert into foo values (1)")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS

    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR

    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from foo").fetchone() is None
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


def test_autocommit(conn):
    assert conn.autocommit is False
    conn.autocommit = True
    assert conn.autocommit
    cur = conn.cursor()
    assert cur.execute("select 1").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE


def test_autocommit_connect(dsn):
    conn = Connection.connect(dsn, autocommit=True)
    assert conn.autocommit


def test_autocommit_intrans(conn):
    cur = conn.cursor()
    assert cur.execute("select 1").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    with pytest.raises(psycopg3.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit_inerror(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR
    with pytest.raises(psycopg3.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit_unknown(conn):
    conn.close()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg3.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


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
def test_connect_args(monkeypatch, pgconn, testdsn, kwargs, want):
    the_conninfo = None

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    psycopg3.Connection.connect(testdsn, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)


@pytest.mark.parametrize(
    "args, kwargs", [((), {}), (("", ""), {}), ((), {"nosuchparam": 42})],
)
def test_connect_badargs(monkeypatch, pgconn, args, kwargs):
    def fake_connect(conninfo):
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    with pytest.raises((TypeError, psycopg3.ProgrammingError)):
        psycopg3.Connection.connect(*args, **kwargs)


def test_broken_connection(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("select pg_terminate_backend(pg_backend_pid())")
    assert conn.closed


def test_notice_callbacks(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3")
    messages = []
    severities = []

    def cb1(res):
        messages.append(
            res.error_field(psycopg3.pq.DiagnosticField.MESSAGE_PRIMARY)
        )

    def cb2(res):
        raise Exception("hello from cb2")

    def cb3(res):
        severities.append(
            res.error_field(psycopg3.pq.DiagnosticField.SEVERITY_NONLOCALIZED)
        )

    conn.add_notice_callback(cb1)
    conn.add_notice_callback(cb2)
    conn.add_notice_callback("the wrong thing")
    conn.add_notice_callback(cb3)

    cur = conn.cursor()
    cur.execute(
        """
do $$
begin
    raise notice 'hello notice';
end
$$ language plpgsql
    """
    )
    assert messages == [b"hello notice"]
    assert severities == [b"NOTICE"]

    assert len(caplog.records) == 2
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello from cb2" in rec.message
    rec = caplog.records[1]
    assert rec.levelno == logging.ERROR
    assert "the wrong thing" in rec.message

    conn.remove_notice_callback(cb1)
    conn.remove_notice_callback("the wrong thing")
    cur.execute(
        """
do $$
begin
    raise warning 'hello warning';
end
$$ language plpgsql
    """
    )
    assert len(caplog.records) == 3
    assert messages == [b"hello notice"]
    assert severities == [b"NOTICE", b"WARNING"]

    with pytest.raises(ValueError):
        conn.remove_notice_callback(cb1)
