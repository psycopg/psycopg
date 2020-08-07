import gc
import pytest
import logging
import weakref

import psycopg3
from psycopg3 import Connection
from psycopg3.conninfo import conninfo_to_dict


def test_connect(dsn):
    conn = Connection.connect(dsn)
    assert conn.status == conn.ConnStatus.OK


def test_connect_str_subclass(dsn):
    class MyString(str):
        pass

    conn = Connection.connect(MyString(dsn))
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


def test_weakref(dsn):
    conn = psycopg3.connect(dsn)
    w = weakref.ref(conn)
    conn.close()
    del conn
    gc.collect()
    assert w() is None


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

    with pytest.raises(psycopg3.errors.InFailedSqlTransaction):
        cur.execute("select 1")

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


@pytest.mark.parametrize(
    "enc, out, codec",
    [
        ("utf8", "UTF8", "utf-8"),
        ("utf-8", "UTF8", "utf-8"),
        ("utf_8", "UTF8", "utf-8"),
        ("eucjp", "EUC_JP", "euc_jp"),
        ("euc-jp", "EUC_JP", "euc_jp"),
    ],
)
def test_normalize_encoding(conn, enc, out, codec):
    conn.set_client_encoding(enc)
    assert conn.encoding == out
    assert conn.codec.name == codec


@pytest.mark.parametrize(
    "enc, out, codec",
    [
        ("utf8", "UTF8", "utf-8"),
        ("utf-8", "UTF8", "utf-8"),
        ("utf_8", "UTF8", "utf-8"),
        ("eucjp", "EUC_JP", "euc_jp"),
        ("euc-jp", "EUC_JP", "euc_jp"),
    ],
)
def test_encoding_env_var(dsn, monkeypatch, enc, out, codec):
    monkeypatch.setenv("PGCLIENTENCODING", enc)
    conn = psycopg3.connect(dsn)
    assert conn.encoding == out
    assert conn.codec.name == codec


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


def test_notice_handlers(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3")
    messages = []
    severities = []

    def cb1(diag):
        messages.append(diag.message_primary)

    def cb2(res):
        raise Exception("hello from cb2")

    conn.add_notice_handler(cb1)
    conn.add_notice_handler(cb2)
    conn.add_notice_handler("the wrong thing")
    conn.add_notice_handler(lambda diag: severities.append(diag.severity))

    conn.pgconn.exec_(b"set client_min_messages to notice")
    cur = conn.cursor()
    cur.execute(
        "do $$begin raise notice 'hello notice'; end$$ language plpgsql"
    )
    assert messages == ["hello notice"]
    assert severities == ["NOTICE"]

    assert len(caplog.records) == 2
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello from cb2" in rec.message
    rec = caplog.records[1]
    assert rec.levelno == logging.ERROR
    assert "the wrong thing" in rec.message

    conn.remove_notice_handler(cb1)
    conn.remove_notice_handler("the wrong thing")
    cur.execute(
        "do $$begin raise warning 'hello warning'; end$$ language plpgsql"
    )
    assert len(caplog.records) == 3
    assert messages == ["hello notice"]
    assert severities == ["NOTICE", "WARNING"]

    with pytest.raises(ValueError):
        conn.remove_notice_handler(cb1)


def test_notify_handlers(conn):
    nots1 = []
    nots2 = []

    def cb1(n):
        nots1.append(n)

    conn.add_notify_handler(cb1)
    conn.add_notify_handler(lambda n: nots2.append(n))

    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("listen foo")
    cur.execute("notify foo, 'n1'")

    assert len(nots1) == 1
    n = nots1[0]
    assert n.channel == "foo"
    assert n.payload == "n1"
    assert n.pid == conn.pgconn.backend_pid

    assert len(nots2) == 1
    assert nots2[0] == nots1[0]

    conn.remove_notify_handler(cb1)
    cur.execute("notify foo, 'n2'")

    assert len(nots1) == 1
    assert len(nots2) == 2
    n = nots2[1]
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == conn.pgconn.backend_pid

    with pytest.raises(ValueError):
        conn.remove_notify_handler(cb1)
