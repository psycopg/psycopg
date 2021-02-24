import gc
import sys
import time
import socket
import pytest
import logging
import weakref
from threading import Thread

import psycopg3
from psycopg3 import encodings
from psycopg3 import Connection, Notify
from psycopg3.rows import tuple_row
from psycopg3.errors import UndefinedTable
from psycopg3.conninfo import conninfo_to_dict
from .test_cursor import my_row_factory


def test_connect(dsn):
    conn = Connection.connect(dsn)
    assert not conn.closed
    assert conn.pgconn.status == conn.ConnStatus.OK


def test_connect_str_subclass(dsn):
    class MyString(str):
        pass

    conn = Connection.connect(MyString(dsn))
    assert not conn.closed
    assert conn.pgconn.status == conn.ConnStatus.OK


def test_connect_bad():
    with pytest.raises(psycopg3.OperationalError):
        Connection.connect("dbname=nosuchdb")


@pytest.mark.slow
@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="connect() hangs on Win32")
def test_connect_timeout():
    s = socket.socket(socket.AF_INET)
    s.bind(("localhost", 0))
    port = s.getsockname()[1]
    s.listen(0)

    def closer():
        time.sleep(1.5)
        s.close()

    Thread(target=closer).start()

    t0 = time.time()
    with pytest.raises(psycopg3.DatabaseError):
        Connection.connect(host="localhost", port=port, connect_timeout=1)
    elapsed = time.time() - t0
    assert elapsed == pytest.approx(1.0, abs=0.05)


def test_close(conn):
    assert not conn.closed
    conn.close()
    assert conn.closed
    assert conn.pgconn.status == conn.ConnStatus.BAD

    cur = conn.cursor()

    conn.close()
    assert conn.closed
    assert conn.pgconn.status == conn.ConnStatus.BAD

    with pytest.raises(psycopg3.OperationalError):
        cur.execute("select 1")


def test_connection_warn_close(dsn, recwarn):
    conn = Connection.connect(dsn)
    conn.close()
    del conn
    assert not recwarn

    conn = Connection.connect(dsn)
    del conn
    assert "IDLE" in str(recwarn.pop(ResourceWarning).message)

    conn = Connection.connect(dsn)
    conn.execute("select 1")
    del conn
    assert "INTRANS" in str(recwarn.pop(ResourceWarning).message)

    conn = Connection.connect(dsn)
    try:
        conn.execute("select wat")
    except Exception:
        pass
    del conn
    assert "INERROR" in str(recwarn.pop(ResourceWarning).message)

    with Connection.connect(dsn) as conn:
        pass
    del conn
    assert not recwarn


def test_context_commit(conn, dsn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists textctx")
            cur.execute("create table textctx ()")

    assert conn.closed

    with psycopg3.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from textctx")
            assert cur.fetchall() == []


def test_context_rollback(conn, dsn):
    with conn.cursor() as cur:
        cur.execute("drop table if exists textctx")
    conn.commit()

    with pytest.raises(ZeroDivisionError):
        with conn:
            with conn.cursor() as cur:
                cur.execute("create table textctx ()")
                1 / 0

    assert conn.closed

    with psycopg3.connect(dsn) as conn:
        with conn.cursor() as cur:
            with pytest.raises(UndefinedTable):
                cur.execute("select * from textctx")


def test_context_rollback_no_clobber(conn, dsn, recwarn):
    with pytest.raises(ZeroDivisionError):
        with psycopg3.connect(dsn) as conn2:
            conn2.execute("select 1")
            conn.execute(
                "select pg_terminate_backend(%s::int)",
                [conn2.pgconn.backend_pid],
            )
            1 / 0

    assert "rolling back" in str(recwarn.pop(RuntimeWarning).message)


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
    assert conn.client_encoding == encodings.pg2py(enc)


def test_set_encoding(conn):
    newenc = "iso8859-1" if conn.client_encoding != "iso8859-1" else "utf-8"
    assert conn.client_encoding != newenc
    conn.client_encoding = newenc
    assert conn.client_encoding == newenc
    (enc,) = conn.cursor().execute("show client_encoding").fetchone()
    assert encodings.pg2py(enc) == newenc


@pytest.mark.parametrize(
    "enc, out, codec",
    [
        ("utf8", "UTF8", "utf-8"),
        ("utf-8", "UTF8", "utf-8"),
        ("utf_8", "UTF8", "utf-8"),
        ("eucjp", "EUC_JP", "euc_jp"),
        ("euc-jp", "EUC_JP", "euc_jp"),
        ("latin9", "LATIN9", "iso8859-15"),
    ],
)
def test_normalize_encoding(conn, enc, out, codec):
    conn.client_encoding = enc
    assert (
        conn.pgconn.parameter_status(b"client_encoding").decode("utf-8") == out
    )
    assert conn.client_encoding == codec


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
    assert (
        conn.pgconn.parameter_status(b"client_encoding").decode("utf-8") == out
    )
    assert conn.client_encoding == codec


def test_set_encoding_unsupported(conn):
    cur = conn.cursor()
    cur.execute("set client_encoding to EUC_TW")
    with pytest.raises(psycopg3.NotSupportedError):
        cur.execute("select 'x'")


def test_set_encoding_bad(conn):
    with pytest.raises(LookupError):
        conn.client_encoding = "WAT"


@pytest.mark.parametrize(
    "args, kwargs, want",
    [
        ((), {}, ""),
        (("",), {}, ""),
        (("host=foo user=bar",), {}, "host=foo user=bar"),
        (("host=foo",), {"user": "baz"}, "host=foo user=baz"),
        (
            ("host=foo port=5432",),
            {"host": "qux", "user": "joe"},
            "host=qux user=joe port=5432",
        ),
        (("host=foo",), {"user": None}, "host=foo"),
    ],
)
def test_connect_args(monkeypatch, pgconn, args, kwargs, want):
    the_conninfo = None

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    psycopg3.Connection.connect(*args, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)


@pytest.mark.parametrize(
    "args, kwargs",
    [
        (("host=foo", "host=bar"), {}),
        (("", ""), {}),
        ((), {"nosuchparam": 42}),
    ],
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
    assert isinstance(n, Notify)
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == conn.pgconn.backend_pid
    assert hash(n)

    with pytest.raises(ValueError):
        conn.remove_notify_handler(cb1)


def test_execute(conn):
    cur = conn.execute("select %s, %s", [10, 20])
    assert cur.fetchone() == (10, 20)

    cur = conn.execute("select %(a)s, %(b)s", {"a": 11, "b": 21})
    assert cur.fetchone() == (11, 21)

    cur = conn.execute("select 12, 22")
    assert cur.fetchone() == (12, 22)


def test_row_factory(dsn):
    conn = Connection.connect(dsn)
    assert conn.row_factory is tuple_row

    conn = Connection.connect(dsn, row_factory=my_row_factory)
    assert conn.row_factory is my_row_factory

    cur = conn.execute("select 'a' as ve")
    assert cur.fetchone() == ["Ave"]

    with conn.cursor(row_factory=lambda c: set) as cur:
        cur.execute("select 1, 1, 2")
        assert cur.fetchall() == [{1, 2}]

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("select 1, 1, 2")
        assert cur.fetchall() == [(1, 1, 2)]

    conn.row_factory = tuple_row
    cur = conn.execute("select 'vale'")
    assert cur.fetchone() == ("vale",)


def test_str(conn):
    assert "[IDLE]" in str(conn)
    conn.close()
    assert "[BAD]" in str(conn)
