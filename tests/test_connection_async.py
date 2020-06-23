import gc
import pytest
import logging
import weakref

import psycopg3
from psycopg3 import AsyncConnection
from psycopg3.conninfo import conninfo_to_dict

pytestmark = pytest.mark.asyncio


async def test_connect(dsn):
    conn = await AsyncConnection.connect(dsn)
    assert conn.status == conn.ConnStatus.OK


@pytest.mark.asyncio
async def test_connect_bad():
    with pytest.raises(psycopg3.OperationalError):
        await AsyncConnection.connect("dbname=nosuchdb")


async def test_connect_str_subclass(dsn):
    class MyString(str):
        pass

    conn = await AsyncConnection.connect(MyString(dsn))
    assert conn.status == conn.ConnStatus.OK


async def test_close(aconn):
    assert not aconn.closed
    await aconn.close()
    assert aconn.closed
    assert aconn.status == aconn.ConnStatus.BAD
    await aconn.close()
    assert aconn.closed
    assert aconn.status == aconn.ConnStatus.BAD


async def test_weakref(dsn):
    conn = await psycopg3.AsyncConnection.connect(dsn)
    w = weakref.ref(conn)
    await conn.close()
    del conn
    gc.collect()
    assert w() is None


async def test_commit(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    aconn.pgconn.exec_(b"insert into foo values (1)")
    await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.get_value(0, 0) == b"1"

    await aconn.close()
    with pytest.raises(psycopg3.OperationalError):
        await aconn.commit()


async def test_rollback(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")
    aconn.pgconn.exec_(b"begin")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    aconn.pgconn.exec_(b"insert into foo values (1)")
    await aconn.rollback()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    res = aconn.pgconn.exec_(b"select id from foo where id = 1")
    assert res.ntuples == 0

    await aconn.close()
    with pytest.raises(psycopg3.OperationalError):
        await aconn.rollback()


async def test_auto_transaction(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = aconn.cursor()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    await cur.execute("insert into foo values (1)")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS

    await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    await cur.execute("select * from foo")
    assert await cur.fetchone() == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


async def test_auto_transaction_fail(aconn):
    aconn.pgconn.exec_(b"drop table if exists foo")
    aconn.pgconn.exec_(b"create table foo (id int primary key)")

    cur = aconn.cursor()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE

    await cur.execute("insert into foo values (1)")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS

    with pytest.raises(psycopg3.DatabaseError):
        await cur.execute("meh")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR

    await aconn.commit()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE
    await cur.execute("select * from foo")
    assert await cur.fetchone() is None
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


async def test_autocommit(aconn):
    assert aconn.autocommit is False
    aconn.autocommit = True
    assert aconn.autocommit
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.IDLE


async def test_autocommit_connect(dsn):
    aconn = await psycopg3.AsyncConnection.connect(dsn, autocommit=True)
    assert aconn.autocommit


async def test_autocommit_intrans(aconn):
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    with pytest.raises(psycopg3.ProgrammingError):
        aconn.autocommit = True
    assert not aconn.autocommit


async def test_autocommit_inerror(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        await cur.execute("meh")
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR
    with pytest.raises(psycopg3.ProgrammingError):
        aconn.autocommit = True
    assert not aconn.autocommit


async def test_autocommit_unknown(aconn):
    await aconn.close()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg3.ProgrammingError):
        aconn.autocommit = True
    assert not aconn.autocommit


async def test_get_encoding(aconn):
    cur = aconn.cursor()
    await cur.execute("show client_encoding")
    (enc,) = await cur.fetchone()
    assert enc == aconn.encoding


async def test_set_encoding(aconn):
    newenc = "LATIN1" if aconn.encoding != "LATIN1" else "UTF8"
    assert aconn.encoding != newenc
    await aconn.set_client_encoding(newenc)
    assert aconn.encoding == newenc
    cur = aconn.cursor()
    await cur.execute("show client_encoding")
    (enc,) = await cur.fetchone()
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
async def test_normalize_encoding(aconn, enc, out, codec):
    await aconn.set_client_encoding(enc)
    assert aconn.encoding == out
    assert aconn.codec.name == codec


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
async def test_encoding_env_var(dsn, monkeypatch, enc, out, codec):
    monkeypatch.setenv("PGCLIENTENCODING", enc)
    aconn = await psycopg3.AsyncConnection.connect(dsn)
    assert aconn.encoding == out
    assert aconn.codec.name == codec


async def test_set_encoding_unsupported(aconn):
    await aconn.set_client_encoding("EUC_TW")
    with pytest.raises(psycopg3.NotSupportedError):
        await aconn.cursor().execute("select 1")


async def test_set_encoding_bad(aconn):
    with pytest.raises(psycopg3.DatabaseError):
        await aconn.set_client_encoding("WAT")


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
async def test_connect_args(monkeypatch, pgconn, testdsn, kwargs, want):
    the_conninfo = None

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    await psycopg3.AsyncConnection.connect(testdsn, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)


@pytest.mark.parametrize(
    "args, kwargs", [((), {}), (("", ""), {}), ((), {"nosuchparam": 42})],
)
async def test_connect_badargs(monkeypatch, pgconn, args, kwargs):
    def fake_connect(conninfo):
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    with pytest.raises((TypeError, psycopg3.ProgrammingError)):
        await psycopg3.AsyncConnection.connect(*args, **kwargs)


async def test_broken_connection(aconn):
    cur = aconn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        await cur.execute("select pg_terminate_backend(pg_backend_pid())")
    assert aconn.closed


async def test_notice_handlers(aconn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3")
    messages = []
    severities = []

    def cb1(diag):
        messages.append(diag.message_primary)

    def cb2(res):
        raise Exception("hello from cb2")

    aconn.add_notice_handler(cb1)
    aconn.add_notice_handler(cb2)
    aconn.add_notice_handler("the wrong thing")
    aconn.add_notice_handler(lambda diag: severities.append(diag.severity))

    aconn.pgconn.exec_(b"set client_min_messages to notice")
    cur = aconn.cursor()
    await cur.execute(
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

    aconn.remove_notice_handler(cb1)
    aconn.remove_notice_handler("the wrong thing")
    await cur.execute(
        "do $$begin raise warning 'hello warning'; end$$ language plpgsql"
    )
    assert len(caplog.records) == 3
    assert messages == ["hello notice"]
    assert severities == ["NOTICE", "WARNING"]

    with pytest.raises(ValueError):
        aconn.remove_notice_handler(cb1)


async def test_notify_handlers(aconn):
    nots1 = []
    nots2 = []

    def cb1(n):
        nots1.append(n)

    aconn.add_notify_handler(cb1)
    aconn.add_notify_handler(lambda n: nots2.append(n))

    aconn.autocommit = True
    cur = aconn.cursor()
    await cur.execute("listen foo")
    await cur.execute("notify foo, 'n1'")

    assert len(nots1) == 1
    n = nots1[0]
    assert n.channel == "foo"
    assert n.payload == "n1"
    assert n.pid == aconn.pgconn.backend_pid

    assert len(nots2) == 1
    assert nots2[0] == nots1[0]

    aconn.remove_notify_handler(cb1)
    await cur.execute("notify foo, 'n2'")

    assert len(nots1) == 1
    assert len(nots2) == 2
    n = nots2[1]
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == aconn.pgconn.backend_pid

    with pytest.raises(ValueError):
        aconn.remove_notify_handler(cb1)
