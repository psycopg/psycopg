import gc
import os
import ctypes
import logging
import weakref
from select import select

import pytest

import psycopg3
from psycopg3 import pq
import psycopg3.generators


def test_connectdb(dsn):
    conn = pq.PGconn.connect(dsn.encode("utf8"))
    assert conn.status == pq.ConnStatus.OK, conn.error_message


def test_connectdb_error():
    conn = pq.PGconn.connect(b"dbname=psycopg3_test_not_for_real")
    assert conn.status == pq.ConnStatus.BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_connectdb_badtype(baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connect(baddsn)


def test_connect_async(dsn):
    conn = pq.PGconn.connect_start(dsn.encode("utf8"))
    conn.nonblocking = 1
    while 1:
        assert conn.status != pq.ConnStatus.BAD
        rv = conn.connect_poll()
        if rv == pq.PollingStatus.OK:
            break
        elif rv == pq.PollingStatus.READING:
            select([conn.socket], [], [])
        elif rv == pq.PollingStatus.WRITING:
            select([], [conn.socket], [])
        else:
            assert False, rv

    assert conn.status == pq.ConnStatus.OK

    conn.finish()
    with pytest.raises(psycopg3.OperationalError):
        conn.connect_poll()


def test_connect_async_bad(dsn):
    parsed_dsn = {
        e.keyword: e.val
        for e in pq.Conninfo.parse(dsn.encode("utf8"))
        if e.val
    }
    parsed_dsn[b"dbname"] = b"psycopg3_test_not_for_real"
    dsn = b" ".join(b"%s='%s'" % item for item in parsed_dsn.items())
    conn = pq.PGconn.connect_start(dsn)
    while 1:
        assert conn.status != pq.ConnStatus.BAD, conn.error_message
        rv = conn.connect_poll()
        if rv == pq.PollingStatus.FAILED:
            break
        elif rv == pq.PollingStatus.READING:
            select([conn.socket], [], [])
        elif rv == pq.PollingStatus.WRITING:
            select([], [conn.socket], [])
        else:
            assert False, rv

    assert conn.status == pq.ConnStatus.BAD


def test_finish(pgconn):
    assert pgconn.status == pq.ConnStatus.OK
    pgconn.finish()
    assert pgconn.status == pq.ConnStatus.BAD
    pgconn.finish()
    assert pgconn.status == pq.ConnStatus.BAD


def test_weakref(dsn):
    conn = pq.PGconn.connect(dsn.encode("utf8"))
    w = weakref.ref(conn)
    conn.finish()
    del conn
    gc.collect()
    assert w() is None


def test_pgconn_ptr(pgconn, libpq):
    pgconn.pgconn_ptr is not None

    f = libpq.PQserverVersion
    f.argtypes = [ctypes.c_void_p]
    f.restype = ctypes.c_int
    ver = f(pgconn.pgconn_ptr)
    assert ver == pgconn.server_version

    pgconn.finish()
    assert pgconn.pgconn_ptr is None


def test_info(dsn, pgconn):
    info = pgconn.info
    assert len(info) > 20
    dbname = [d for d in info if d.keyword == b"dbname"][0]
    assert dbname.envvar == b"PGDATABASE"
    assert dbname.label == b"Database-Name"
    assert dbname.dispchar == b""
    assert dbname.dispsize == 20

    parsed = pq.Conninfo.parse(dsn.encode("utf8"))
    name = [o.val for o in parsed if o.keyword == b"dbname"][0]
    user = [o.val for o in parsed if o.keyword == b"user"][0]
    assert dbname.val == (name or user)

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.info


def test_reset(pgconn):
    assert pgconn.status == pq.ConnStatus.OK
    pgconn.exec_(b"select pg_terminate_backend(pg_backend_pid())")
    assert pgconn.status == pq.ConnStatus.BAD
    pgconn.reset()
    assert pgconn.status == pq.ConnStatus.OK

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.reset()

    assert pgconn.status == pq.ConnStatus.BAD


def test_reset_async(pgconn):
    assert pgconn.status == pq.ConnStatus.OK
    pgconn.exec_(b"select pg_terminate_backend(pg_backend_pid())")
    assert pgconn.status == pq.ConnStatus.BAD
    pgconn.reset_start()
    while 1:
        rv = pgconn.reset_poll()
        if rv == pq.PollingStatus.READING:
            select([pgconn.socket], [], [])
        elif rv == pq.PollingStatus.WRITING:
            select([], [pgconn.socket], [])
        else:
            break

    assert rv == pq.PollingStatus.OK
    assert pgconn.status == pq.ConnStatus.OK

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.reset_start()

    with pytest.raises(psycopg3.OperationalError):
        pgconn.reset_poll()


def test_ping(dsn):
    rv = pq.PGconn.ping(dsn.encode("utf8"))
    assert rv == pq.Ping.OK

    rv = pq.PGconn.ping(b"port=99999")
    assert rv == pq.Ping.NO_RESPONSE


def test_db(pgconn):
    name = [o.val for o in pgconn.info if o.keyword == b"dbname"][0]
    assert pgconn.db == name
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.db


def test_user(pgconn):
    user = [o.val for o in pgconn.info if o.keyword == b"user"][0]
    assert pgconn.user == user
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.user


def test_password(pgconn):
    # not in info
    assert isinstance(pgconn.password, bytes)
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.password


def test_host(pgconn):
    # might be not in info
    assert isinstance(pgconn.host, bytes)
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.host


@pytest.mark.libpq(">= 12")
def test_hostaddr(pgconn):
    # not in info
    assert isinstance(pgconn.hostaddr, bytes), pgconn.hostaddr
    pgconn.finish()
    if psycopg3.pq.__impl__ == "python":
        with pytest.raises(psycopg3.OperationalError):
            pgconn.hostaddr

    else:
        if pgconn.hostaddr == b"TODO":
            pytest.xfail("implement hostaddr in psycopg3_c.pq")
        else:
            assert False, "you did it! not fix the test"


@pytest.mark.xfail
@pytest.mark.libpq("< 12")
def test_hostaddr_missing(pgconn):
    with pytest.raises(psycopg3.NotSupportedError):
        pgconn.hostaddr


def test_port(pgconn):
    port = [o.val for o in pgconn.info if o.keyword == b"port"][0]
    assert pgconn.port == port
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.port


def test_tty(pgconn):
    tty = [o.val for o in pgconn.info if o.keyword == b"tty"][0]
    assert pgconn.tty == tty
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.tty


def test_transaction_status(pgconn):
    assert pgconn.transaction_status == pq.TransactionStatus.IDLE
    pgconn.exec_(b"begin")
    assert pgconn.transaction_status == pq.TransactionStatus.INTRANS
    pgconn.send_query(b"select 1")
    assert pgconn.transaction_status == pq.TransactionStatus.ACTIVE
    psycopg3.waiting.wait(psycopg3.generators.execute(pgconn), pgconn.socket)
    assert pgconn.transaction_status == pq.TransactionStatus.INTRANS
    pgconn.finish()
    assert pgconn.transaction_status == pq.TransactionStatus.UNKNOWN


def test_parameter_status(dsn, monkeypatch):
    monkeypatch.setenv("PGAPPNAME", "psycopg3 tests")
    pgconn = pq.PGconn.connect(dsn.encode("utf8"))
    assert pgconn.parameter_status(b"application_name") == b"psycopg3 tests"
    assert pgconn.parameter_status(b"wat") is None
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.parameter_status(b"application_name")


def test_encoding(pgconn):
    res = pgconn.exec_(b"set client_encoding to latin1")
    assert res.status == pq.ExecStatus.COMMAND_OK
    assert pgconn.parameter_status(b"client_encoding") == b"LATIN1"

    res = pgconn.exec_(b"set client_encoding to 'utf-8'")
    assert res.status == pq.ExecStatus.COMMAND_OK
    assert pgconn.parameter_status(b"client_encoding") == b"UTF8"

    res = pgconn.exec_(b"set client_encoding to wat")
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert pgconn.parameter_status(b"client_encoding") == b"UTF8"

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.parameter_status(b"client_encoding")


def test_protocol_version(pgconn):
    assert pgconn.protocol_version == 3
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.protocol_version


def test_server_version(pgconn):
    assert pgconn.server_version >= 90400
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.server_version


def test_error_message(pgconn):
    assert pgconn.error_message == b""
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.FATAL_ERROR
    msg = pgconn.error_message
    assert b"wat" in msg
    pgconn.finish()
    assert b"NULL" in pgconn.error_message  # TODO: i10n?


def test_backend_pid(pgconn):
    assert isinstance(pgconn.backend_pid, int)
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.backend_pid


def test_needs_password(pgconn):
    # assume connection worked so an eventually needed password wasn't missing
    assert pgconn.needs_password is False
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.needs_password


def test_used_password(pgconn, dsn, monkeypatch):
    assert isinstance(pgconn.used_password, bool)

    # Assume that if a password was passed then it was needed.
    # Note that the server may still need a password passed via pgpass
    # so it may be that has_password is false but still a password was
    # requested by the server and passed by libpq.
    info = pq.Conninfo.parse(dsn.encode("utf8"))
    has_password = (
        "PGPASSWORD" in os.environ
        or [i for i in info if i.keyword == b"password"][0].val is not None
    )
    if has_password:
        assert pgconn.used_password

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.used_password


def test_ssl_in_use(pgconn):
    assert isinstance(pgconn.ssl_in_use, bool)

    # If connecting via socket then ssl is not in use
    if pgconn.host.startswith(b"/"):
        assert not pgconn.ssl_in_use
    else:
        sslmode = [i.val for i in pgconn.info if i.keyword == b"sslmode"][0]
        if sslmode not in (b"disable", b"allow", b"prefer"):
            assert pgconn.ssl_in_use

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.ssl_in_use


def test_set_single_row_mode(pgconn):
    with pytest.raises(pq.PQerror):
        pgconn.set_single_row_mode()

    pgconn.send_query(b"select 1")
    pgconn.set_single_row_mode()


def test_cancel(pgconn):
    cancel = pgconn.get_cancel()
    cancel.cancel()
    cancel.cancel()
    pgconn.finish()
    cancel.cancel()
    with pytest.raises(pq.PQerror):
        pgconn.get_cancel()


def test_cancel_free(pgconn):
    cancel = pgconn.get_cancel()
    cancel.free()
    with pytest.raises(pq.PQerror):
        cancel.cancel()
    cancel.free()


def test_notify(pgconn):
    assert pgconn.notifies() is None

    pgconn.exec_(b"listen foo")
    pgconn.exec_(b"listen bar")
    pgconn.exec_(b"notify foo, '1'")
    pgconn.exec_(b"notify bar, '2'")
    pgconn.exec_(b"notify foo, '3'")

    n = pgconn.notifies()
    assert n.relname == b"foo"
    assert n.be_pid == pgconn.backend_pid
    assert n.extra == b"1"

    n = pgconn.notifies()
    assert n.relname == b"bar"
    assert n.be_pid == pgconn.backend_pid
    assert n.extra == b"2"

    n = pgconn.notifies()
    assert n.relname == b"foo"
    assert n.be_pid == pgconn.backend_pid
    assert n.extra == b"3"

    assert pgconn.notifies() is None


def test_notice_nohandler(pgconn):
    pgconn.exec_(b"set client_min_messages to notice")
    res = pgconn.exec_(
        b"do $$begin raise notice 'hello notice'; end$$ language plpgsql"
    )
    assert res.status == pq.ExecStatus.COMMAND_OK


def test_notice(pgconn):
    msgs = []

    def callback(res):
        assert res.status == pq.ExecStatus.NONFATAL_ERROR
        msgs.append(res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY))

    pgconn.exec_(b"set client_min_messages to notice")
    pgconn.notice_handler = callback
    res = pgconn.exec_(
        b"do $$begin raise notice 'hello notice'; end$$ language plpgsql"
    )

    assert res.status == pq.ExecStatus.COMMAND_OK
    assert msgs and msgs[0] == b"hello notice"


def test_notice_error(pgconn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3")

    def callback(res):
        raise Exception("hello error")

    pgconn.exec_(b"set client_min_messages to notice")
    pgconn.notice_handler = callback
    res = pgconn.exec_(
        b"do $$begin raise notice 'hello notice'; end$$ language plpgsql"
    )

    assert res.status == pq.ExecStatus.COMMAND_OK
    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello error" in rec.message


def test_str(pgconn, dsn):
    assert "[IDLE]" in str(pgconn)
    pgconn.finish()
    assert "[BAD]" in str(pgconn)

    pgconn2 = pq.PGconn.connect_start(dsn.encode("utf8"))
    assert "[" in str(pgconn2)
    assert "[IDLE]" not in str(pgconn2)
