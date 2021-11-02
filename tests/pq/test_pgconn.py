import os
import ctypes
import logging
import weakref
from select import select

import pytest

import psycopg
from psycopg import pq
import psycopg.generators

from ..utils import gc_collect


def test_connectdb(dsn):
    conn = pq.PGconn.connect(dsn.encode())
    assert conn.status == pq.ConnStatus.OK, conn.error_message


def test_connectdb_error():
    conn = pq.PGconn.connect(b"dbname=psycopg_test_not_for_real")
    assert conn.status == pq.ConnStatus.BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_connectdb_badtype(baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connect(baddsn)


def test_connect_async(dsn):
    conn = pq.PGconn.connect_start(dsn.encode())
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
    with pytest.raises(psycopg.OperationalError):
        conn.connect_poll()


def test_connect_async_bad(dsn):
    parsed_dsn = {
        e.keyword: e.val for e in pq.Conninfo.parse(dsn.encode()) if e.val
    }
    parsed_dsn[b"dbname"] = b"psycopg_test_not_for_real"
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


@pytest.mark.slow
def test_weakref(dsn):
    conn = pq.PGconn.connect(dsn.encode())
    w = weakref.ref(conn)
    conn.finish()
    del conn
    gc_collect()
    assert w() is None


def test_pgconn_ptr(pgconn, libpq):
    assert isinstance(pgconn.pgconn_ptr, int)

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

    parsed = pq.Conninfo.parse(dsn.encode())
    # take the name and the user either from params or from env vars
    name = [
        o.val or os.environ.get(o.envvar.decode(), "").encode()
        for o in parsed
        if o.keyword == b"dbname" and o.envvar
    ][0]
    user = [
        o.val or os.environ.get(o.envvar.decode(), "").encode()
        for o in parsed
        if o.keyword == b"user" and o.envvar
    ][0]
    assert dbname.val == (name or user)

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.info


def test_reset(pgconn):
    assert pgconn.status == pq.ConnStatus.OK
    pgconn.exec_(b"select pg_terminate_backend(pg_backend_pid())")
    assert pgconn.status == pq.ConnStatus.BAD
    pgconn.reset()
    assert pgconn.status == pq.ConnStatus.OK

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
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
    with pytest.raises(psycopg.OperationalError):
        pgconn.reset_start()

    with pytest.raises(psycopg.OperationalError):
        pgconn.reset_poll()


def test_ping(dsn):
    rv = pq.PGconn.ping(dsn.encode())
    assert rv == pq.Ping.OK

    rv = pq.PGconn.ping(b"port=9999")
    assert rv == pq.Ping.NO_RESPONSE


def test_db(pgconn):
    name = [o.val for o in pgconn.info if o.keyword == b"dbname"][0]
    assert pgconn.db == name
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.db


def test_user(pgconn):
    user = [o.val for o in pgconn.info if o.keyword == b"user"][0]
    assert pgconn.user == user
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.user


def test_password(pgconn):
    # not in info
    assert isinstance(pgconn.password, bytes)
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.password


def test_host(pgconn):
    # might be not in info
    assert isinstance(pgconn.host, bytes)
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.host


@pytest.mark.libpq(">= 12")
def test_hostaddr(pgconn):
    # not in info
    assert isinstance(pgconn.hostaddr, bytes), pgconn.hostaddr
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.hostaddr


@pytest.mark.libpq("< 12")
def test_hostaddr_missing(pgconn):
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.hostaddr


def test_port(pgconn):
    port = [o.val for o in pgconn.info if o.keyword == b"port"][0]
    assert pgconn.port == port
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.port


@pytest.mark.libpq("< 14")
def test_tty(pgconn):
    tty = [o.val for o in pgconn.info if o.keyword == b"tty"][0]
    assert pgconn.tty == tty
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.tty


@pytest.mark.libpq(">= 14")
def test_tty_noop(pgconn):
    assert not any(o.val for o in pgconn.info if o.keyword == b"tty")
    assert pgconn.tty == b""


def test_transaction_status(pgconn):
    assert pgconn.transaction_status == pq.TransactionStatus.IDLE
    pgconn.exec_(b"begin")
    assert pgconn.transaction_status == pq.TransactionStatus.INTRANS
    pgconn.send_query(b"select 1")
    assert pgconn.transaction_status == pq.TransactionStatus.ACTIVE
    psycopg.waiting.wait(psycopg.generators.execute(pgconn), pgconn.socket)
    assert pgconn.transaction_status == pq.TransactionStatus.INTRANS
    pgconn.finish()
    assert pgconn.transaction_status == pq.TransactionStatus.UNKNOWN


def test_parameter_status(dsn, monkeypatch):
    monkeypatch.setenv("PGAPPNAME", "psycopg tests")
    pgconn = pq.PGconn.connect(dsn.encode())
    assert pgconn.parameter_status(b"application_name") == b"psycopg tests"
    assert pgconn.parameter_status(b"wat") is None
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
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
    with pytest.raises(psycopg.OperationalError):
        pgconn.parameter_status(b"client_encoding")


def test_protocol_version(pgconn):
    assert pgconn.protocol_version == 3
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.protocol_version


def test_server_version(pgconn):
    assert pgconn.server_version >= 90400
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.server_version


def test_socket(pgconn):
    socket = pgconn.socket
    assert socket > 0
    pgconn.exec_(f"select pg_terminate_backend({pgconn.backend_pid})".encode())
    # TODO: on my box it raises OperationalError as it should. Not on Travis,
    # so let's see if at least an ok value comes out of it.
    try:
        assert pgconn.socket == socket
    except psycopg.OperationalError:
        pass


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
    with pytest.raises(psycopg.OperationalError):
        pgconn.backend_pid


def test_needs_password(pgconn):
    # assume connection worked so an eventually needed password wasn't missing
    assert pgconn.needs_password is False
    pgconn.finish()
    pgconn.needs_password


def test_used_password(pgconn, dsn, monkeypatch):
    assert isinstance(pgconn.used_password, bool)

    # Assume that if a password was passed then it was needed.
    # Note that the server may still need a password passed via pgpass
    # so it may be that has_password is false but still a password was
    # requested by the server and passed by libpq.
    info = pq.Conninfo.parse(dsn.encode())
    has_password = (
        "PGPASSWORD" in os.environ
        or [i for i in info if i.keyword == b"password"][0].val is not None
    )
    if has_password:
        # The assumption that the password is needed is broken on the Travis
        # PG 10 setup so let's skip that
        print("\n".join(map(str, sorted(os.environ.items()))))
        if not (os.environ.get("TRAVIS") and os.environ.get("PGVER") == "10"):
            assert pgconn.used_password

    pgconn.finish()
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
    with pytest.raises(psycopg.OperationalError):
        pgconn.ssl_in_use


def test_set_single_row_mode(pgconn):
    with pytest.raises(psycopg.OperationalError):
        pgconn.set_single_row_mode()

    pgconn.send_query(b"select 1")
    pgconn.set_single_row_mode()


def test_cancel(pgconn):
    cancel = pgconn.get_cancel()
    cancel.cancel()
    cancel.cancel()
    pgconn.finish()
    cancel.cancel()
    with pytest.raises(psycopg.OperationalError):
        pgconn.get_cancel()


def test_cancel_free(pgconn):
    cancel = pgconn.get_cancel()
    cancel.free()
    with pytest.raises(psycopg.OperationalError):
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
    caplog.set_level(logging.WARNING, logger="psycopg")

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


@pytest.mark.libpq(">= 10")
def test_encrypt_password(pgconn):
    enc = pgconn.encrypt_password(b"psycopg2", b"ashesh", b"md5")
    assert enc == b"md594839d658c28a357126f105b9cb14cfc"


@pytest.mark.libpq(">= 10")
def test_encrypt_password_scram(pgconn):
    enc = pgconn.encrypt_password(b"psycopg2", b"ashesh", b"scram-sha-256")
    assert enc.startswith(b"SCRAM-SHA-256$")


@pytest.mark.libpq(">= 10")
def test_encrypt_password_badalgo(pgconn):
    with pytest.raises(psycopg.OperationalError):
        assert pgconn.encrypt_password(b"psycopg2", b"ashesh", b"wat")


@pytest.mark.libpq(">= 10")
def test_encrypt_password_query(pgconn):
    res = pgconn.exec_(b"set password_encryption to 'md5'")
    assert res.status == pq.ExecStatus.COMMAND_OK
    enc = pgconn.encrypt_password(b"psycopg2", b"ashesh")
    assert enc == b"md594839d658c28a357126f105b9cb14cfc"

    res = pgconn.exec_(b"set password_encryption to 'scram-sha-256'")
    assert res.status == pq.ExecStatus.COMMAND_OK
    enc = pgconn.encrypt_password(b"psycopg2", b"ashesh")
    assert enc.startswith(b"SCRAM-SHA-256$")


@pytest.mark.libpq(">= 10")
def test_encrypt_password_closed(pgconn):
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        assert pgconn.encrypt_password(b"psycopg2", b"ashesh")


@pytest.mark.libpq("< 10")
def test_encrypt_password_not_supported(pgconn):
    # it might even be supported, but not worth the lifetime
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.encrypt_password(b"psycopg2", b"ashesh", b"md5")

    with pytest.raises(psycopg.NotSupportedError):
        pgconn.encrypt_password(b"psycopg2", b"ashesh", b"scram-sha-256")


def test_str(pgconn, dsn):
    assert "[IDLE]" in str(pgconn)
    pgconn.finish()
    assert "[BAD]" in str(pgconn)

    pgconn2 = pq.PGconn.connect_start(dsn.encode())
    assert "[" in str(pgconn2)
    assert "[IDLE]" not in str(pgconn2)
