from __future__ import annotations

import os
import sys
import time
import ctypes
import logging
import weakref
import contextlib
from select import select
from typing import TYPE_CHECKING
from functools import partial
from collections.abc import Iterator

import pytest

import psycopg
import psycopg.generators
from psycopg import pq
from psycopg.conninfo import make_conninfo

from ..fix_crdb import crdb_anydb

if TYPE_CHECKING:
    from psycopg.pq.abc import PGcancelConn, PGconn


def wait(
    conn: PGconn | PGcancelConn,
    poll_method: str = "connect_poll",
    return_on: pq.PollingStatus = pq.PollingStatus.OK,
    timeout: int | None = None,
) -> None:
    poll = getattr(conn, poll_method)
    while True:
        assert conn.status != pq.ConnStatus.BAD, conn.error_message

        if (rv := poll()) == return_on:
            return
        elif rv == pq.PollingStatus.READING:
            select([conn.socket], [], [], timeout)
        elif rv == pq.PollingStatus.WRITING:
            select([], [conn.socket], [], timeout)
        else:
            pytest.fail(f"unexpected poll result: {rv}")
    assert (
        conn.status == pq.ConnStatus.OK
    ), f"unexpected connection status: {conn.error_message}"


def test_connectdb(dsn):
    conn = pq.PGconn.connect(dsn.encode())
    assert conn.status == pq.ConnStatus.OK, conn.error_message


@crdb_anydb
def test_connectdb_error(dsn):
    conn = pq.PGconn.connect(make_conninfo(dsn, dbname="nosuchdb").encode())
    assert conn.status == pq.ConnStatus.BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_connectdb_badtype(baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connect(baddsn)


def test_connect_async(dsn):
    conn = pq.PGconn.connect_start(dsn.encode())
    conn.nonblocking = 1
    wait(conn)
    conn.finish()
    with pytest.raises(psycopg.OperationalError):
        conn.connect_poll()


@pytest.mark.crdb("skip", reason="connects to any db name")
def test_connect_async_bad(dsn):
    parsed_dsn = {e.keyword: e.val for e in pq.Conninfo.parse(dsn.encode()) if e.val}
    parsed_dsn[b"dbname"] = b"psycopg_test_not_for_real"
    dsn = b" ".join(b"%s='%s'" % item for item in parsed_dsn.items())
    conn = pq.PGconn.connect_start(dsn)
    wait(conn, return_on=pq.PollingStatus.FAILED)
    assert conn.status == pq.ConnStatus.BAD


def test_finish(pgconn):
    assert pgconn.status == pq.ConnStatus.OK
    pgconn.finish()
    assert pgconn.status == pq.ConnStatus.BAD
    pgconn.finish()
    assert pgconn.status == pq.ConnStatus.BAD


@pytest.mark.slow
def test_weakref(dsn, gc_collect):
    conn = pq.PGconn.connect(dsn.encode())
    w = weakref.ref(conn)
    conn.finish()
    del conn
    gc_collect()
    assert w() is None


@pytest.mark.skipif(
    sys.platform == "win32"
    and os.environ.get("CI") == "true"
    and pq.__impl__ != "python",
    reason="can't figure out how to make ctypes run, don't care",
)
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


@pytest.mark.crdb_skip("pg_terminate_backend")
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


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_reset_async(pgconn):
    assert pgconn.status == pq.ConnStatus.OK
    pgconn.exec_(b"select pg_terminate_backend(pg_backend_pid())")
    assert pgconn.status == pq.ConnStatus.BAD
    pgconn.reset_start()
    wait(pgconn, "reset_poll")

    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.reset_start()

    with pytest.raises(psycopg.OperationalError):
        pgconn.reset_poll()


def test_ping(dsn):
    rv = pq.PGconn.ping(dsn.encode())
    assert rv == pq.Ping.OK

    rv = pq.PGconn.ping(make_conninfo(dsn, port=9999, connect_timeout=3).encode())
    assert rv == pq.Ping.NO_RESPONSE


def test_db(pgconn):
    name = [o.val for o in pgconn.info if o.keyword == b"dbname"][0]
    assert pgconn.db == name
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.db


@pytest.mark.libpq(">= 18")
def test_service(pgconn):
    assert isinstance(pgconn.service, bytes)


@pytest.mark.libpq("< 18")
def test_service_notimpl(pgconn):
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.service


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


@pytest.mark.crdb_skip("encoding")
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


@pytest.mark.libpq(">= 18")
def test_full_protocol_version(pgconn):
    assert pgconn.full_protocol_version >= 30000
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        pgconn.full_protocol_version


@pytest.mark.libpq("< 18")
def test_full_protocol_version_notimpl(pgconn):
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.full_protocol_version


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


def test_get_error_message(pgconn):
    assert pgconn.get_error_message() == "no error details available"
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.FATAL_ERROR
    msg = pgconn.get_error_message()
    assert "wat" in msg
    pgconn.finish()
    assert "NULL" in pgconn.get_error_message()


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

    if "PGPASSWORD" in os.environ:
        assert pgconn.used_password
    if [i for i in info if i.keyword == b"password"][0].val is not None:
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


@pytest.mark.libpq(">= 17")
def test_set_chunked_rows_mode(pgconn):
    with pytest.raises(psycopg.OperationalError):
        pgconn.set_chunked_rows_mode(42)

    pgconn.send_query(b"select 1")
    pgconn.set_chunked_rows_mode(42)


@contextlib.contextmanager
def cancellable_query(pgconn: PGconn) -> Iterator[None]:
    dsn = b" ".join(b"%s='%s'" % (i.keyword, i.val) for i in pgconn.info if i.val)
    monitor_conn = pq.PGconn.connect(dsn)
    assert (
        monitor_conn.status == pq.ConnStatus.OK
    ), f"bad connection: {monitor_conn.get_error_message()}"

    pgconn.send_query_params(b"SELECT pg_sleep($1)", [b"10"])

    while True:
        r = monitor_conn.exec_(
            b"SELECT count(*) FROM pg_stat_activity"
            b" WHERE query = 'SELECT pg_sleep($1)'"
            b" AND state = 'active'"
        )
        assert r.status == pq.ExecStatus.TUPLES_OK
        if r.get_value(0, 0) != b"0":
            del monitor_conn
            break

        time.sleep(0.01)

    yield None

    res = pgconn.get_result()
    assert res is not None
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert res.error_field(pq.DiagnosticField.SQLSTATE) == b"57014"
    while pgconn.is_busy():
        pgconn.consume_input()


@pytest.mark.libpq(">= 17")
@pytest.mark.crdb("skip", reason="test hang - TODO investigate")
def test_cancel_conn_blocking(pgconn):
    # test PQcancelBlocking, similarly to test_cancel() from
    # src/test/modules/libpq_pipeline/libpq_pipeline.c
    pgconn.nonblocking = 1

    with cancellable_query(pgconn):
        cancel_conn = pgconn.cancel_conn()
        assert cancel_conn.status == pq.ConnStatus.ALLOCATED
        cancel_conn.blocking()
        assert cancel_conn.status == pq.ConnStatus.OK

    # test PQcancelReset works on the cancel connection and it can be reused
    # after
    cancel_conn.reset()
    with cancellable_query(pgconn):
        cancel_conn.blocking()
        assert cancel_conn.status == pq.ConnStatus.OK


@pytest.mark.libpq(">= 17")
@pytest.mark.crdb("skip", reason="test *might* hang - TODO investigate")
def test_cancel_conn_nonblocking(pgconn):
    # test PQcancelStart() and then polling with PQcancelPoll, similarly to
    # test_cancel() from src/test/modules/libpq_pipeline/libpq_pipeline.c
    pgconn.nonblocking = 1

    wait_cancel = partial(wait, poll_method="poll", timeout=3)

    # test PQcancelCreate and then polling with PQcancelPoll
    with cancellable_query(pgconn):
        cancel_conn = pgconn.cancel_conn()
        assert cancel_conn.status == pq.ConnStatus.ALLOCATED
        cancel_conn.start()
        # On network sockets, connection starts with STARTED.
        # On Unix sockets, connection starts with MADE.
        assert cancel_conn.status in (pq.ConnStatus.STARTED, pq.ConnStatus.MADE)
        wait_cancel(cancel_conn)
        assert cancel_conn.status == pq.ConnStatus.OK

    # test PQcancelReset works on the cancel connection and it can be reused
    # after
    cancel_conn.reset()
    with cancellable_query(pgconn):
        cancel_conn.start()
        wait_cancel(cancel_conn)
        assert cancel_conn.status == pq.ConnStatus.OK


@pytest.mark.libpq(">= 17")
def test_cancel_conn_finished(pgconn):
    cancel_conn = pgconn.cancel_conn()
    cancel_conn.reset()
    cancel_conn.finish()
    with pytest.raises(psycopg.OperationalError):
        cancel_conn.start()
    with pytest.raises(psycopg.OperationalError):
        cancel_conn.blocking()
    with pytest.raises(psycopg.OperationalError):
        cancel_conn.poll()
    with pytest.raises(psycopg.OperationalError):
        cancel_conn.reset()
    assert cancel_conn.get_error_message() == "connection pointer is NULL"


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


@pytest.mark.crdb_skip("notify")
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


@pytest.mark.crdb_skip("do")
def test_notice_nohandler(pgconn):
    pgconn.exec_(b"set client_min_messages to notice")
    res = pgconn.exec_(
        b"do $$begin raise notice 'hello notice'; end$$ language plpgsql"
    )
    assert res.status == pq.ExecStatus.COMMAND_OK


@pytest.mark.crdb_skip("do")
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


@pytest.mark.crdb_skip("do")
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


@pytest.mark.libpq("< 14")
@pytest.mark.skipif("sys.platform != 'linux'")
def test_trace_pre14(pgconn, tmp_path):
    tracef = tmp_path / "trace"
    with tracef.open("w") as f:
        pgconn.trace(f.fileno())
        with pytest.raises(psycopg.NotSupportedError):
            pgconn.set_trace_flags(0)
        pgconn.exec_(b"select 1")
        pgconn.untrace()
        pgconn.exec_(b"select 2")
    traces = tracef.read_text()
    assert "select 1" in traces
    assert "select 2" not in traces


@pytest.mark.libpq(">= 14")
@pytest.mark.skipif("sys.platform != 'linux'")
def test_trace(pgconn, tmp_path):
    tracef = tmp_path / "trace"
    with tracef.open("w") as f:
        pgconn.trace(f.fileno())
        pgconn.set_trace_flags(pq.Trace.SUPPRESS_TIMESTAMPS | pq.Trace.REGRESS_MODE)
        pgconn.exec_(b"select 1::int4 as foo")
        pgconn.untrace()
        pgconn.exec_(b"select 2::int4 as foo")
    traces = [line.split("\t") for line in tracef.read_text().splitlines()]
    assert traces == [
        ["F", "26", "Query", ' "select 1::int4 as foo"'],
        ["B", "28", "RowDescription", ' 1 "foo" NNNN 0 NNNN 4 -1 0'],
        ["B", "11", "DataRow", " 1 1 '1'"],
        ["B", "13", "CommandComplete", ' "SELECT 1"'],
        ["B", "5", "ReadyForQuery", " I"],
    ]


@pytest.mark.skipif("sys.platform == 'linux'")
def test_trace_nonlinux(pgconn):
    with pytest.raises(psycopg.NotSupportedError):
        pgconn.trace(1)


@pytest.mark.libpq(">= 17")
def test_change_password_error(pgconn):
    with pytest.raises(
        psycopg.OperationalError, match='role(/user)? "ashesh" does not exist'
    ):
        pgconn.change_password(b"ashesh", b"psycopg")


@pytest.fixture
def role(pgconn: PGconn) -> Iterator[tuple[bytes, bytes]]:
    user, passwd = "ashesh", "psycopg2"
    r = pgconn.exec_(f"CREATE USER {user} LOGIN PASSWORD '{passwd}'".encode())
    if r.status != pq.ExecStatus.COMMAND_OK:
        pytest.skip(f"cannot create a PostgreSQL role: {r.get_error_message()}")
    yield user.encode(), passwd.encode()
    r = pgconn.exec_(f"DROP USER {user}".encode())
    if r.status != pq.ExecStatus.COMMAND_OK:
        pytest.fail(f"failed to drop {user} role: {r.get_error_message()}")


@pytest.mark.libpq(">= 17")
def test_change_password(pgconn, dsn, role):
    user, passwd = role
    conninfo = {e.keyword: e.val for e in pq.Conninfo.parse(dsn.encode()) if e.val}
    conninfo.update({b"dbname": b"postgres", b"user": user, b"password": passwd})
    # Avoid peer authentication
    if b"host" not in conninfo:
        conninfo[b"host"] = b"localhost"
    conn = pq.PGconn.connect(b" ".join(b"%s='%s'" % item for item in conninfo.items()))
    assert conn.status == pq.ConnStatus.OK, conn.error_message

    pgconn.change_password(user, b"psycopg")
    conninfo[b"password"] = b"psycopg"
    conn = pq.PGconn.connect(b" ".join(b"%s='%s'" % item for item in conninfo.items()))
    assert conn.status == pq.ConnStatus.OK, conn.error_message


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
@pytest.mark.crdb_skip("password_encryption")
def test_encrypt_password_query(pgconn):
    res = pgconn.exec_(b"set password_encryption to 'md5'")
    assert res.status == pq.ExecStatus.COMMAND_OK, pgconn.get_error_message()
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
