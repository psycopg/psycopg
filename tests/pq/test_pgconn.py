from select import select

import pytest


def test_connectdb(pq, dsn):
    conn = pq.PGconn.connect(dsn.encode("utf8"))
    assert conn.status == pq.ConnStatus.CONNECTION_OK, conn.error_message


def test_connectdb_error(pq):
    conn = pq.PGconn.connect(b"dbname=psycopg3_test_not_for_real")
    assert conn.status == pq.ConnStatus.CONNECTION_BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_connectdb_badtype(pq, baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connect(baddsn)


def test_connect_async(pq, dsn):
    conn = pq.PGconn.connect_start(dsn.encode("utf8"))
    conn.nonblocking = 1
    while 1:
        assert conn.status != pq.ConnStatus.CONNECTION_BAD
        rv = conn.connect_poll()
        if rv == pq.PollingStatus.PGRES_POLLING_OK:
            break
        elif rv == pq.PollingStatus.PGRES_POLLING_READING:
            select([conn.socket], [], [])
        elif rv == pq.PollingStatus.PGRES_POLLING_WRITING:
            select([], [conn.socket], [])
        else:
            assert False, rv

    assert conn.status == pq.ConnStatus.CONNECTION_OK


def test_connect_async_bad(pq, dsn):
    conn = pq.PGconn.connect_start(b"dbname=psycopg3_test_not_for_real")
    while 1:
        assert conn.status != pq.ConnStatus.CONNECTION_BAD
        rv = conn.connect_poll()
        if rv == pq.PollingStatus.PGRES_POLLING_FAILED:
            break
        elif rv == pq.PollingStatus.PGRES_POLLING_READING:
            select([conn.socket], [], [])
        elif rv == pq.PollingStatus.PGRES_POLLING_WRITING:
            select([], [conn.socket], [])
        else:
            assert False, rv

    assert conn.status == pq.ConnStatus.CONNECTION_BAD


def test_info(pq, dsn, pgconn):
    info = pgconn.info
    assert len(info) > 20
    dbname = [d for d in info if d.keyword == b"dbname"][0]
    assert dbname.envvar == b"PGDATABASE"
    assert dbname.label == b"Database-Name"
    assert dbname.dispatcher == b""
    assert dbname.dispsize == 20

    parsed = pq.Conninfo.parse(dsn.encode("utf8"))
    name = [o.val for o in parsed if o.keyword == b"dbname"][0]
    assert dbname.val == name


def test_reset(pq, pgconn):
    assert pgconn.status == pq.ConnStatus.CONNECTION_OK
    # TODO: break it
    pgconn.reset()
    assert pgconn.status == pq.ConnStatus.CONNECTION_OK


def test_reset_async(pq, pgconn):
    assert pgconn.status == pq.ConnStatus.CONNECTION_OK
    # TODO: break it
    pgconn.reset_start()
    while 1:
        rv = pgconn.connect_poll()
        if rv == pq.PollingStatus.PGRES_POLLING_READING:
            select([pgconn.socket], [], [])
        elif rv == pq.PollingStatus.PGRES_POLLING_WRITING:
            select([], [pgconn.socket], [])
        else:
            break

    assert rv == pq.PollingStatus.PGRES_POLLING_OK
    assert pgconn.status == pq.ConnStatus.CONNECTION_OK


def test_ping(pq, dsn):
    rv = pq.PGconn.ping(dsn.encode("utf8"))
    assert rv == pq.Ping.PQPING_OK

    rv = pq.PGconn.ping(b"port=99999")
    assert rv == pq.Ping.PQPING_NO_RESPONSE


def test_db(pgconn):
    name = [o.val for o in pgconn.info if o.keyword == b"dbname"][0]
    assert pgconn.db == name


def test_user(pgconn):
    user = [o.val for o in pgconn.info if o.keyword == b"user"][0]
    assert pgconn.user == user


def test_password(pgconn):
    # not in info
    assert isinstance(pgconn.password, bytes)


def test_host(pgconn):
    # might be not in info
    assert isinstance(pgconn.host, bytes)


def test_hostaddr(pgconn):
    # not in info
    assert isinstance(pgconn.hostaddr, bytes)


def test_tty(pgconn):
    tty = [o.val for o in pgconn.info if o.keyword == b"tty"][0]
    assert pgconn.tty == tty


def test_transaction_status(pq, pgconn):
    assert pgconn.transaction_status == pq.TransactionStatus.PQTRANS_IDLE
    # TODO: test other states
    pgconn.finish()
    assert pgconn.transaction_status == pq.TransactionStatus.PQTRANS_UNKNOWN


def test_parameter_status(pq, dsn, tempenv):
    tempenv["PGAPPNAME"] = "psycopg3 tests"
    pgconn = pq.PGconn.connect(dsn.encode("utf8"))
    assert pgconn.parameter_status(b"application_name") == b"psycopg3 tests"
    assert pgconn.parameter_status(b"wat") is None


def test_encoding(pq, pgconn):
    res = pgconn.exec_(b"set client_encoding to latin1")
    assert res.status == pq.ExecStatus.PGRES_COMMAND_OK
    assert pgconn.parameter_status(b"client_encoding") == b"LATIN1"

    res = pgconn.exec_(b"set client_encoding to 'utf-8'")
    assert res.status == pq.ExecStatus.PGRES_COMMAND_OK
    assert pgconn.parameter_status(b"client_encoding") == b"UTF8"

    res = pgconn.exec_(b"set client_encoding to wat")
    assert res.status == pq.ExecStatus.PGRES_FATAL_ERROR
    assert pgconn.parameter_status(b"client_encoding") == b"UTF8"


def test_protocol_version(pgconn):
    assert pgconn.protocol_version == 3


def test_server_version(pgconn):
    assert pgconn.server_version >= 90400


def test_error_message(pq, pgconn):
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.PGRES_FATAL_ERROR
    msg = pgconn.error_message
    assert b"wat" in msg


def test_backend_pid(pgconn):
    assert 2 <= pgconn.backend_pid <= 65535  # Unless increased in kernel?


def test_needs_password(pgconn):
    # assume connection worked so an eventually needed password wasn't missing
    assert pgconn.needs_password is False


def test_used_password(pq, pgconn, tempenv, dsn):
    assert isinstance(pgconn.used_password, bool)

    # Assume that if a password was passed then it was needed.
    # Note that the server may still need a password passed via pgpass
    # so it may be that has_password is false but still a password was
    # requested by the server and passed by libpq.
    info = pq.Conninfo.parse(dsn.encode("utf8"))
    has_password = (
        "PGPASSWORD" in tempenv
        or [i for i in info if i.keyword == b"password"][0].val is not None
    )
    if has_password:
        assert pgconn.used_password


def test_ssl_in_use(pgconn):
    assert isinstance(pgconn.ssl_in_use, bool)

    # If connecting via socket then ssl is not in use
    if pgconn.host.startswith(b"/"):
        assert not pgconn.ssl_in_use
    else:
        sslmode = [i.val for i in pgconn.info if i.keyword == b"sslmode"][0]
        if sslmode not in (b"disable", b"allow"):
            # 'prefer' may still connect without ssl
            # but maybe unlikely in the tests environment?
            assert pgconn.ssl_in_use


def test_make_empty_result(pq, pgconn):
    pgconn.exec_(b"wat")
    res = pgconn.make_empty_result(pq.ExecStatus.PGRES_FATAL_ERROR)
    assert res.status == pq.ExecStatus.PGRES_FATAL_ERROR
    assert b"wat" in res.error_message
