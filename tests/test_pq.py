import os
from select import select

import pytest

from psycopg3.pq_enums import ConnStatus, PostgresPollingStatus, PGPing


def test_connectdb(pq, dsn):
    conn = pq.PGconn.connect(dsn)
    assert conn.status == ConnStatus.CONNECTION_OK, conn.error_message


def test_connectdb_bytes(pq, dsn):
    conn = pq.PGconn.connect(dsn.encode("utf8"))
    assert conn.status == ConnStatus.CONNECTION_OK, conn.error_message


def test_connectdb_error(pq):
    conn = pq.PGconn.connect("dbname=psycopg3_test_not_for_real")
    assert conn.status == ConnStatus.CONNECTION_BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_connectdb_badtype(pq, baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connect(baddsn)


def test_connect_async(pq, dsn):
    conn = pq.PGconn.connect_start(dsn)
    while 1:
        assert conn.status != ConnStatus.CONNECTION_BAD
        rv = conn.connect_poll()
        if rv == PostgresPollingStatus.PGRES_POLLING_OK:
            break
        elif rv == PostgresPollingStatus.PGRES_POLLING_READING:
            select([conn.socket], [], [])
        elif rv == PostgresPollingStatus.PGRES_POLLING_WRITING:
            select([], [conn.socket], [])
        else:
            assert False, rv

    assert conn.status == ConnStatus.CONNECTION_OK


def test_connect_async_bad(pq, dsn):
    conn = pq.PGconn.connect_start("dbname=psycopg3_test_not_for_real")
    while 1:
        assert conn.status != ConnStatus.CONNECTION_BAD
        rv = conn.connect_poll()
        if rv == PostgresPollingStatus.PGRES_POLLING_FAILED:
            break
        elif rv == PostgresPollingStatus.PGRES_POLLING_READING:
            select([conn.socket], [], [])
        elif rv == PostgresPollingStatus.PGRES_POLLING_WRITING:
            select([], [conn.socket], [])
        else:
            assert False, rv

    assert conn.status == ConnStatus.CONNECTION_BAD


def test_defaults(pq):
    oldport = os.environ.get("PGPORT")
    try:
        os.environ["PGPORT"] = "15432"
        defs = pq.PGconn.get_defaults()
    finally:
        if oldport is not None:
            os.environ["PGPORT"] = oldport
        else:
            del os.environ["PGPORT"]

    assert len(defs) > 20
    port = [d for d in defs if d.keyword == "port"][0]
    assert port.envvar == "PGPORT"
    assert port.compiled == "5432"
    assert port.val == "15432"
    assert port.label == "Database-Port"
    assert port.dispatcher == ""
    assert port.dispsize == 6


def test_info(pq, dsn):
    conn = pq.PGconn.connect(dsn)
    info = conn.info
    assert len(info) > 20
    dbname = [d for d in info if d.keyword == "dbname"][0]
    assert dbname.envvar == "PGDATABASE"
    assert dbname.val == "psycopg3_test"  # TODO: parse from dsn
    assert dbname.label == "Database-Name"
    assert dbname.dispatcher == ""
    assert dbname.dispsize == 20


def test_conninfo_parse(pq):
    info = pq.PGconn.parse_conninfo(
        "postgresql://host1:123,host2:456/somedb"
        "?target_session_attrs=any&application_name=myapp"
    )
    info = {i.keyword: i.val for i in info if i.val is not None}
    assert info["host"] == "host1,host2"
    assert info["port"] == "123,456"
    assert info["dbname"] == "somedb"
    assert info["application_name"] == "myapp"


def test_conninfo_parse_bad(pq):
    with pytest.raises(pq.PQerror) as e:
        pq.PGconn.parse_conninfo("bad_conninfo=")
        assert "bad_conninfo" in str(e.value)


def test_reset(pq, dsn):
    conn = pq.PGconn.connect(dsn)
    assert conn.status == ConnStatus.CONNECTION_OK
    # TODO: break it
    conn.reset()
    assert conn.status == ConnStatus.CONNECTION_OK


def test_reset_async(pq, dsn):
    conn = pq.PGconn.connect(dsn)
    assert conn.status == ConnStatus.CONNECTION_OK
    # TODO: break it
    conn.reset_start()
    while 1:
        rv = conn.connect_poll()
        if rv == PostgresPollingStatus.PGRES_POLLING_READING:
            select([conn.socket], [], [])
        elif rv == PostgresPollingStatus.PGRES_POLLING_WRITING:
            select([], [conn.socket], [])
        else:
            break

    assert rv == PostgresPollingStatus.PGRES_POLLING_OK
    assert conn.status == ConnStatus.CONNECTION_OK


def test_ping(pq, dsn):
    rv = pq.PGconn.ping(dsn)
    assert rv == PGPing.PQPING_OK

    rv = pq.PGconn.ping("port=99999")
    assert rv == PGPing.PQPING_NO_RESPONSE
