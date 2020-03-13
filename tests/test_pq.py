from select import select

import pytest

from psycopg3.pq_enums import ConnStatus, PostgresPollingStatus


def test_connectdb(pq, dsn):
    conn = pq.PGconn.connectdb(dsn)
    assert conn.status == ConnStatus.CONNECTION_OK, conn.error_message


def test_connectdb_bytes(pq, dsn):
    conn = pq.PGconn.connectdb(dsn.encode("utf8"))
    assert conn.status == ConnStatus.CONNECTION_OK, conn.error_message


def test_connectdb_error(pq):
    conn = pq.PGconn.connectdb("dbname=psycopg3_test_not_for_real")
    assert conn.status == ConnStatus.CONNECTION_BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_connectdb_badtype(pq, baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connectdb(baddsn)


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
