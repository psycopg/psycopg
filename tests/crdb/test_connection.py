import psycopg.crdb
from psycopg import errors as e
from psycopg.crdb import CrdbConnection

import pytest

pytestmark = pytest.mark.crdb


def test_is_crdb(conn):
    assert CrdbConnection.is_crdb(conn)
    assert CrdbConnection.is_crdb(conn.pgconn)


def test_connect(dsn):
    with CrdbConnection.connect(dsn) as conn:
        assert isinstance(conn, CrdbConnection)

    with psycopg.crdb.connect(dsn) as conn:
        assert isinstance(conn, CrdbConnection)


def test_xid(dsn):
    with CrdbConnection.connect(dsn) as conn:
        with pytest.raises(e.NotSupportedError):
            conn.xid(1, "gtrid", "bqual")


def test_tpc_begin(dsn):
    with CrdbConnection.connect(dsn) as conn:
        with pytest.raises(e.NotSupportedError):
            conn.tpc_begin("foo")


def test_tpc_recover(dsn):
    with CrdbConnection.connect(dsn) as conn:
        with pytest.raises(e.NotSupportedError):
            conn.tpc_recover()
