import psycopg.crdb
from psycopg.crdb import CrdbConnection

import pytest

pytestmark = pytest.mark.crdb


def test_is_crdb(conn):
    assert CrdbConnection.is_crdb(conn)
    assert CrdbConnection.is_crdb(conn.pgconn)


def test_connect(dsn):
    with psycopg.crdb.connect(dsn) as conn:
        assert isinstance(conn, CrdbConnection)
