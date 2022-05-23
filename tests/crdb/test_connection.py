from psycopg.crdb import CrdbConnection

import pytest

pytestmark = pytest.mark.crdb


def test_is_crdb(conn):
    assert CrdbConnection.is_crdb(conn)
    assert CrdbConnection.is_crdb(conn.pgconn)
