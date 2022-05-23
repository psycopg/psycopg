from psycopg.crdb import CrdbConnection

import pytest

pytestmark = pytest.mark.crdb("skip")


def test_is_crdb(conn):
    assert not CrdbConnection.is_crdb(conn)
    assert not CrdbConnection.is_crdb(conn.pgconn)
