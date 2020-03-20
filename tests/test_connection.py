import pytest

import psycopg3
from psycopg3 import Connection


def test_connect(pq, dsn):
    conn = Connection.connect(dsn)
    assert conn.pgconn.status == pq.ConnStatus.CONNECTION_OK


def test_connect_bad():
    with pytest.raises(psycopg3.OperationalError):
        Connection.connect("dbname=nosuchdb")
