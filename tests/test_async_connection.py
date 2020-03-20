import pytest

import psycopg3
from psycopg3 import AsyncConnection


def test_connect(pq, dsn, loop):
    conn = loop.run_until_complete(AsyncConnection.connect(dsn))
    assert conn.pgconn.status == pq.ConnStatus.CONNECTION_OK


def test_connect_bad(loop):
    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(AsyncConnection.connect("dbname=nosuchdb"))
