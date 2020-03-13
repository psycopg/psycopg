import pytest


def test_PQconnectdb(pq, dsn):
    conn = pq.PGconn.connectdb(dsn)
    assert conn.status == pq.ConnStatus.CONNECTION_OK, conn.error_message


def test_PQconnectdb_bytes(pq, dsn):
    conn = pq.PGconn.connectdb(dsn.encode("utf8"))
    assert conn.status == pq.ConnStatus.CONNECTION_OK, conn.error_message


def test_PQconnectdb_error(pq):
    conn = pq.PGconn.connectdb("dbname=psycopg3_test_not_for_real")
    assert conn.status == pq.ConnStatus.CONNECTION_BAD


@pytest.mark.parametrize("baddsn", [None, 42])
def test_PQconnectdb_badtype(pq, baddsn):
    with pytest.raises(TypeError):
        pq.PGconn.connectdb(baddsn)
