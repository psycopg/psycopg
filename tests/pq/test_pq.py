from psycopg import pq


def test_version():
    rv = pq.version()
    assert rv > 90500
    assert rv < 200000  # you are good for a while
