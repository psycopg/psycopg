from psycopg3 import pool


def test_pool(dsn):
    p = pool.ConnectionPool(dsn, minconn=2, timeout_sec=1.0)
    with p.connection() as conn:
        with conn.execute("select pg_backend_pid()") as cur:
            (pid1,) = cur.fetchone()

        with p.connection() as conn2:
            with conn2.execute("select pg_backend_pid()") as cur:
                (pid2,) = cur.fetchone()

    with p.connection() as conn:
        assert conn.pgconn.backend_pid in (pid1, pid2)
