import time
import threading

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


@pytest.mark.slow
def test_broken_connection(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg.DatabaseError):
        cur.execute("cancel session (select session_id from [show session_id])")
    assert conn.closed


@pytest.mark.slow
def test_broken(conn):
    (session_id,) = conn.execute("show session_id").fetchone()
    with pytest.raises(psycopg.OperationalError):
        conn.execute("cancel session %s", [session_id])

    assert conn.closed
    assert conn.broken
    conn.close()
    assert conn.closed
    assert conn.broken


@pytest.mark.slow
def test_identify_closure(conn_cls, dsn):
    with conn_cls.connect(dsn, autocommit=True) as conn:
        with conn_cls.connect(dsn, autocommit=True) as conn2:
            (session_id,) = conn.execute("show session_id").fetchone()

            def closer():
                time.sleep(0.2)
                conn2.execute("cancel session %s", [session_id])

            t = threading.Thread(target=closer)
            t.start()
            t0 = time.time()
            try:
                with pytest.raises(psycopg.OperationalError):
                    conn.execute("select pg_sleep(3.0)")
                dt = time.time() - t0
                # CRDB seems to take not less than 1s
                assert 0.2 < dt < 2
            finally:
                t.join()
