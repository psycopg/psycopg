from psycopg.pq import TransactionStatus
from psycopg.crdb import CrdbConnection

import pytest

pytestmark = pytest.mark.crdb("skip")


def test_is_crdb(conn):
    assert not CrdbConnection.is_crdb(conn)
    assert not CrdbConnection.is_crdb(conn.pgconn)


def test_tpc_on_pg_connection(conn, tpc):
    xid = conn.xid(1, "gtrid", "bqual")
    assert conn.info.transaction_status == TransactionStatus.IDLE

    conn.tpc_begin(xid)
    assert conn.info.transaction_status == TransactionStatus.INTRANS

    cur = conn.cursor()
    cur.execute("insert into test_tpc values ('test_tpc_commit')")
    assert tpc.count_xacts() == 0
    assert tpc.count_test_records() == 0

    conn.tpc_prepare()
    assert conn.info.transaction_status == TransactionStatus.IDLE
    assert tpc.count_xacts() == 1
    assert tpc.count_test_records() == 0

    conn.tpc_commit()
    assert conn.info.transaction_status == TransactionStatus.IDLE
    assert tpc.count_xacts() == 0
    assert tpc.count_test_records() == 1
