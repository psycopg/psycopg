import pytest

import psycopg
from psycopg.pq import TransactionStatus

pytestmark = pytest.mark.crdb_skip("2-phase commit")


def test_tpc_disabled(conn, pipeline):
    val = int(conn.execute("show max_prepared_transactions").fetchone()[0])
    if val:
        pytest.skip("prepared transactions enabled")

    conn.rollback()
    conn.tpc_begin("x")
    with pytest.raises(psycopg.NotSupportedError):
        conn.tpc_prepare()


class TestTPC:
    def test_tpc_commit(self, conn, tpc):
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

    def test_tpc_commit_one_phase(self, conn, tpc):
        xid = conn.xid(1, "gtrid", "bqual")
        assert conn.info.transaction_status == TransactionStatus.IDLE

        conn.tpc_begin(xid)
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        cur = conn.cursor()
        cur.execute("insert into test_tpc values ('test_tpc_commit_1p')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        conn.tpc_commit()
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 1

    def test_tpc_commit_recovered(self, conn_cls, conn, dsn, tpc):
        xid = conn.xid(1, "gtrid", "bqual")
        assert conn.info.transaction_status == TransactionStatus.IDLE

        conn.tpc_begin(xid)
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        cur = conn.cursor()
        cur.execute("insert into test_tpc values ('test_tpc_commit_rec')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        conn.tpc_prepare()
        conn.close()
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        with conn_cls.connect(dsn) as conn:
            xid = conn.xid(1, "gtrid", "bqual")
            conn.tpc_commit(xid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 1

    def test_tpc_rollback(self, conn, tpc):
        xid = conn.xid(1, "gtrid", "bqual")
        assert conn.info.transaction_status == TransactionStatus.IDLE

        conn.tpc_begin(xid)
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        cur = conn.cursor()
        cur.execute("insert into test_tpc values ('test_tpc_rollback')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        conn.tpc_prepare()
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        conn.tpc_rollback()
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

    def test_tpc_rollback_one_phase(self, conn, tpc):
        xid = conn.xid(1, "gtrid", "bqual")
        assert conn.info.transaction_status == TransactionStatus.IDLE

        conn.tpc_begin(xid)
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        cur = conn.cursor()
        cur.execute("insert into test_tpc values ('test_tpc_rollback_1p')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        conn.tpc_rollback()
        assert conn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

    def test_tpc_rollback_recovered(self, conn_cls, conn, dsn, tpc):
        xid = conn.xid(1, "gtrid", "bqual")
        assert conn.info.transaction_status == TransactionStatus.IDLE

        conn.tpc_begin(xid)
        assert conn.info.transaction_status == TransactionStatus.INTRANS

        cur = conn.cursor()
        cur.execute("insert into test_tpc values ('test_tpc_commit_rec')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        conn.tpc_prepare()
        conn.close()
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        with conn_cls.connect(dsn) as conn:
            xid = conn.xid(1, "gtrid", "bqual")
            conn.tpc_rollback(xid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

    def test_status_after_recover(self, conn, tpc):
        assert conn.info.transaction_status == TransactionStatus.IDLE
        conn.tpc_recover()
        assert conn.info.transaction_status == TransactionStatus.IDLE

        cur = conn.cursor()
        cur.execute("select 1")
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        conn.tpc_recover()
        assert conn.info.transaction_status == TransactionStatus.INTRANS

    def test_recovered_xids(self, conn, tpc):
        # insert a few test xns
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("begin; prepare transaction '1-foo'")
        cur.execute("begin; prepare transaction '2-bar'")

        # read the values to return
        cur.execute(
            """
            select gid, prepared, owner, database from pg_prepared_xacts
            where database = %s
            """,
            (conn.info.dbname,),
        )
        okvals = cur.fetchall()
        okvals.sort()

        xids = conn.tpc_recover()
        xids = [xid for xid in xids if xid.database == conn.info.dbname]
        xids.sort(key=lambda x: x.gtrid)

        # check the values returned
        assert len(okvals) == len(xids)
        for (xid, (gid, prepared, owner, database)) in zip(xids, okvals):
            assert xid.gtrid == gid
            assert xid.prepared == prepared
            assert xid.owner == owner
            assert xid.database == database

    def test_xid_encoding(self, conn, tpc):
        xid = conn.xid(42, "gtrid", "bqual")
        conn.tpc_begin(xid)
        conn.tpc_prepare()

        cur = conn.cursor()
        cur.execute(
            "select gid from pg_prepared_xacts where database = %s",
            (conn.info.dbname,),
        )
        assert "42_Z3RyaWQ=_YnF1YWw=" == cur.fetchone()[0]

    @pytest.mark.parametrize(
        "fid, gtrid, bqual",
        [
            (0, "", ""),
            (42, "gtrid", "bqual"),
            (0x7FFFFFFF, "x" * 64, "y" * 64),
        ],
    )
    def test_xid_roundtrip(self, conn_cls, conn, dsn, tpc, fid, gtrid, bqual):
        xid = conn.xid(fid, gtrid, bqual)
        conn.tpc_begin(xid)
        conn.tpc_prepare()
        conn.close()

        with conn_cls.connect(dsn) as conn:
            xids = [x for x in conn.tpc_recover() if x.database == conn.info.dbname]

            assert len(xids) == 1
            xid = xids[0]
            conn.tpc_rollback(xid)

        assert xid.format_id == fid
        assert xid.gtrid == gtrid
        assert xid.bqual == bqual

    @pytest.mark.parametrize(
        "tid",
        [
            "",
            "hello, world!",
            "x" * 199,  # PostgreSQL's limit in transaction id length
        ],
    )
    def test_unparsed_roundtrip(self, conn_cls, conn, dsn, tpc, tid):
        conn.tpc_begin(tid)
        conn.tpc_prepare()
        conn.close()

        with conn_cls.connect(dsn) as conn:
            xids = [x for x in conn.tpc_recover() if x.database == conn.info.dbname]

            assert len(xids) == 1
            xid = xids[0]
            conn.tpc_rollback(xid)

        assert xid.format_id is None
        assert xid.gtrid == tid
        assert xid.bqual is None

    def test_xid_unicode(self, conn_cls, conn, dsn, tpc):
        x1 = conn.xid(10, "uni", "code")
        conn.tpc_begin(x1)
        conn.tpc_prepare()
        conn.close()

        with conn_cls.connect(dsn) as conn:
            xid = [x for x in conn.tpc_recover() if x.database == conn.info.dbname][0]
        assert 10 == xid.format_id
        assert "uni" == xid.gtrid
        assert "code" == xid.bqual

    def test_xid_unicode_unparsed(self, conn_cls, conn, dsn, tpc):
        # We don't expect people shooting snowmen as transaction ids,
        # so if something explodes in an encode error I don't mind.
        # Let's just check unicode is accepted as type.
        conn.execute("set client_encoding to utf8")
        conn.commit()

        conn.tpc_begin("transaction-id")
        conn.tpc_prepare()
        conn.close()

        with conn_cls.connect(dsn) as conn:
            xid = [x for x in conn.tpc_recover() if x.database == conn.info.dbname][0]

        assert xid.format_id is None
        assert xid.gtrid == "transaction-id"
        assert xid.bqual is None

    def test_cancel_fails_prepared(self, conn, tpc):
        conn.tpc_begin("cancel")
        conn.tpc_prepare()
        with pytest.raises(psycopg.ProgrammingError):
            conn.cancel()

    def test_tpc_recover_non_dbapi_connection(self, conn_cls, conn, dsn, tpc):
        conn.row_factory = psycopg.rows.dict_row
        conn.tpc_begin("dict-connection")
        conn.tpc_prepare()
        conn.close()

        with conn_cls.connect(dsn) as conn:
            xids = conn.tpc_recover()
            xid = [x for x in xids if x.database == conn.info.dbname][0]

        assert xid.format_id is None
        assert xid.gtrid == "dict-connection"
        assert xid.bqual is None


class TestXidObject:
    def test_xid_construction(self):
        x1 = psycopg.Xid(74, "foo", "bar")
        74 == x1.format_id
        "foo" == x1.gtrid
        "bar" == x1.bqual

    def test_xid_from_string(self):
        x2 = psycopg.Xid.from_string("42_Z3RyaWQ=_YnF1YWw=")
        42 == x2.format_id
        "gtrid" == x2.gtrid
        "bqual" == x2.bqual

        x3 = psycopg.Xid.from_string("99_xxx_yyy")
        None is x3.format_id
        "99_xxx_yyy" == x3.gtrid
        None is x3.bqual

    def test_xid_to_string(self):
        x1 = psycopg.Xid.from_string("42_Z3RyaWQ=_YnF1YWw=")
        str(x1) == "42_Z3RyaWQ=_YnF1YWw="

        x2 = psycopg.Xid.from_string("99_xxx_yyy")
        str(x2) == "99_xxx_yyy"
