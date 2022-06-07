import pytest

import psycopg
from psycopg.pq import TransactionStatus

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.crdb_skip("2-phase commit"),
]


async def test_tpc_disabled(aconn, apipeline):
    cur = await aconn.execute("show max_prepared_transactions")
    val = int((await cur.fetchone())[0])
    if val:
        pytest.skip("prepared transactions enabled")

    await aconn.rollback()
    await aconn.tpc_begin("x")
    with pytest.raises(psycopg.NotSupportedError):
        await aconn.tpc_prepare()


class TestTPC:
    async def test_tpc_commit(self, aconn, tpc):
        xid = aconn.xid(1, "gtrid", "bqual")
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        await aconn.tpc_begin(xid)
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

        cur = aconn.cursor()
        await cur.execute("insert into test_tpc values ('test_tpc_commit')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        await aconn.tpc_prepare()
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        await aconn.tpc_commit()
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 1

    async def test_tpc_commit_one_phase(self, aconn, tpc):
        xid = aconn.xid(1, "gtrid", "bqual")
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        await aconn.tpc_begin(xid)
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

        cur = aconn.cursor()
        await cur.execute("insert into test_tpc values ('test_tpc_commit_1p')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        await aconn.tpc_commit()
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 1

    async def test_tpc_commit_recovered(self, aconn_cls, aconn, dsn, tpc):
        xid = aconn.xid(1, "gtrid", "bqual")
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        await aconn.tpc_begin(xid)
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

        cur = aconn.cursor()
        await cur.execute("insert into test_tpc values ('test_tpc_commit_rec')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        await aconn.tpc_prepare()
        await aconn.close()
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        async with await aconn_cls.connect(dsn) as aconn:
            xid = aconn.xid(1, "gtrid", "bqual")
            await aconn.tpc_commit(xid)
            assert aconn.info.transaction_status == TransactionStatus.IDLE

        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 1

    async def test_tpc_rollback(self, aconn, tpc):
        xid = aconn.xid(1, "gtrid", "bqual")
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        await aconn.tpc_begin(xid)
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

        cur = aconn.cursor()
        await cur.execute("insert into test_tpc values ('test_tpc_rollback')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        await aconn.tpc_prepare()
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        await aconn.tpc_rollback()
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

    async def test_tpc_rollback_one_phase(self, aconn, tpc):
        xid = aconn.xid(1, "gtrid", "bqual")
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        await aconn.tpc_begin(xid)
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

        cur = aconn.cursor()
        await cur.execute("insert into test_tpc values ('test_tpc_rollback_1p')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        await aconn.tpc_rollback()
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

    async def test_tpc_rollback_recovered(self, aconn_cls, aconn, dsn, tpc):
        xid = aconn.xid(1, "gtrid", "bqual")
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        await aconn.tpc_begin(xid)
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

        cur = aconn.cursor()
        await cur.execute("insert into test_tpc values ('test_tpc_commit_rec')")
        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

        await aconn.tpc_prepare()
        await aconn.close()
        assert tpc.count_xacts() == 1
        assert tpc.count_test_records() == 0

        async with await aconn_cls.connect(dsn) as aconn:
            xid = aconn.xid(1, "gtrid", "bqual")
            await aconn.tpc_rollback(xid)
            assert aconn.info.transaction_status == TransactionStatus.IDLE

        assert tpc.count_xacts() == 0
        assert tpc.count_test_records() == 0

    async def test_status_after_recover(self, aconn, tpc):
        assert aconn.info.transaction_status == TransactionStatus.IDLE
        await aconn.tpc_recover()
        assert aconn.info.transaction_status == TransactionStatus.IDLE

        cur = aconn.cursor()
        await cur.execute("select 1")
        assert aconn.info.transaction_status == TransactionStatus.INTRANS
        await aconn.tpc_recover()
        assert aconn.info.transaction_status == TransactionStatus.INTRANS

    async def test_recovered_xids(self, aconn, tpc):
        # insert a few test xns
        await aconn.set_autocommit(True)
        cur = aconn.cursor()
        await cur.execute("begin; prepare transaction '1-foo'")
        await cur.execute("begin; prepare transaction '2-bar'")

        # read the values to return
        await cur.execute(
            """
            select gid, prepared, owner, database from pg_prepared_xacts
            where database = %s
            """,
            (aconn.info.dbname,),
        )
        okvals = await cur.fetchall()
        okvals.sort()

        xids = await aconn.tpc_recover()
        xids = [xid for xid in xids if xid.database == aconn.info.dbname]
        xids.sort(key=lambda x: x.gtrid)

        # check the values returned
        assert len(okvals) == len(xids)
        for (xid, (gid, prepared, owner, database)) in zip(xids, okvals):
            assert xid.gtrid == gid
            assert xid.prepared == prepared
            assert xid.owner == owner
            assert xid.database == database

    async def test_xid_encoding(self, aconn, tpc):
        xid = aconn.xid(42, "gtrid", "bqual")
        await aconn.tpc_begin(xid)
        await aconn.tpc_prepare()

        cur = aconn.cursor()
        await cur.execute(
            "select gid from pg_prepared_xacts where database = %s",
            (aconn.info.dbname,),
        )
        assert "42_Z3RyaWQ=_YnF1YWw=" == (await cur.fetchone())[0]

    @pytest.mark.parametrize(
        "fid, gtrid, bqual",
        [
            (0, "", ""),
            (42, "gtrid", "bqual"),
            (0x7FFFFFFF, "x" * 64, "y" * 64),
        ],
    )
    async def test_xid_roundtrip(self, aconn_cls, aconn, dsn, tpc, fid, gtrid, bqual):
        xid = aconn.xid(fid, gtrid, bqual)
        await aconn.tpc_begin(xid)
        await aconn.tpc_prepare()
        await aconn.close()

        async with await aconn_cls.connect(dsn) as aconn:
            xids = [
                x for x in await aconn.tpc_recover() if x.database == aconn.info.dbname
            ]
            assert len(xids) == 1
            xid = xids[0]
            await aconn.tpc_rollback(xid)

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
    async def test_unparsed_roundtrip(self, aconn_cls, aconn, dsn, tpc, tid):
        await aconn.tpc_begin(tid)
        await aconn.tpc_prepare()
        await aconn.close()

        async with await aconn_cls.connect(dsn) as aconn:
            xids = [
                x for x in await aconn.tpc_recover() if x.database == aconn.info.dbname
            ]
            assert len(xids) == 1
            xid = xids[0]
            await aconn.tpc_rollback(xid)

        assert xid.format_id is None
        assert xid.gtrid == tid
        assert xid.bqual is None

    async def test_xid_unicode(self, aconn_cls, aconn, dsn, tpc):
        x1 = aconn.xid(10, "uni", "code")
        await aconn.tpc_begin(x1)
        await aconn.tpc_prepare()
        await aconn.close()

        async with await aconn_cls.connect(dsn) as aconn:
            xid = [
                x for x in await aconn.tpc_recover() if x.database == aconn.info.dbname
            ][0]

        assert 10 == xid.format_id
        assert "uni" == xid.gtrid
        assert "code" == xid.bqual

    async def test_xid_unicode_unparsed(self, aconn_cls, aconn, dsn, tpc):
        # We don't expect people shooting snowmen as transaction ids,
        # so if something explodes in an encode error I don't mind.
        # Let's just check unicode is accepted as type.
        await aconn.execute("set client_encoding to utf8")
        await aconn.commit()

        await aconn.tpc_begin("transaction-id")
        await aconn.tpc_prepare()
        await aconn.close()

        async with await aconn_cls.connect(dsn) as aconn:
            xid = [
                x for x in await aconn.tpc_recover() if x.database == aconn.info.dbname
            ][0]

        assert xid.format_id is None
        assert xid.gtrid == "transaction-id"
        assert xid.bqual is None

    async def test_cancel_fails_prepared(self, aconn, tpc):
        await aconn.tpc_begin("cancel")
        await aconn.tpc_prepare()
        with pytest.raises(psycopg.ProgrammingError):
            aconn.cancel()

    async def test_tpc_recover_non_dbapi_connection(self, aconn_cls, aconn, dsn, tpc):
        aconn.row_factory = psycopg.rows.dict_row
        await aconn.tpc_begin("dict-connection")
        await aconn.tpc_prepare()
        await aconn.close()

        async with await aconn_cls.connect(dsn) as aconn:
            xids = await aconn.tpc_recover()
            xid = [x for x in xids if x.database == aconn.info.dbname][0]

        assert xid.format_id is None
        assert xid.gtrid == "dict-connection"
        assert xid.bqual is None
