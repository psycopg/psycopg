import psycopg.crdb
from psycopg import errors as e
from psycopg.crdb import AsyncCrdbConnection

import pytest

pytestmark = [pytest.mark.crdb, pytest.mark.asyncio]


async def test_is_crdb(aconn):
    assert AsyncCrdbConnection.is_crdb(aconn)
    assert AsyncCrdbConnection.is_crdb(aconn.pgconn)


async def test_connect(dsn):
    async with await AsyncCrdbConnection.connect(dsn) as conn:
        assert isinstance(conn, psycopg.crdb.AsyncCrdbConnection)


async def test_xid(dsn):
    async with await AsyncCrdbConnection.connect(dsn) as conn:
        with pytest.raises(e.NotSupportedError):
            conn.xid(1, "gtrid", "bqual")


async def test_tpc_begin(dsn):
    async with await AsyncCrdbConnection.connect(dsn) as conn:
        with pytest.raises(e.NotSupportedError):
            await conn.tpc_begin("foo")


async def test_tpc_recover(dsn):
    async with await AsyncCrdbConnection.connect(dsn) as conn:
        with pytest.raises(e.NotSupportedError):
            await conn.tpc_recover()
