import psycopg.crdb
from psycopg.crdb import AsyncCrdbConnection

import pytest

pytestmark = [pytest.mark.crdb, pytest.mark.asyncio]


async def test_is_crdb(aconn):
    assert AsyncCrdbConnection.is_crdb(aconn)
    assert AsyncCrdbConnection.is_crdb(aconn.pgconn)


async def test_connect(dsn):
    async with await psycopg.crdb.AsyncCrdbConnection.connect(dsn) as conn:
        assert isinstance(conn, psycopg.crdb.AsyncCrdbConnection)
