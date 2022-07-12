import pytest

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types import TypeInfo


@pytest.mark.parametrize("name", ["text", sql.Identifier("text")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
def test_fetch(conn, name, status):
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        conn.execute("select 1")

    assert conn.info.transaction_status == status
    info = TypeInfo.fetch(conn, name)
    assert conn.info.transaction_status == status

    assert info.name == "text"
    # TODO: add the schema?
    # assert info.schema == "pg_catalog"

    assert info.oid == psycopg.adapters.types["text"].oid
    assert info.array_oid == psycopg.adapters.types["text"].array_oid
    assert info.regtype == "text"


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["text", sql.Identifier("text")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
async def test_fetch_async(aconn, name, status):
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        await aconn.execute("select 1")

    assert aconn.info.transaction_status == status
    info = await TypeInfo.fetch(aconn, name)
    assert aconn.info.transaction_status == status

    assert info.name == "text"
    # assert info.schema == "pg_catalog"
    assert info.oid == psycopg.adapters.types["text"].oid
    assert info.array_oid == psycopg.adapters.types["text"].array_oid


@pytest.mark.parametrize("name", ["nosuch", sql.Identifier("nosuch")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
def test_fetch_not_found(conn, name, status):
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        conn.execute("select 1")

    assert conn.info.transaction_status == status
    info = TypeInfo.fetch(conn, name)
    assert conn.info.transaction_status == status
    assert info is None


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["nosuch", sql.Identifier("nosuch")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
async def test_fetch_not_found_async(aconn, name, status):
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        await aconn.execute("select 1")

    assert aconn.info.transaction_status == status
    info = await TypeInfo.fetch(aconn, name)
    assert aconn.info.transaction_status == status

    assert info is None


@pytest.mark.crdb_skip("composite")
@pytest.mark.parametrize(
    "name", ["testschema.testtype", sql.Identifier("testschema", "testtype")]
)
def test_fetch_by_schema_qualified_string(conn, name):
    conn.execute("create schema if not exists testschema")
    conn.execute("create type testschema.testtype as (foo text)")

    info = TypeInfo.fetch(conn, name)
    assert info.name == "testtype"
    # assert info.schema == "testschema"
    cur = conn.execute(
        """
        select oid, typarray from pg_type
        where oid = 'testschema.testtype'::regtype
        """
    )
    assert cur.fetchone() == (info.oid, info.array_oid)


@pytest.mark.parametrize(
    "name",
    [
        "text",
        # TODO: support these?
        # "pg_catalog.text",
        # sql.Identifier("text"),
        # sql.Identifier("pg_catalog", "text"),
    ],
)
def test_registry_by_builtin_name(conn, name):
    info = psycopg.adapters.types[name]
    assert info.name == "text"
    assert info.oid == 25


def test_registry_empty():
    r = psycopg.types.TypesRegistry()
    assert r.get("text") is None
    with pytest.raises(KeyError):
        r["text"]


@pytest.mark.parametrize("oid, aoid", [(1, 2), (1, 0), (0, 2), (0, 0)])
def test_registry_invalid_oid(oid, aoid):
    r = psycopg.types.TypesRegistry()
    ti = psycopg.types.TypeInfo("test", oid, aoid)
    r.add(ti)
    assert r["test"] is ti
    if oid:
        assert r[oid] is ti
    if aoid:
        assert r[aoid] is ti
    with pytest.raises(KeyError):
        r[0]


def test_registry_copy():
    r = psycopg.types.TypesRegistry(psycopg.postgres.types)
    assert r.get("text") is r["text"] is r[25]
    assert r["text"].oid == 25


def test_registry_isolated():
    orig = psycopg.postgres.types
    tinfo = orig["text"]
    r = psycopg.types.TypesRegistry(orig)
    tdummy = psycopg.types.TypeInfo("dummy", tinfo.oid, tinfo.array_oid)
    r.add(tdummy)
    assert r[25] is r["dummy"] is tdummy
    assert orig[25] is r["text"] is tinfo
