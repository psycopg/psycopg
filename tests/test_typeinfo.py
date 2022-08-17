import pytest

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types import TypeInfo
from psycopg.types.composite import CompositeInfo
from psycopg.types.enum import EnumInfo
from psycopg.types.multirange import MultirangeInfo
from psycopg.types.range import RangeInfo

from .fix_crdb import crdb_encoding


@pytest.mark.parametrize("name", ["text", sql.Identifier("text")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS", None])
@pytest.mark.parametrize(
    "encoding", ["utf8", crdb_encoding("latin1"), crdb_encoding("sql_ascii")]
)
def test_fetch(conn, name, status, encoding):
    with conn.transaction():
        conn.execute("select set_config('client_encoding', %s, false)", [encoding])

    if status:
        status = getattr(TransactionStatus, status)
        if status == TransactionStatus.INTRANS:
            conn.execute("select 1")
    else:
        conn.autocommit = True
        status = TransactionStatus.IDLE

    assert conn.info.transaction_status == status
    info = TypeInfo.fetch(conn, name)
    assert conn.info.transaction_status == status

    assert info.name == "text"
    # TODO: add the schema?
    # assert info.schema == "pg_catalog"

    assert info.oid == psycopg.adapters.types["text"].oid
    assert info.array_oid == psycopg.adapters.types["text"].array_oid
    assert info.regtype == "text"


@pytest.mark.parametrize("name", ["text", sql.Identifier("text")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS", None])
@pytest.mark.parametrize(
    "encoding", ["utf8", crdb_encoding("latin1"), crdb_encoding("sql_ascii")]
)
async def test_fetch_async(aconn, name, status, encoding):
    async with aconn.transaction():
        await aconn.execute(
            "select set_config('client_encoding', %s, false)", [encoding]
        )

    if status:
        status = getattr(TransactionStatus, status)
        if status == TransactionStatus.INTRANS:
            await aconn.execute("select 1")
    else:
        await aconn.set_autocommit(True)
        status = TransactionStatus.IDLE

    assert aconn.info.transaction_status == status
    info = await TypeInfo.fetch(aconn, name)
    assert aconn.info.transaction_status == status

    assert info.name == "text"
    # assert info.schema == "pg_catalog"
    assert info.oid == psycopg.adapters.types["text"].oid
    assert info.array_oid == psycopg.adapters.types["text"].array_oid


_name = pytest.mark.parametrize("name", ["nosuch", sql.Identifier("nosuch")])
_status = pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
_info_cls = pytest.mark.parametrize(
    "info_cls",
    [
        pytest.param(TypeInfo),
        pytest.param(RangeInfo, marks=pytest.mark.crdb_skip("range")),
        pytest.param(
            MultirangeInfo,
            marks=(pytest.mark.crdb_skip("range"), pytest.mark.pg(">= 14")),
        ),
        pytest.param(CompositeInfo, marks=pytest.mark.crdb_skip("composite")),
        pytest.param(EnumInfo),
    ],
)


@_name
@_status
@_info_cls
def test_fetch_not_found(conn, name, status, info_cls, monkeypatch):
    if TypeInfo._has_to_regtype_function(conn):
        exit_orig = psycopg.Transaction.__exit__

        def exit(self, exc_type, exc_val, exc_tb):
            assert exc_val is None
            return exit_orig(self, exc_type, exc_val, exc_tb)

        monkeypatch.setattr(psycopg.Transaction, "__exit__", exit)
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        conn.execute("select 1")

    assert conn.info.transaction_status == status
    info = info_cls.fetch(conn, name)
    assert conn.info.transaction_status == status
    assert info is None


@_name
@_status
@_info_cls
async def test_fetch_not_found_async(aconn, name, status, info_cls, monkeypatch):
    if TypeInfo._has_to_regtype_function(aconn):
        exit_orig = psycopg.AsyncTransaction.__aexit__

        async def aexit(self, exc_type, exc_val, exc_tb):
            assert exc_val is None
            return await exit_orig(self, exc_type, exc_val, exc_tb)

        monkeypatch.setattr(psycopg.AsyncTransaction, "__aexit__", aexit)
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        await aconn.execute("select 1")

    assert aconn.info.transaction_status == status
    info = await info_cls.fetch(aconn, name)
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
