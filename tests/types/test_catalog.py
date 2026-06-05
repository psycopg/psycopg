from __future__ import annotations

import pytest

from psycopg import pq, sql
from psycopg.adapt import PyFormat, Transformer
from psycopg.postgres import types as builtins
from psycopg.types.cid import CID
from psycopg.types.lsn import LSN
from psycopg.types.tid import TID
from psycopg.types.xid import XID, XID8
from psycopg.types.oidvector import OidVector
from psycopg.types.int2vector import Int2Vector

pytestmark = pytest.mark.crdb_skip("pg_catalog_types")


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["0", "MAX"])
@pytest.mark.parametrize(
    "cls", [CID, XID, pytest.param(XID8, marks=pytest.mark.pg(">=13"))]
)
def test_roundtrip_int_catalog_types(conn, val, fmt_in, fmt_out, cls, with_loaders):
    if val == "MAX":
        val = str(2**64 - 1 if cls is XID8 else str(2**32 - 1))
    cur = conn.cursor(binary=fmt_out)
    typname = cls.__name__.lower()
    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, cls)
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], cls)
    assert result[0] == val


@pytest.mark.parametrize(
    "cls", [CID, XID, pytest.param(XID8, marks=pytest.mark.pg(">=13"))]
)
def test_quote_int_catalog_types(conn, cls):
    tx = Transformer()
    val = cls.from_int(12)
    typname = cls.__name__.lower()

    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == b"'12'"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}::" + typname).format(v=sql.Literal(val)))
    assert isinstance(val, cls)
    assert cur.fetchone()[0] == val


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["0/0", LSN.from_int(2**64 - 1)])
def test_roundtrip_lsn(conn, val, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    typname = "pg_lsn"
    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, LSN)
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], LSN)
    assert result[0] == val


def test_quote_lsn(conn):
    tx = Transformer()
    val = LSN.from_int(12)
    typname = "pg_lsn"

    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == b"'0/C'"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}::" + typname).format(v=sql.Literal(val)))
    assert isinstance(val, LSN)
    assert cur.fetchone()[0] == val


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize(
    "val", ["(0,0)", TID.from_block_and_offset(2**32 - 1, 2**16 - 1)]
)
def test_roundtrip_tid(conn, val, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    typname = "tid"
    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, TID)
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], TID)
    assert result[0] == val


def test_quote_tid(conn):
    tx = Transformer()
    val = TID.from_block_and_offset(400, 12)
    typname = "tid"

    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == b"'(400,12)'"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}::" + typname).format(v=sql.Literal(val)))
    assert isinstance(val, TID)
    assert cur.fetchone()[0] == val


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["", "0 0 0", "MAX"])
@pytest.mark.parametrize("cls", [Int2Vector, OidVector])
def test_roundtrip_vector_catalog_types(conn, val, fmt_in, fmt_out, cls):
    if val == "MAX":
        val = " ".join(
            str(2**15 - 1 if cls is Int2Vector else str(2**32 - 1)) for _ in range(3)
        )
    cur = conn.cursor(binary=fmt_out)
    typname = cls.__name__.lower()
    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, cls)
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], cls)
    assert result[0] == val


@pytest.mark.parametrize("cls", [Int2Vector, OidVector])
def test_quote_vector_catalog_types(conn, cls):
    tx = Transformer()
    val = cls.from_list([12, 30, 56])
    typname = cls.__name__.lower()

    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == b"'12 30 56'"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}::" + typname).format(v=sql.Literal(val)))
    assert isinstance(val, cls)
    assert cur.fetchone()[0] == val


class TestStrSubclasses:
    @pytest.mark.parametrize("lsn", [LSN.from_int(12), LSN.from_buffer(b"0/C")])
    def test_lsn(self, lsn):
        assert lsn.value == 12
        assert lsn.high == 0
        assert lsn.low == 12
        assert repr(lsn) == "LSN('0/C')"
        assert lsn == "0/C"

    @pytest.mark.parametrize(
        "tid", [TID.from_block_and_offset(12, 3), TID.from_buffer(b"(12,3)")]
    )
    def test_tid(self, tid):
        assert tid.block == 12
        assert tid.offset == 3
        assert repr(tid) == "TID('(12,3)')"
        assert tid == "(12,3)"

    @pytest.mark.parametrize("xid", [XID.from_int(12), XID.from_buffer(b"12")])
    def test_xid(self, xid):
        assert xid.value == 12
        assert repr(xid) == "XID('12')"
        assert xid == "12"

    @pytest.mark.parametrize(
        "xid8",
        [XID8.from_int(2**32 + 12), XID8.from_buffer(str(2**32 + 12).encode("ascii"))],
    )
    def test_xid8(self, xid8):
        xid = xid8.xid
        assert xid.value == 12
        assert isinstance(xid, XID)
        assert xid == XID.from_int(12)
        assert xid8.epoch == 1
        assert xid8.value == 2**32 + 12
        assert repr(xid8) == f"XID8('{2**32 + 12}')"
        assert xid8 == str(2**32 + 12)

    @pytest.mark.parametrize(
        "int2vector",
        [Int2Vector.from_list([12, 2, 0]), Int2Vector.from_buffer(b"12 2 0")],
    )
    def test_int2vector(self, int2vector):
        assert int2vector.value == [12, 2, 0]
        assert repr(int2vector) == "Int2Vector('12 2 0')"
        assert int2vector == "12 2 0"

    @pytest.mark.parametrize(
        "oidvector",
        [OidVector.from_list([12, 2, 0]), OidVector.from_buffer(b"12 2 0")],
    )
    def test_oidvector(self, oidvector):
        assert oidvector.value == [12, 2, 0]
        assert repr(oidvector) == "OidVector('12 2 0')"
        assert oidvector == "12 2 0"
