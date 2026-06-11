from __future__ import annotations

import pytest

from psycopg import pq, sql
from psycopg.adapt import PyFormat, Transformer
from psycopg.postgres import types as builtins

try:
    from psycopg.types.cid import CID
    from psycopg.types.lsn import LSN
    from psycopg.types.tid import TID
    from psycopg.types.xid import XID, XID8
    from psycopg.types.oidvector import OidVector
    from psycopg.types.int2vector import Int2Vector
except ImportError:
    # allow importing on older versions of psycopg
    # so that psycopg_pool compatibility tests can run.
    class Dummy(type):
        def __call__(self, *args, **kwargs):
            return self

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    CID = LSN = TID = XID = XID8 = OidVector = Int2Vector = Dummy()  # type: ignore

pytestmark = pytest.mark.crdb_skip("catalog types")


def unregister_loaders(adapters, typname, format):
    adapters.unregister_loader(typname, format)
    if format is pq.Format.BINARY:
        pytest.skip("Backwards compatibility not preserved for binary")


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["0", "MAX"])
@pytest.mark.parametrize(
    "cls", [CID, XID, pytest.param(XID8, marks=pytest.mark.pg(">=13"))]
)
@pytest.mark.parametrize("with_loaders", [True, False])
def test_roundtrip_int_catalog_types(conn, val, fmt_in, fmt_out, cls, with_loaders):
    if val == "MAX":
        val = str(2**64 - 1 if cls is XID8 else str(2**32 - 1))
    cur = conn.cursor(binary=fmt_out)
    typname = cls.__name__.lower()

    if not with_loaders:
        unregister_loaders(cur.adapters, typname, fmt_out)

    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, cls) is with_loaders
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], cls) is with_loaders
    assert result[0] == val


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["0/0", LSN.from_int(2**64 - 1)])
@pytest.mark.parametrize("with_loaders", [True, False])
def test_roundtrip_lsn(conn, val, fmt_in, fmt_out, with_loaders):
    cur = conn.cursor(binary=fmt_out)
    typname = "pg_lsn"

    if not with_loaders:
        unregister_loaders(cur.adapters, typname, fmt_out)

    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, LSN) is with_loaders
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], LSN) is with_loaders
    assert result[0] == val


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize(
    "val", ["(0,0)", TID.from_block_and_offset(2**32 - 1, 2**16 - 1)]
)
@pytest.mark.parametrize("with_loaders", [True, False])
def test_roundtrip_tid(conn, val, fmt_in, fmt_out, with_loaders):
    cur = conn.cursor(binary=fmt_out)
    typname = "tid"

    if not with_loaders:
        unregister_loaders(cur.adapters, typname, fmt_out)

    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, TID) is with_loaders
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], TID) is with_loaders
    assert result[0] == val


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["", "0 0 0", "MAX"])
@pytest.mark.parametrize("cls", [Int2Vector, OidVector])
@pytest.mark.parametrize("with_loaders", [True, False])
def test_roundtrip_vector_catalog_types(conn, val, fmt_in, fmt_out, cls, with_loaders):
    if val == "MAX":
        val = " ".join(
            str(2**15 - 1 if cls is Int2Vector else str(2**32 - 1)) for _ in range(3)
        )
    cur = conn.cursor(binary=fmt_out)
    typname = cls.__name__.lower()

    if not with_loaders:
        unregister_loaders(cur.adapters, typname, fmt_out)

    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, cls) is with_loaders
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], cls) is with_loaders
    assert result[0] == val


@pytest.mark.parametrize(
    "typname,val,quoted",
    [
        ("cid", CID.from_int(12), b"'12'"),
        ("xid", XID.from_int(12), b"'12'"),
        pytest.param("xid8", XID8.from_int(12), b"'12'", marks=pytest.mark.pg(">=13")),
        ("pg_lsn", LSN.from_int(12), b"'0/C'"),
        ("tid", TID.from_tuple((400, 12)), b"'(400,12)'"),
        ("int2vector", Int2Vector.from_list([12, 30, 56]), b"'12 30 56'"),
        ("oidvector", OidVector.from_list([12, 30, 56]), b"'12 30 56'"),
    ],
)
def test_quote_catalog_types(conn, typname, val, quoted):
    tx = Transformer()

    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == quoted

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}::" + typname).format(v=sql.Literal(val)))
    assert isinstance(val, type(val))
    assert cur.fetchone()[0] == val


class TestStrSubclasses:
    @pytest.mark.parametrize(
        "val",
        [
            LSN.from_int(0),
            TID.from_tuple((0, 0)),
            XID.from_int(0),
            XID8.from_int(0),
            Int2Vector.from_list([]),
            OidVector.from_list([]),
        ],
    )
    def test_slots(self, val):
        assert not hasattr(val, "__dict__")

    @pytest.mark.parametrize("lsn", [LSN.from_int(12), LSN("0/C")])
    def test_lsn(self, lsn):
        assert lsn.value == 12
        assert lsn.high == 0
        assert lsn.low == 12
        assert repr(lsn) == "LSN('0/C')"
        assert lsn == "0/C"

    @pytest.mark.parametrize("tid", [TID.from_tuple((12, 3)), TID("(12,3)")])
    def test_tid(self, tid):
        assert tid.value == (12, 3)
        assert tid.block == 12
        assert tid.offset == 3
        assert repr(tid) == "TID('(12,3)')"
        assert tid == "(12,3)"

    @pytest.mark.parametrize("xid", [XID.from_int(12), XID("12")])
    def test_xid(self, xid):
        assert xid.value == 12
        assert repr(xid) == "XID('12')"
        assert xid == "12"

    @pytest.mark.parametrize(
        "xid8",
        [XID8.from_int(2**32 + 12), XID8(str(2**32 + 12))],
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
        [Int2Vector.from_list([12, 2, 0]), Int2Vector("12 2 0")],
    )
    def test_int2vector(self, int2vector):
        assert int2vector.value == [12, 2, 0]
        assert repr(int2vector) == "Int2Vector('12 2 0')"
        assert int2vector == "12 2 0"

    @pytest.mark.parametrize(
        "oidvector",
        [OidVector.from_list([12, 2, 0]), OidVector("12 2 0")],
    )
    def test_oidvector(self, oidvector):
        assert oidvector.value == [12, 2, 0]
        assert repr(oidvector) == "OidVector('12 2 0')"
        assert oidvector == "12 2 0"
