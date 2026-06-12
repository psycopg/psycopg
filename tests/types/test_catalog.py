from __future__ import annotations

from typing import Any

import pytest

import psycopg
from psycopg import adapt, adapters, pq, sql
from psycopg.adapt import PyFormat, Transformer
from psycopg.postgres import types as builtins
from psycopg.types.array import register_all_arrays

try:
    from psycopg._oids import INVALID_OID
except ImportError:
    from psycopg.postgres import INVALID_OID  # type: ignore

from ..fix_db import check_connection_version, maybe_trace

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
    class Dummy:
        def __call__(self, *args, **kwargs):
            return self

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    CID = LSN = TID = XID = XID8 = OidVector = Int2Vector = Dummy()  # type: ignore

pytestmark = pytest.mark.crdb_skip("catalog types")

# Construct a minimal AdaptersMap to test back compat
empty_map = adapt.AdaptersMap(types=adapters.types)
empty_map.adapters.register_loader(
    INVALID_OID,
    adapters.get_loader(INVALID_OID, pq.Format.TEXT),  # type: ignore
)
register_all_arrays(empty_map)
for fmt in PyFormat:
    for typ in (str, list):
        empty_map.adapters.register_dumper(typ, adapters.get_dumper(typ, fmt))


@pytest.fixture
def blank_conn(dsn, request, tracefile):
    check_connection_version(request.node)

    conn = psycopg.connect(dsn, context=empty_map)
    with maybe_trace(conn.pgconn, tracefile, request.function):
        yield conn
    conn.close()


def check_roundtrip(cur, typname, cls, val, fmt_in, with_loaders=True):
    result = cur.execute(f"select %{fmt_in.value}::{typname}", (val,)).fetchone()[0]
    assert cur.description[0].type_code == builtins[typname].oid
    assert isinstance(result, str)
    assert isinstance(result, cls) is with_loaders
    assert result == val

    result = cur.execute(f"select %{fmt_in.value}::{typname}[]", ([val],)).fetchone()[0]
    assert cur.description[0].type_code == builtins[typname].array_oid
    assert isinstance(result[0], str)
    assert isinstance(result[0], cls) is with_loaders
    assert result[0] == val


@pytest.mark.parametrize("format", pq.Format)
def test_system_columns(conn, format):
    conn.execute("CREATE TABLE test_system_columns (id BIGSERIAL)")
    conn.execute("INSERT INTO test_system_columns DEFAULT VALUES")
    with conn.cursor(binary=format) as cur:
        cur.execute(
            "SELECT tableoid, cmax, xmax, cmin, xmin, ctid from test_system_columns"
        )
        tableoid, cmax, xmax, cmin, xmin, ctid = cur.fetchone()
        for c in (cmax, cmin):
            assert isinstance(c, CID)
        for x in (xmax, xmin):
            assert isinstance(x, XID)
        assert isinstance(ctid, TID)
        assert isinstance(tableoid, int)


@pytest.mark.parametrize("format", pq.Format)
def test_current_lsn(conn, format):
    with conn.cursor(binary=format) as cur:
        cur.execute("SELECT pg_current_wal_lsn()")
        (lsn,) = cur.fetchone()
        assert isinstance(lsn, LSN)


@pytest.mark.parametrize("format", pq.Format)
def test_current_xid8(conn, format):
    with conn.cursor(binary=format) as cur:
        cur.execute("SELECT pg_current_xact_id()")
        (lsn,) = cur.fetchone()
        assert isinstance(lsn, XID8)


@pytest.mark.parametrize("format", pq.Format)
def test_indoption_int2vector(conn, format):
    with conn.cursor(binary=format) as cur:
        cur.execute("SELECT indoption from pg_index")
        (int2vector,) = cur.fetchone()
        assert isinstance(int2vector, Int2Vector)


@pytest.mark.parametrize("format", pq.Format)
def test_indclass_oidvector(conn, format):
    with conn.cursor(binary=format) as cur:
        cur.execute("SELECT indclass from pg_index")
        (int2vector,) = cur.fetchone()
        assert isinstance(int2vector, OidVector)


parametrize_roundtrip_int_val = pytest.mark.parametrize("val", ["0", "MAX"])


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@parametrize_roundtrip_int_val
@pytest.mark.parametrize(
    "cls", [CID, XID, pytest.param(XID8, marks=pytest.mark.pg(">=13"))]
)
def test_roundtrip_int_catalog_types(conn, val, fmt_in, fmt_out, cls):
    if val == "MAX":
        val = str(2**64 - 1 if cls is XID8 else str(2**32 - 1))
    cur = conn.cursor(binary=fmt_out)
    typname = cls.__name__.lower()

    check_roundtrip(cur, typname, cls, val, fmt_in)


@pytest.mark.parametrize("fmt_in", PyFormat)
@parametrize_roundtrip_int_val
@pytest.mark.parametrize(
    "cls", [CID, XID, pytest.param(XID8, marks=pytest.mark.pg(">=13"))]
)
def test_roundtrip_int_catalog_types_no_loaders(blank_conn, val, fmt_in, cls):
    if val == "MAX":
        val = str(2**64 - 1 if cls is XID8 else str(2**32 - 1))
    cur = blank_conn.cursor(binary=False)
    typname = cls.__name__.lower()

    check_roundtrip(cur, typname, cls, val, fmt_in, with_loaders=False)


parametrize_roundtrip_lsn_val = pytest.mark.parametrize(
    "val", ["0/0", LSN.from_int(2**64 - 1)]
)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@parametrize_roundtrip_lsn_val
def test_roundtrip_lsn(conn, val, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    typname = "pg_lsn"

    check_roundtrip(cur, typname, LSN, val, fmt_in)


@pytest.mark.parametrize("fmt_in", PyFormat)
@parametrize_roundtrip_lsn_val
def test_roundtrip_lsn_no_loaders(blank_conn, val, fmt_in):
    cur = blank_conn.cursor(binary=False)
    typname = "pg_lsn"

    check_roundtrip(cur, typname, LSN, val, fmt_in, with_loaders=False)


parametrize_roundtrip_tid_val = pytest.mark.parametrize(
    "val", ["(0,0)", TID.from_tuple((2**32 - 1, 2**16 - 1))]
)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@parametrize_roundtrip_tid_val
def test_roundtrip_tid(conn, val, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    typname = "tid"

    check_roundtrip(cur, typname, TID, val, fmt_in)


@pytest.mark.parametrize("fmt_in", PyFormat)
@parametrize_roundtrip_tid_val
def test_roundtrip_tid_no_loaders(blank_conn, val, fmt_in):
    cur = blank_conn.cursor(binary=False)
    typname = "tid"

    check_roundtrip(cur, typname, LSN, val, fmt_in, with_loaders=False)


parametrize_roundtrip_vector_val = pytest.mark.parametrize("val", ["", "0 0 0", "MAX"])


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@parametrize_roundtrip_vector_val
@pytest.mark.parametrize("cls", [Int2Vector, OidVector])
def test_roundtrip_vector_catalog_types(conn, val, fmt_in, fmt_out, cls):
    if val == "MAX":
        val = " ".join(
            str(2**15 - 1 if cls is Int2Vector else str(2**32 - 1)) for _ in range(3)
        )
    cur = conn.cursor(binary=fmt_out)
    typname = cls.__name__.lower()

    check_roundtrip(cur, typname, cls, val, fmt_in)


@pytest.mark.parametrize("fmt_in", PyFormat)
@parametrize_roundtrip_vector_val
@pytest.mark.parametrize("cls", [Int2Vector, OidVector])
def test_roundtrip_vector_catalog_types_no_loaders(blank_conn, val, fmt_in, cls):
    if val == "MAX":
        val = " ".join(
            str(2**15 - 1 if cls is Int2Vector else str(2**32 - 1)) for _ in range(3)
        )
    cur = blank_conn.cursor(binary=False)
    typname = cls.__name__.lower()

    check_roundtrip(cur, typname, cls, val, fmt_in, with_loaders=False)


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
    cur.execute(
        sql.SQL("select {v}::{t}").format(v=sql.Literal(val), t=sql.Identifier(typname))
    )
    assert isinstance(val, type(val))
    assert cur.fetchone()[0] == val


class TestStrSubclasses:
    @pytest.mark.parametrize(
        "val",
        [
            LSN.from_int(0),
            CID.from_int(0),
            TID.from_tuple((0, 0)),
            XID.from_int(0),
            XID8.from_int(0),
            Int2Vector.from_list([]),
            OidVector.from_list([]),
        ],
    )
    def test_slots(self, val):
        assert not hasattr(val, "__dict__")

    @pytest.mark.parametrize(
        "cls",
        [LSN, CID, XID, XID8],
    )
    def test_add(self, cls):
        assert cls.from_int(1) + 1 == cls.from_int(2)
        with pytest.raises(TypeError):
            _ = 1 + cls.from_int(1) == 2

    @pytest.mark.parametrize(
        "cls",
        [LSN, CID, XID, XID8],
    )
    def test_sub(self, cls):
        assert cls.from_int(1) - 1 == cls.from_int(0)
        with pytest.raises(OverflowError, match=cls.__name__):
            _ = cls.from_int(0) - 1
        assert cls.from_int(2) - cls.from_int(1) == 1
        with pytest.raises(TypeError):
            _ = 1 - cls.from_int(1)

    @pytest.mark.parametrize(
        "cls,trueval,falseval",
        [
            (LSN, "0/1", "0/0"),
            (CID, "1", "0"),
            (TID, "(0,1)", "(0,0)"),
            (XID, "1", "0"),
            (XID8, "1", "0"),
            (Int2Vector, "1", ""),
            (OidVector, "1", ""),
        ],
    )
    def test_bool(self, cls, trueval, falseval):
        assert cls(trueval)
        assert not cls(falseval)

    @pytest.mark.parametrize(
        "constructor,unsorted_vals,sorted_vals",
        [
            (
                LSN.from_int,
                [1, 45, 12, 0],
                [0, 1, 12, 45],
            ),
            (
                CID.from_int,
                [1, 45, 12, 0],
                [0, 1, 12, 45],
            ),
            (
                TID.from_tuple,
                [(1, 1), (0, 0), (1, 0), (0, 1)],
                [(0, 0), (0, 1), (1, 0), (1, 1)],
            ),
            (
                XID.from_int,
                [1, 45, 12, 0],
                [0, 1, 12, 45],
            ),
            (
                XID8.from_int,
                [1, 45, 12, 0],
                [0, 1, 12, 45],
            ),
            (
                Int2Vector.from_list,
                [[1, 1, 1], [], [1, 1], [1, 1, 0], [0, 0], [0], [0, 1], [1]],
                [[], [0], [0, 0], [0, 1], [1], [1, 1], [1, 1, 0], [1, 1, 1]],
            ),
            (
                OidVector.from_list,
                [[1, 1, 1], [], [1, 1], [1, 1, 0], [0, 0], [0], [0, 1], [1]],
                [[], [0], [0, 0], [0, 1], [1], [1, 1], [1, 1, 0], [1, 1, 1]],
            ),
        ],
    )
    def test_ordering(self, constructor, unsorted_vals, sorted_vals):
        assert list(sorted(constructor(val) for val in unsorted_vals)) == list(
            constructor(val) for val in sorted_vals
        )

    @pytest.mark.parametrize(
        "cls,overval,underval",
        [
            (LSN, "100000000/00000000", "0/100000000"),
            (CID, 2**32, -1),
            (TID, (2**32, 1), (-1, 1)),
            (TID, (1, 2**16), (1, -1)),
            (XID, 2**32, -1),
            (XID8, 2**64, -1),
            (Int2Vector, 2**15, -(2**15) - 1),
            (OidVector, 2**32, -1),
        ],
    )
    def test_overflow_default(self, cls, overval, underval):
        for val in (overval, underval):
            obj = cls(str(val))
            with pytest.raises(OverflowError, match=cls.__name__):
                obj.value

        if isinstance(overval, int):
            valid_vals: tuple[Any, Any] = (overval - 1, underval + 1)
        elif cls is LSN:
            valid_vals = ("FFFFFFFF/FFFFFFFF", "0/0")
        else:
            valid_vals = (
                tuple(val - 1 for val in overval),
                tuple(val + 1 for val in underval),
            )

        for val in valid_vals:
            obj = cls(str(val))
            obj.value

    @pytest.mark.parametrize(
        "constructor,overval,underval",
        [
            (LSN.from_int, 2**64, -1),
            (CID.from_int, 2**32, -1),
            (TID.from_tuple, (2**32, 1), (-1, 1)),
            (TID.from_tuple, (1, 2**16), (1, -1)),
            (XID.from_int, 2**32, -1),
            (XID8.from_int, 2**64, -1),
            (Int2Vector.from_list, [2**15], [-(2**15) - 1]),
            (OidVector.from_list, [2**32], [-1]),
        ],
    )
    def test_overflow_from(self, constructor, overval, underval):
        for val in (overval, underval):
            with pytest.raises(OverflowError, match=constructor.__self__.__name__):
                obj = constructor(val)

        if isinstance(overval, int):
            valid_over = overval - 1
            valid_under = underval + 1
        else:
            valid_over = tuple(val - 1 for val in overval)  # type: ignore
            valid_under = tuple(val + 1 for val in underval)

        for val in (valid_over, valid_under):
            obj = constructor(val)
            obj.value

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
