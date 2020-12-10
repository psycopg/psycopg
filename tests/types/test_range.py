import pytest

from psycopg3.sql import Identifier
from psycopg3.oids import builtins
from psycopg3.types import range as mrange


type2cls = {
    "int4range": mrange.Int4Range,
    "int8range": mrange.Int8Range,
    "numrange": mrange.DecimalRange,
    "daterange": mrange.DateRange,
    "tsrange": mrange.DateTimeRange,
    "tstzrange": mrange.DateTimeTZRange,
}
type2sub = {
    "int4range": "int4",
    "int8range": "int8",
    "numrange": "numeric",
    "daterange": "date",
    "tsrange": "timestamp",
    "tstzrange": "timestamptz",
}

samples = [
    ("int4range", None, None, "()"),
    ("int4range", 10, 20, "[]"),
    ("int4range", -(2 ** 31), (2 ** 31) - 1, "[)"),
    ("int8range", None, None, "()"),
    ("int8range", 10, 20, "[)"),
    ("int8range", -(2 ** 63), (2 ** 63) - 1, "[)"),
    # TODO: complete samples
]


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_dump_builtin_empty(conn, pgtype):
    r = type2cls[pgtype](empty=True)
    cur = conn.cursor()
    cur.execute(f"select 'empty'::{pgtype} = %s", (r,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype, min, max, bounds", samples)
def test_dump_builtin_range(conn, pgtype, min, max, bounds):
    r = type2cls[pgtype](min, max, bounds)
    sub = type2sub[pgtype]
    cur = conn.cursor()
    cur.execute(
        f"select {pgtype}(%s::{sub}, %s::{sub}, %s) = %s",
        (min, max, bounds, r),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_load_builtin_empty(conn, pgtype):
    r = type2cls[pgtype](empty=True)
    cur = conn.cursor()
    (got,) = cur.execute(f"select 'empty'::{pgtype}").fetchone()
    assert type(got) is type2cls[pgtype]
    assert got == r


@pytest.mark.parametrize("pgtype, min, max, bounds", samples)
def test_load_builtin_range(conn, pgtype, min, max, bounds):
    r = type2cls[pgtype](min, max, bounds)
    sub = type2sub[pgtype]
    cur = conn.cursor()
    cur.execute(
        f"select {pgtype}(%s::{sub}, %s::{sub}, %s)", (min, max, bounds)
    )
    # normalise discrete ranges
    if r.upper_inc and isinstance(r.upper, int):
        bounds = "[)" if r.lower_inc else "()"
        r = type(r)(r.lower, r.upper + 1, bounds)
    assert cur.fetchone()[0] == r


@pytest.fixture(scope="session")
def testrange(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        create schema if not exists testschema;

        drop type if exists testrange cascade;
        drop type if exists testschema.testrange cascade;

        create type testrange as range (subtype = text, collation = "C");
        create type testschema.testrange as range (subtype = float8);
        """
    )


fetch_cases = [
    ("testrange", "text"),
    ("testschema.testrange", "float8"),
    (Identifier("testrange"), "text"),
    (Identifier("testschema", "testrange"), "float8"),
]


@pytest.mark.parametrize("name, subtype", fetch_cases)
def test_fetch_info(conn, testrange, name, subtype):
    info = mrange.RangeInfo.fetch(conn, name)
    assert info.name == "testrange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == builtins[subtype].oid


@pytest.mark.asyncio
@pytest.mark.parametrize("name, subtype", fetch_cases)
async def test_fetch_info_async(aconn, testrange, name, subtype):
    info = await mrange.RangeInfo.fetch_async(aconn, name)
    assert info.name == "testrange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == builtins[subtype].oid


def test_dump_custom_empty(conn, testrange):
    class StrRange(mrange.Range):
        pass

    info = mrange.RangeInfo.fetch(conn, "testrange")
    info.register(conn, range_class=StrRange)

    r = StrRange(empty=True)
    cur = conn.cursor()
    cur.execute("select 'empty'::testrange = %s", (r,))
    assert cur.fetchone()[0] is True


def test_dump_quoting(conn, testrange):
    class StrRange(mrange.Range):
        pass

    info = mrange.RangeInfo.fetch(conn, "testrange")
    info.register(conn, range_class=StrRange)
    cur = conn.cursor()
    for i in range(1, 254):
        cur.execute(
            "select ascii(lower(%(r)s)) = %(low)s and ascii(upper(%(r)s)) = %(up)s",
            {"r": StrRange(chr(i), chr(i + 1)), "low": i, "up": i + 1},
        )
        assert cur.fetchone()[0] is True


def test_load_custom_empty(conn, testrange):
    info = mrange.RangeInfo.fetch(conn, "testrange")
    info.register(conn)

    cur = conn.cursor()
    (got,) = cur.execute("select 'empty'::testrange").fetchone()
    assert isinstance(got, mrange.Range)
    assert got.isempty


def test_load_quoting(conn, testrange):
    info = mrange.RangeInfo.fetch(conn, "testrange")
    info.register(conn)
    cur = conn.cursor()
    for i in range(1, 254):
        cur.execute(
            "select testrange(chr(%(low)s::int), chr(%(up)s::int))",
            {"low": i, "up": i + 1},
        )
        got = cur.fetchone()[0]
        assert isinstance(got, mrange.Range)
        assert ord(got.lower) == i
        assert ord(got.upper) == i + 1
