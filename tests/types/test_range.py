import pickle
import datetime as dt
from decimal import Decimal

import pytest

from psycopg3.sql import Identifier
from psycopg3.types import Range, RangeInfo


type2sub = {
    "int4range": "int4",
    "int8range": "int8",
    "numrange": "numeric",
    "daterange": "date",
    "tsrange": "timestamp",
    "tstzrange": "timestamptz",
}

tzinfo = dt.timezone(dt.timedelta(hours=2))

samples = [
    ("int4range", None, None, "()"),
    ("int4range", 10, 20, "[]"),
    ("int4range", -(2 ** 31), (2 ** 31) - 1, "[)"),
    ("int8range", None, None, "()"),
    ("int8range", 10, 20, "[)"),
    ("int8range", -(2 ** 63), (2 ** 63) - 1, "[)"),
    ("numrange", Decimal(-100), Decimal("100.123"), "(]"),
    ("daterange", dt.date(2000, 1, 1), dt.date(2020, 1, 1), "[)"),
    (
        "tsrange",
        dt.datetime(2000, 1, 1, 00, 00),
        dt.datetime(2020, 1, 1, 23, 59, 59, 999999),
        "[]",
    ),
    (
        "tstzrange",
        dt.datetime(2000, 1, 1, 00, 00, tzinfo=tzinfo),
        dt.datetime(2020, 1, 1, 23, 59, 59, 999999, tzinfo=tzinfo),
        "()",
    ),
]


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_dump_builtin_empty(conn, pgtype):
    r = Range(empty=True)
    cur = conn.execute(f"select 'empty'::{pgtype} = %s", (r,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_dump_builtin_array(conn, pgtype):
    r1 = Range(empty=True)
    r2 = Range(bounds="()")
    cur = conn.execute(
        f"select array['empty'::{pgtype}, '(,)'::{pgtype}] = %s",
        ([r1, r2],),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype, min, max, bounds", samples)
def test_dump_builtin_range(conn, pgtype, min, max, bounds):
    r = Range(min, max, bounds)
    sub = type2sub[pgtype]
    cur = conn.execute(
        f"select {pgtype}(%s::{sub}, %s::{sub}, %s) = %s::{pgtype}",
        (min, max, bounds, r),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_load_builtin_empty(conn, pgtype):
    r = Range(empty=True)
    (got,) = conn.execute(f"select 'empty'::{pgtype}").fetchone()
    assert type(got) is Range
    assert got == r
    assert not got
    assert got.isempty


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_load_builtin_inf(conn, pgtype):
    r = Range(bounds="()")
    (got,) = conn.execute(f"select '(,)'::{pgtype}").fetchone()
    assert type(got) is Range
    assert got == r
    assert got
    assert not got.isempty
    assert got.lower_inf
    assert got.upper_inf


@pytest.mark.parametrize(
    "pgtype",
    "int4range int8range numrange daterange tsrange tstzrange".split(),
)
def test_load_builtin_array(conn, pgtype):
    r1 = Range(empty=True)
    r2 = Range(bounds="()")
    (got,) = conn.execute(
        f"select array['empty'::{pgtype}, '(,)'::{pgtype}]"
    ).fetchone()
    assert got == [r1, r2]


@pytest.mark.parametrize("pgtype, min, max, bounds", samples)
def test_load_builtin_range(conn, pgtype, min, max, bounds):
    r = Range(min, max, bounds)
    sub = type2sub[pgtype]
    cur = conn.execute(
        f"select {pgtype}(%s::{sub}, %s::{sub}, %s)", (min, max, bounds)
    )
    # normalise discrete ranges
    if r.upper_inc and isinstance(r.upper, int):
        bounds = "[)" if r.lower_inc else "()"
        r = type(r)(r.lower, r.upper + 1, bounds)
    assert cur.fetchone()[0] == r


@pytest.fixture(scope="session")
def testrange(svcconn):
    svcconn.execute(
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
    info = RangeInfo.fetch(conn, name)
    assert info.name == "testrange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == conn.adapters.types[subtype].oid


def test_fetch_info_not_found(conn):
    with pytest.raises(conn.ProgrammingError):
        RangeInfo.fetch(conn, "nosuchrange")


@pytest.mark.asyncio
@pytest.mark.parametrize("name, subtype", fetch_cases)
async def test_fetch_info_async(aconn, testrange, name, subtype):
    info = await RangeInfo.fetch_async(aconn, name)
    assert info.name == "testrange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == aconn.adapters.types[subtype].oid


@pytest.mark.asyncio
async def test_fetch_info_not_found_async(aconn):
    with pytest.raises(aconn.ProgrammingError):
        await RangeInfo.fetch_async(aconn, "nosuchrange")


def test_dump_custom_empty(conn, testrange):
    info = RangeInfo.fetch(conn, "testrange")
    info.register(conn)

    r = Range(empty=True)
    cur = conn.execute("select 'empty'::testrange = %s", (r,))
    assert cur.fetchone()[0] is True


def test_dump_quoting(conn, testrange):
    info = RangeInfo.fetch(conn, "testrange")
    info.register(conn)
    cur = conn.cursor()
    for i in range(1, 254):
        cur.execute(
            """
            select ascii(lower(%(r)s)) = %(low)s
                and ascii(upper(%(r)s)) = %(up)s
            """,
            {"r": Range(chr(i), chr(i + 1)), "low": i, "up": i + 1},
        )
        assert cur.fetchone()[0] is True


def test_load_custom_empty(conn, testrange):
    info = RangeInfo.fetch(conn, "testrange")
    info.register(conn)

    (got,) = conn.execute("select 'empty'::testrange").fetchone()
    assert isinstance(got, Range)
    assert got.isempty


def test_load_quoting(conn, testrange):
    info = RangeInfo.fetch(conn, "testrange")
    info.register(conn)
    cur = conn.cursor()
    for i in range(1, 254):
        cur.execute(
            "select testrange(chr(%(low)s::int), chr(%(up)s::int))",
            {"low": i, "up": i + 1},
        )
        got = cur.fetchone()[0]
        assert isinstance(got, Range)
        assert ord(got.lower) == i
        assert ord(got.upper) == i + 1


class TestRangeObject:
    def test_noparam(self):
        r = Range()

        assert not r.isempty
        assert r.lower is None
        assert r.upper is None
        assert r.lower_inf
        assert r.upper_inf
        assert not r.lower_inc
        assert not r.upper_inc

    def test_empty(self):
        r = Range(empty=True)

        assert r.isempty
        assert r.lower is None
        assert r.upper is None
        assert not r.lower_inf
        assert not r.upper_inf
        assert not r.lower_inc
        assert not r.upper_inc

    def test_nobounds(self):
        r = Range(10, 20)
        assert r.lower == 10
        assert r.upper == 20
        assert not r.isempty
        assert not r.lower_inf
        assert not r.upper_inf
        assert r.lower_inc
        assert not r.upper_inc

    def test_bounds(self):
        for bounds, lower_inc, upper_inc in [
            ("[)", True, False),
            ("(]", False, True),
            ("()", False, False),
            ("[]", True, True),
        ]:
            r = Range(10, 20, bounds)
            assert r.lower == 10
            assert r.upper == 20
            assert not r.isempty
            assert not r.lower_inf
            assert not r.upper_inf
            assert r.lower_inc == lower_inc
            assert r.upper_inc == upper_inc

    def test_keywords(self):
        r = Range(upper=20)
        r.lower is None
        r.upper == 20
        assert not r.isempty
        assert r.lower_inf
        assert not r.upper_inf
        assert not r.lower_inc
        assert not r.upper_inc

        r = Range(lower=10, bounds="(]")
        r.lower == 10
        r.upper is None
        assert not r.isempty
        assert not r.lower_inf
        assert r.upper_inf
        assert not r.lower_inc
        assert not r.upper_inc

    def test_bad_bounds(self):
        with pytest.raises(ValueError):
            Range(bounds="(")
        with pytest.raises(ValueError):
            Range(bounds="[}")

    def test_in(self):
        r = Range(empty=True)
        assert 10 not in r

        r = Range()
        assert 10 in r

        r = Range(lower=10, bounds="[)")
        assert 9 not in r
        assert 10 in r
        assert 11 in r

        r = Range(lower=10, bounds="()")
        assert 9 not in r
        assert 10 not in r
        assert 11 in r

        r = Range(upper=20, bounds="()")
        assert 19 in r
        assert 20 not in r
        assert 21 not in r

        r = Range(upper=20, bounds="(]")
        assert 19 in r
        assert 20 in r
        assert 21 not in r

        r = Range(10, 20)
        assert 9 not in r
        assert 10 in r
        assert 11 in r
        assert 19 in r
        assert 20 not in r
        assert 21 not in r

        r = Range(10, 20, "(]")
        assert 9 not in r
        assert 10 not in r
        assert 11 in r
        assert 19 in r
        assert 20 in r
        assert 21 not in r

        r = Range(20, 10)
        assert 9 not in r
        assert 10 not in r
        assert 11 not in r
        assert 19 not in r
        assert 20 not in r
        assert 21 not in r

    def test_nonzero(self):
        assert Range()
        assert Range(10, 20)
        assert not Range(empty=True)

    def test_eq_hash(self):
        def assert_equal(r1, r2):
            assert r1 == r2
            assert hash(r1) == hash(r2)

        assert_equal(Range(empty=True), Range(empty=True))
        assert_equal(Range(), Range())
        assert_equal(Range(10, None), Range(10, None))
        assert_equal(Range(10, 20), Range(10, 20))
        assert_equal(Range(10, 20), Range(10, 20, "[)"))
        assert_equal(Range(10, 20, "[]"), Range(10, 20, "[]"))

        def assert_not_equal(r1, r2):
            assert r1 != r2
            assert hash(r1) != hash(r2)

        assert_not_equal(Range(10, 20), Range(10, 21))
        assert_not_equal(Range(10, 20), Range(11, 20))
        assert_not_equal(Range(10, 20, "[)"), Range(10, 20, "[]"))

    def test_eq_wrong_type(self):
        assert Range(10, 20) != ()

    # as the postgres docs describe for the server-side stuff,
    # ordering is rather arbitrary, but will remain stable
    # and consistent.

    def test_lt_ordering(self):
        assert Range(empty=True) < Range(0, 4)
        assert not Range(1, 2) < Range(0, 4)
        assert Range(0, 4) < Range(1, 2)
        assert not Range(1, 2) < Range()
        assert Range() < Range(1, 2)
        assert not Range(1) < Range(upper=1)
        assert not Range() < Range()
        assert not Range(empty=True) < Range(empty=True)
        assert not Range(1, 2) < Range(1, 2)
        with pytest.raises(TypeError):
            assert 1 < Range(1, 2)
        with pytest.raises(TypeError):
            assert not Range(1, 2) < 1

    def test_gt_ordering(self):
        assert not Range(empty=True) > Range(0, 4)
        assert Range(1, 2) > Range(0, 4)
        assert not Range(0, 4) > Range(1, 2)
        assert Range(1, 2) > Range()
        assert not Range() > Range(1, 2)
        assert Range(1) > Range(upper=1)
        assert not Range() > Range()
        assert not Range(empty=True) > Range(empty=True)
        assert not Range(1, 2) > Range(1, 2)
        with pytest.raises(TypeError):
            assert not 1 > Range(1, 2)
        with pytest.raises(TypeError):
            assert Range(1, 2) > 1

    def test_le_ordering(self):
        assert Range(empty=True) <= Range(0, 4)
        assert not Range(1, 2) <= Range(0, 4)
        assert Range(0, 4) <= Range(1, 2)
        assert not Range(1, 2) <= Range()
        assert Range() <= Range(1, 2)
        assert not Range(1) <= Range(upper=1)
        assert Range() <= Range()
        assert Range(empty=True) <= Range(empty=True)
        assert Range(1, 2) <= Range(1, 2)
        with pytest.raises(TypeError):
            assert 1 <= Range(1, 2)
        with pytest.raises(TypeError):
            assert not Range(1, 2) <= 1

    def test_ge_ordering(self):
        assert not Range(empty=True) >= Range(0, 4)
        assert Range(1, 2) >= Range(0, 4)
        assert not Range(0, 4) >= Range(1, 2)
        assert Range(1, 2) >= Range()
        assert not Range() >= Range(1, 2)
        assert Range(1) >= Range(upper=1)
        assert Range() >= Range()
        assert Range(empty=True) >= Range(empty=True)
        assert Range(1, 2) >= Range(1, 2)
        with pytest.raises(TypeError):
            assert not 1 >= Range(1, 2)
        with pytest.raises(TypeError):
            (Range(1, 2) >= 1)

    def test_pickling(self):
        r = Range(0, 4)
        assert pickle.loads(pickle.dumps(r)) == r

    def test_str(self):
        """
        Range types should have a short and readable ``str`` implementation.
        """
        expected = [
            "(0, 4)",
            "[0, 4]",
            "(0, 4]",
            "[0, 4)",
            "empty",
        ]
        results = []

        for bounds in ("()", "[]", "(]", "[)"):
            r = Range(0, 4, bounds=bounds)
            results.append(str(r))

        r = Range(empty=True)
        results.append(str(r))
        assert results == expected

    def test_str_datetime(self):
        """
        Date-Time ranges should return a human-readable string as well on
        string conversion.
        """
        tz = dt.timezone(dt.timedelta(hours=-5))
        r = Range(
            dt.datetime(2010, 1, 1, tzinfo=tz),
            dt.datetime(2011, 1, 1, tzinfo=tz),
        )
        expected = "[2010-01-01 00:00:00-05:00, 2011-01-01 00:00:00-05:00)"
        result = str(r)
        assert result == expected
