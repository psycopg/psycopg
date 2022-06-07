import pickle
import datetime as dt
from decimal import Decimal

import pytest

from psycopg import pq, sql
from psycopg import errors as e
from psycopg.adapt import PyFormat
from psycopg.types import range as range_module
from psycopg.types.range import Range, RangeInfo, register_range

from ..utils import eur
from ..fix_crdb import is_crdb, crdb_skip_message

pytestmark = pytest.mark.crdb_skip("range")

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
    ("int4range", -(2**31), (2**31) - 1, "[)"),
    ("int8range", None, None, "()"),
    ("int8range", 10, 20, "[)"),
    ("int8range", -(2**63), (2**63) - 1, "[)"),
    ("numrange", Decimal(-100), Decimal("100.123"), "(]"),
    ("numrange", Decimal(100), None, "()"),
    ("numrange", None, Decimal(100), "()"),
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

range_names = """
    int4range int8range numrange daterange tsrange tstzrange
    """.split()

range_classes = """
    Int4Range Int8Range NumericRange DateRange TimestampRange TimestamptzRange
    """.split()


@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty(conn, pgtype, fmt_in):
    r = Range(empty=True)  # type: ignore[var-annotated]
    cur = conn.execute(f"select 'empty'::{pgtype} = %{fmt_in.value}", (r,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("wrapper", range_classes)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty_wrapper(conn, wrapper, fmt_in):
    wrapper = getattr(range_module, wrapper)
    r = wrapper(empty=True)
    cur = conn.execute(f"select 'empty' = %{fmt_in.value}", (r,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize(
    "fmt_in",
    [
        PyFormat.AUTO,
        PyFormat.TEXT,
        # There are many ways to work around this (use text, use a cast on the
        # placeholder, use specific Range subclasses).
        pytest.param(
            PyFormat.BINARY,
            marks=pytest.mark.xfail(
                reason="can't dump an array of untypes binary range without cast"
            ),
        ),
    ],
)
def test_dump_builtin_array(conn, pgtype, fmt_in):
    r1 = Range(empty=True)  # type: ignore[var-annotated]
    r2 = Range(bounds="()")  # type: ignore[var-annotated]
    cur = conn.execute(
        f"select array['empty'::{pgtype}, '(,)'::{pgtype}] = %{fmt_in.value}",
        ([r1, r2],),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_array_with_cast(conn, pgtype, fmt_in):
    r1 = Range(empty=True)  # type: ignore[var-annotated]
    r2 = Range(bounds="()")  # type: ignore[var-annotated]
    cur = conn.execute(
        f"select array['empty'::{pgtype}, '(,)'::{pgtype}] "
        f"= %{fmt_in.value}::{pgtype}[]",
        ([r1, r2],),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("wrapper", range_classes)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_array_wrapper(conn, wrapper, fmt_in):
    wrapper = getattr(range_module, wrapper)
    r1 = wrapper(empty=True)
    r2 = wrapper(bounds="()")
    cur = conn.execute(f"""select '{{empty,"(,)"}}' = %{fmt_in.value}""", ([r1, r2],))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype, min, max, bounds", samples)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_range(conn, pgtype, min, max, bounds, fmt_in):
    r = Range(min, max, bounds)  # type: ignore[var-annotated]
    sub = type2sub[pgtype]
    cur = conn.execute(
        f"select {pgtype}(%s::{sub}, %s::{sub}, %s) = %{fmt_in.value}",
        (min, max, bounds, r),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_empty(conn, pgtype, fmt_out):
    r = Range(empty=True)  # type: ignore[var-annotated]
    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute(f"select 'empty'::{pgtype}").fetchone()
    assert type(got) is Range
    assert got == r
    assert not got
    assert got.isempty


@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_inf(conn, pgtype, fmt_out):
    r = Range(bounds="()")  # type: ignore[var-annotated]
    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute(f"select '(,)'::{pgtype}").fetchone()
    assert type(got) is Range
    assert got == r
    assert got
    assert not got.isempty
    assert got.lower_inf
    assert got.upper_inf


@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_array(conn, pgtype, fmt_out):
    r1 = Range(empty=True)  # type: ignore[var-annotated]
    r2 = Range(bounds="()")  # type: ignore[var-annotated]
    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute(f"select array['empty'::{pgtype}, '(,)'::{pgtype}]").fetchone()
    assert got == [r1, r2]


@pytest.mark.parametrize("pgtype, min, max, bounds", samples)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_range(conn, pgtype, min, max, bounds, fmt_out):
    r = Range(min, max, bounds)  # type: ignore[var-annotated]
    sub = type2sub[pgtype]
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select {pgtype}(%s::{sub}, %s::{sub}, %s)", (min, max, bounds))
    # normalise discrete ranges
    if r.upper_inc and isinstance(r.upper, int):
        bounds = "[)" if r.lower_inc else "()"
        r = type(r)(r.lower, r.upper + 1, bounds)
    assert cur.fetchone()[0] == r


@pytest.mark.parametrize(
    "min, max, bounds",
    [
        ("2000,1,1", "2001,1,1", "[)"),
        ("2000,1,1", None, "[)"),
        (None, "2001,1,1", "()"),
        (None, None, "()"),
        (None, None, "empty"),
    ],
)
@pytest.mark.parametrize("format", pq.Format)
def test_copy_in(conn, min, max, bounds, format):
    cur = conn.cursor()
    cur.execute("create table copyrange (id serial primary key, r daterange)")

    if bounds != "empty":
        min = dt.date(*map(int, min.split(","))) if min else None
        max = dt.date(*map(int, max.split(","))) if max else None
        r = Range[dt.date](min, max, bounds)
    else:
        r = Range(empty=True)

    try:
        with cur.copy(f"copy copyrange (r) from stdin (format {format.name})") as copy:
            copy.write_row([r])
    except e.ProtocolViolation:
        if not min and not max and format == pq.Format.BINARY:
            pytest.xfail("TODO: add annotation to dump ranges with no type info")
        else:
            raise

    rec = cur.execute("select r from copyrange order by id").fetchone()
    assert rec[0] == r


@pytest.mark.parametrize("bounds", "() empty".split())
@pytest.mark.parametrize("wrapper", range_classes)
@pytest.mark.parametrize("format", pq.Format)
def test_copy_in_empty_wrappers(conn, bounds, wrapper, format):
    cur = conn.cursor()
    cur.execute("create table copyrange (id serial primary key, r daterange)")

    cls = getattr(range_module, wrapper)
    r = cls(empty=True) if bounds == "empty" else cls(None, None, bounds)

    with cur.copy(f"copy copyrange (r) from stdin (format {format.name})") as copy:
        copy.write_row([r])

    rec = cur.execute("select r from copyrange order by id").fetchone()
    assert rec[0] == r


@pytest.mark.parametrize("bounds", "() empty".split())
@pytest.mark.parametrize("pgtype", range_names)
@pytest.mark.parametrize("format", pq.Format)
def test_copy_in_empty_set_type(conn, bounds, pgtype, format):
    cur = conn.cursor()
    cur.execute(f"create table copyrange (id serial primary key, r {pgtype})")

    if bounds == "empty":
        r = Range(empty=True)  # type: ignore[var-annotated]
    else:
        r = Range(None, None, bounds)

    with cur.copy(f"copy copyrange (r) from stdin (format {format.name})") as copy:
        copy.set_types([pgtype])
        copy.write_row([r])

    rec = cur.execute("select r from copyrange order by id").fetchone()
    assert rec[0] == r


@pytest.fixture(scope="session")
def testrange(svcconn):
    create_test_range(svcconn)


def create_test_range(conn):
    if is_crdb(conn):
        pytest.skip(crdb_skip_message("range"))

    conn.execute(
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
    (sql.Identifier("testrange"), "text"),
    (sql.Identifier("testschema", "testrange"), "float8"),
]


@pytest.mark.parametrize("name, subtype", fetch_cases)
def test_fetch_info(conn, testrange, name, subtype):
    info = RangeInfo.fetch(conn, name)
    assert info.name == "testrange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == conn.adapters.types[subtype].oid


def test_fetch_info_not_found(conn):
    assert RangeInfo.fetch(conn, "nosuchrange") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("name, subtype", fetch_cases)
async def test_fetch_info_async(aconn, testrange, name, subtype):
    info = await RangeInfo.fetch(aconn, name)
    assert info.name == "testrange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == aconn.adapters.types[subtype].oid


@pytest.mark.asyncio
async def test_fetch_info_not_found_async(aconn):
    assert await RangeInfo.fetch(aconn, "nosuchrange") is None


def test_dump_custom_empty(conn, testrange):
    info = RangeInfo.fetch(conn, "testrange")
    register_range(info, conn)

    r = Range[str](empty=True)
    cur = conn.execute("select 'empty'::testrange = %s", (r,))
    assert cur.fetchone()[0] is True


def test_dump_quoting(conn, testrange):
    info = RangeInfo.fetch(conn, "testrange")
    register_range(info, conn)
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


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_custom_empty(conn, testrange, fmt_out):
    info = RangeInfo.fetch(conn, "testrange")
    register_range(info, conn)

    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute("select 'empty'::testrange").fetchone()
    assert isinstance(got, Range)
    assert got.isempty


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_quoting(conn, testrange, fmt_out):
    info = RangeInfo.fetch(conn, "testrange")
    register_range(info, conn)
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 254):
        cur.execute(
            "select testrange(chr(%(low)s::int), chr(%(up)s::int))",
            {"low": i, "up": i + 1},
        )
        got: Range[str] = cur.fetchone()[0]
        assert isinstance(got, Range)
        assert got.lower and ord(got.lower) == i
        assert got.upper and ord(got.upper) == i + 1


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_mixed_array_types(conn, fmt_out):
    conn.execute("create table testmix (a daterange[], b tstzrange[])")
    r1 = Range(dt.date(2000, 1, 1), dt.date(2001, 1, 1), "[)")
    r2 = Range(
        dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2001, 1, 1, tzinfo=dt.timezone.utc),
        "[)",
    )
    conn.execute("insert into testmix values (%s, %s)", [[r1], [r2]])
    got = conn.execute("select * from testmix").fetchone()
    assert got == ([r1], [r2])


class TestRangeObject:
    def test_noparam(self):
        r = Range()  # type: ignore[var-annotated]

        assert not r.isempty
        assert r.lower is None
        assert r.upper is None
        assert r.lower_inf
        assert r.upper_inf
        assert not r.lower_inc
        assert not r.upper_inc

    def test_empty(self):
        r = Range(empty=True)  # type: ignore[var-annotated]

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
            assert r.bounds == bounds
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
        r = Range[int](empty=True)
        assert 10 not in r
        assert "x" not in r  # type: ignore[operator]

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

    def test_exclude_inf_bounds(self):
        r = Range(None, 10, "[]")
        assert r.lower is None
        assert not r.lower_inc
        assert r.bounds == "(]"

        r = Range(10, None, "[]")
        assert r.upper is None
        assert not r.upper_inc
        assert r.bounds == "[)"

        r = Range(None, None, "[]")
        assert r.lower is None
        assert not r.lower_inc
        assert r.upper is None
        assert not r.upper_inc
        assert r.bounds == "()"


def test_no_info_error(conn):
    with pytest.raises(TypeError, match="range"):
        register_range(None, conn)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["a-b", f"{eur}", "order"])
def test_literal_invalid_name(conn, name):
    conn.execute("set client_encoding to utf8")
    conn.execute(f'create type "{name}" as range (subtype = text)')
    info = RangeInfo.fetch(conn, f'"{name}"')
    register_range(info, conn)
    obj = Range("a", "z", "[]")
    assert sql.Literal(obj).as_string(conn) == f"'[a,z]'::\"{name}\""
    cur = conn.execute(sql.SQL("select {}").format(obj))
    assert cur.fetchone()[0] == obj
