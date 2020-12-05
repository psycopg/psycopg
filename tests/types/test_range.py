import pytest

from psycopg3.types import range


type2cls = {
    "int4range": range.Int4Range,
    "int8range": range.Int8Range,
    "numrange": range.DecimalRange,
    "daterange": range.DateRange,
    "tsrange": range.DateTimeRange,
    "tstzrange": range.DateTimeTZRange,
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
def test_dump_builtin_range_empty(conn, pgtype):
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
def test_load_builtin_range_empty(conn, pgtype):
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
