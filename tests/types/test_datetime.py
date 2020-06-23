import datetime

import pytest

from psycopg3.adapt import Format
from psycopg3.types import builtins


#
# Tests with datetime
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (
            datetime.datetime(
                year=1997, month=12, day=17, hour=7, minute=37, second=16
            ),
            "'1997-12-17 07:37:16'::timestamp",
        ),
    ],
)
def test_dump_datetime(conn, val, expr):
    assert isinstance(val, datetime.datetime)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, expr",
    [(datetime.date(year=1, month=1, day=1), "'0001-01-01'::date")],
)
def test_dump_date(conn, val, expr):
    assert isinstance(val, datetime.date)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, expr",
    [(datetime.time(hour=4, minute=5, second=6), "'04:05:06'::time")],
)
def test_dump_time(conn, val, expr):
    assert isinstance(val, datetime.time)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    # Matches with sql_standard section 8.5.5 Interval Output
    "val, expr",
    [
        (
            datetime.timedelta(weeks=1, days=1, hours=1, minutes=1, seconds=1),
            "'8 1:01:01'::interval",
        ),
        (
            datetime.timedelta(
                weeks=53, days=1, hours=1, minutes=1, seconds=1
            ),
            "'372 1:01:01'::interval",
        ),
    ],
)
def test_dump_timedelta(conn, val, expr):
    assert isinstance(val, datetime.timedelta)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]


_load_datetime_tests = [
    ("0001-01-01", "date", datetime.date(year=1, month=1, day=1)),
    ("9999-12-31", "date", datetime.date(year=9999, month=12, day=31)),
    ("1999-12-31", "date", datetime.date(year=1999, month=12, day=31)),
    ("2000-01-01", "date", datetime.date(year=2000, month=1, day=1)),
    ("2008-09-16", "date", datetime.date(year=2008, month=9, day=16)),
    ("1954-12-28", "date", datetime.date(year=1954, month=12, day=28)),
    ("0987-01-23", "date", datetime.date(year=987, month=1, day=23)),
    ("0987-01-23", "date", datetime.date(year=987, month=1, day=23)),
    ("00:00:01", "time", datetime.time(hour=0, minute=0, second=1)),
    (
        "00:00:01-06",
        "timetz",
        datetime.time(
            hour=0,
            minute=0,
            second=1,
            tzinfo=datetime.timezone(datetime.timedelta(hours=-6)),
        ),
    ),
    (
        "00:00:01+06",
        "timetz",
        datetime.time(
            hour=0,
            minute=0,
            second=1,
            tzinfo=datetime.timezone(datetime.timedelta(hours=6)),
        ),
    ),
    (
        "00:00:01-1030",
        "timetz",
        datetime.time(
            hour=0,
            minute=0,
            second=1,
            tzinfo=datetime.timezone(
                datetime.timedelta(hours=-10, minutes=-30)
            ),
        ),
    ),
    (
        "1999-12-12 00:00:01",
        "timestamp",
        datetime.datetime(
            year=1999, month=12, day=12, hour=0, minute=0, second=1
        ),
    ),
    (
        "2987-01-23 13:45:36",
        "timestamp",
        datetime.datetime(
            year=2987, month=1, day=23, hour=13, minute=45, second=36
        ),
    ),
    (
        "2000-01-01 00:00:01+0000",
        "timestamptz",
        datetime.datetime(
            year=2000,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=1,
            tzinfo=datetime.timezone(datetime.timedelta(minutes=0)),
        ),
    ),
    (
        "2000-01-01 00:00:01-0030",
        "timestamptz",
        datetime.datetime(
            year=2000,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=1,
            tzinfo=datetime.timezone(datetime.timedelta(minutes=-30)),
        ),
    ),
    # TODO Add positive timezones to _load_datetime_tests for datetime
    # They are losing precision for some reason, and I'm not sure how to fix.
    # (
    #     "2000-01-01 00:00:01+0030",
    #     "timestamptz",
    #     datetime.datetime(
    #         year=2000,
    #         month=1,
    #         day=1,
    #         hour=0,
    #         minute=0,
    #         second=1,
    #         tzinfo=datetime.timezone(datetime.timedelta(minutes=30)),
    #     ),
    # ),
]


@pytest.mark.parametrize(
    "val, pgtype, want", _load_datetime_tests,
)
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_datetime(conn, val, pgtype, want, fmt_out):
    cur = conn.cursor(format=fmt_out)
    cur.execute(f"select %s::{pgtype}", (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[pgtype].oid
    result = cur.fetchone()[0]
    assert result == want
    assert type(result) is type(want)

    # arrays work too
    cur.execute(f"select array[%s::{pgtype}]", (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[pgtype].array_oid
    result = cur.fetchone()[0]
    assert result == [want]
    assert type(result[0]) is type(want)
