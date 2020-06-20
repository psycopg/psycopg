import datetime

import pytest

from psycopg3.adapt import Loader, Transformer, Format
from psycopg3.types import builtins


#
# Tests with datetime
#

@pytest.mark.parametrize(
    "val, expr",
    [
        (
            datetime.datetime(year=1997, month=12, day=17, hour=7, minute=37, second=16),
            "'1997-12-17 07:37:16'::timestamp"
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
    [
        (datetime.date(year=1, month=1, day=1), "'0001-01-01'::date"),
    ],
)
def test_dump_date(conn, val, expr):
    assert isinstance(val, datetime.date)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, expr",
    [
        (datetime.time(hour=4, minute=5, second=6), "'04:05:06'::time"),
    ],
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
            "'8 1:01:01'::interval"
        ),
        (
            datetime.timedelta(weeks=53, days=1, hours=1, minutes=1, seconds=1),
            "'372 1:01:01'::interval"
        ),
    ],
)
def test_dump_timedelta(conn, val, expr):
    assert isinstance(val, datetime.timedelta)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]

