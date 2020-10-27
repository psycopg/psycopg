import datetime as dt
import pytest

from psycopg3 import DataError
from psycopg3.adapt import Format


#
# date tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "0001-01-01"),
        ("1000,1,1", "1000-01-01"),
        ("2000,1,1", "2000-01-01"),
        ("2000,12,31", "2000-12-31"),
        ("3000,1,1", "3000-01-01"),
        ("max", "9999-12-31"),
    ],
)
def test_dump_date(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::date = %s", (as_date(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize("val, expr", [(dt.date(2000, 1, 1), "2000-01-01")])
def test_dump_date_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::date = %b", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_dump_date_datestyle(conn, datestyle_in):
    cur = conn.cursor()
    cur.execute(f"set datestyle = ISO, {datestyle_in}")
    cur.execute("select 'epoch'::date + 1 = %s", (dt.date(1970, 1, 2),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "0001-01-01"),
        ("1000,1,1", "1000-01-01"),
        ("2000,1,1", "2000-01-01"),
        ("2000,12,31", "2000-12-31"),
        ("3000,1,1", "3000-01-01"),
        ("max", "9999-12-31"),
    ],
)
def test_load_date(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::date")
    assert cur.fetchone()[0] == as_date(val)


@pytest.mark.xfail  # TODO: binary load
@pytest.mark.parametrize("val, expr", [("2000,1,1", "2000-01-01")])
def test_load_date_binary(conn, val, expr):
    cur = conn.cursor(format=Format.BINARY)
    cur.execute(f"select '{expr}'::date")
    assert cur.fetchone()[0] == as_date(val)


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_datestyle(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select '2000-01-02'::date")
    assert cur.fetchone()[0] == dt.date(2000, 1, 2)


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_bc(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select %s - 1", (dt.date.min,))
    with pytest.raises(DataError):
        cur.fetchone()[0]


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_too_large(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select %s + 1", (dt.date.max,))
    with pytest.raises(DataError):
        cur.fetchone()[0]


#
# datetime tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "0001-01-01 00:00"),
        ("1000,1,1,0,0", "1000-01-01 00:00"),
        ("2000,1,1,0,0", "2000-01-01 00:00"),
        ("2000,12,31,23,59,59,999999", "2000-12-31 23:59:59.999999"),
        ("3000,1,1,0,0", "3000-01-01 00:00"),
        ("max", "9999-12-31 23:59:59.999999"),
    ],
)
def test_dump_datetime(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select '{expr}'::timestamp = %s", (as_dt(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize(
    "val, expr",
    [("2000,1,1,0,0", "'2000-01-01 00:00'::timestamp")],
)
def test_dump_datetime_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select {expr} = %b", (as_dt(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_dump_datetime_datestyle(conn, datestyle_in):
    cur = conn.cursor()
    cur.execute(f"set datestyle = ISO, {datestyle_in}")
    cur.execute(
        "select 'epoch'::timestamp + '1d 3h 4m 5s'::interval = %s",
        (dt.datetime(1970, 1, 2, 3, 4, 5),),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "0001-01-01"),
        ("1000,1,1", "1000-01-01"),
        ("2000,1,1", "2000-01-01"),
        ("2000,1,2,3,4,5,6", "2000-01-02 03:04:05.000006"),
        ("2000,1,2,3,4,5,678", "2000-01-02 03:04:05.000678"),
        ("2000,1,2,3,0,0,456789", "2000-01-02 03:00:00.456789"),
        ("2000,12,31", "2000-12-31"),
        ("3000,1,1", "3000-01-01"),
        ("max", "9999-12-31 23:59:59.999999"),
    ],
)
@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_load_datetime(conn, val, expr, datestyle_in, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, {datestyle_in}")
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select '{expr}'::timestamp")
    assert cur.fetchone()[0] == as_dt(val)


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_datetime_bc(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select %s - '1s'::interval", (dt.datetime.min,))
    with pytest.raises(DataError):
        cur.fetchone()[0]


@pytest.mark.parametrize("datestyle_out", ["ISO", "SQL", "Postgres", "German"])
def test_load_datetime_too_large(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select %s + '1s'::interval", (dt.datetime.max,))
    with pytest.raises(DataError):
        cur.fetchone()[0]


#
# datetime+tz tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min~2", "0001-01-01 00:00"),
        ("min~-12", "0001-01-01 00:00-12:00"),
        ("min~+12", "0001-01-01 00:00+12:00"),
        ("1000,1,1,0,0~2", "1000-01-01 00:00+2"),
        ("2000,1,1,0,0~2", "2000-01-01 00:00+2"),
        ("2000,1,1,0,0~12", "2000-01-01 00:00+12"),
        ("2000,1,1,0,0~-12", "2000-01-01 00:00-12"),
        ("2000,1,1,0,0~01:02:03", "2000-01-01 00:00+01:02:03"),
        ("2000,1,1,0,0~-01:02:03", "2000-01-01 00:00-01:02:03"),
        ("2000,12,31,23,59,59,999999~2", "2000-12-31 23:59:59.999999+2"),
        ("3000,1,1,0,0~2", "3000-01-01 00:00+2"),
        ("max~2", "9999-12-31 23:59:59.999999"),
    ],
)
def test_dump_datetimetz(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz = %s", (as_dt(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize("val, expr", [("2000,1,1,0,0~2", "2000-01-01 00:00")])
def test_dump_datetimetz_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz = %b", (as_dt(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_dump_datetimetz_datestyle(conn, datestyle_in):
    tzinfo = dt.timezone(dt.timedelta(hours=2))
    cur = conn.cursor()
    cur.execute(f"set datestyle = ISO, {datestyle_in}")
    cur.execute("set timezone to '-02:00'")
    cur.execute(
        "select 'epoch'::timestamptz + '1d 3h 4m 5.678s'::interval = %s",
        (dt.datetime(1970, 1, 2, 5, 4, 5, 678000, tzinfo=tzinfo),),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr, timezone",
    [
        ("2000,1,1~2", "2000-01-01", "-02:00"),
        ("2000,1,2,3,4,5,6~2", "2000-01-02 03:04:05.000006", "-02:00"),
        ("2000,1,2,3,4,5,678~1", "2000-01-02 03:04:05.000678", "Europe/Rome"),
        ("2000,7,2,3,4,5,678~2", "2000-07-02 03:04:05.000678", "Europe/Rome"),
        ("2000,1,2,3,0,0,456789~2", "2000-01-02 03:00:00.456789", "-02:00"),
        ("2000,1,2,3,0,0,456789~-2", "2000-01-02 03:00:00.456789", "+02:00"),
        ("2000,12,31~2", "2000-12-31", "-02:00"),
        ("1900,1,1~05:21:10", "1900-01-01", "Asia/Calcutta"),
    ],
)
@pytest.mark.parametrize("datestyle_out", ["ISO"])
def test_load_datetimetz(conn, val, expr, timezone, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, DMY")
    cur.execute(f"set timezone to '{timezone}'")
    cur.execute(f"select '{expr}'::timestamptz")
    assert cur.fetchone()[0] == as_dt(val)


@pytest.mark.xfail  # parse timezone names
@pytest.mark.parametrize("val, expr", [("2000,1,1~2", "2000-01-01")])
@pytest.mark.parametrize("datestyle_out", ["SQL", "Postgres", "German"])
@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_load_datetimetz_tzname(conn, val, expr, datestyle_in, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, {datestyle_in}")
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz")
    assert cur.fetchone()[0] == as_dt(val)


#
# time tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "00:00"),
        ("10,20,30,40", "10:20:30.000040"),
        ("max", "23:59:59.999999"),
    ],
)
def test_dump_time(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::time = %s", (as_time(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize("val, expr", [("0,0", "00:00")])
def test_dump_time_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::time = %b", (as_time(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "00:00"),
        ("1,2", "01:02"),
        ("10,20", "10:20"),
        ("10,20,30", "10:20:30"),
        ("10,20,30,40", "10:20:30.000040"),
        ("max", "23:59:59.999999"),
    ],
)
def test_load_time(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::time")
    assert cur.fetchone()[0] == as_time(val)


@pytest.mark.xfail  # TODO: binary load
@pytest.mark.parametrize("val, expr", [("0,0", "00:00")])
def test_load_time_binary(conn, val, expr):
    cur = conn.cursor(format=Format.BINARY)
    cur.execute(f"select '{expr}'::time")
    assert cur.fetchone()[0] == as_time(val)


def test_load_time_24(conn):
    cur = conn.cursor()
    cur.execute("select '24:00'::time")
    with pytest.raises(DataError):
        cur.fetchone()[0]


#
# time+tz tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min~-10", "00:00-10:00"),
        ("min~+12", "00:00+12:00"),
        ("10,20,30,40~-2", "10:20:30.000040-02:00"),
        ("10,20,30,40~0", "10:20:30.000040Z"),
        ("10,20,30,40~+2:30", "10:20:30.000040+02:30"),
        ("max~-12", "23:59:59.999999-12:00"),
        ("max~+12", "23:59:59.999999+12:00"),
    ],
)
def test_dump_timetz(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timetz = %s", (as_time(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize("val, expr", [("0,0~0", "00:00Z")])
def test_dump_timetz_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::time = %b", (as_time(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr, timezone",
    [
        ("0,0~-12", "00:00", "12:00"),
        ("0,0~12", "00:00", "-12:00"),
        ("3,4,5,6~2", "03:04:05.000006", "-02:00"),
        ("3,4,5,6~7:8", "03:04:05.000006", "-07:08"),
        ("3,0,0,456789~2", "03:00:00.456789", "-02:00"),
        ("3,0,0,456789~-2", "03:00:00.456789", "+02:00"),
    ],
)
def test_load_timetz(conn, val, timezone, expr):
    cur = conn.cursor()
    cur.execute(f"set timezone to '{timezone}'")
    cur.execute(f"select '{expr}'::timetz")
    assert cur.fetchone()[0] == as_time(val)


@pytest.mark.xfail  # TODO: binary load
@pytest.mark.parametrize("val, expr, timezone", [("0,0~2", "00:00", "-02:00")])
def test_load_timetz_binary(conn, val, expr, timezone):
    cur = conn.cursor(format=Format.BINARY)
    cur.execute(f"set timezone to '{timezone}'")
    cur.execute(f"select '{expr}'::time")
    assert cur.fetchone()[0] == as_time(val)


def test_load_timetz_24(conn):
    cur = conn.cursor()
    cur.execute("select '24:00'::timetz")
    with pytest.raises(DataError):
        cur.fetchone()[0]


#
# Interval
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "-999999999 days"),
        ("1d", "1 day"),
        ("-1d", "-1 day"),
        ("1s", "1 s"),
        ("-1s", "-1 s"),
        ("-1m", "-0.000001 s"),
        ("1m", "0.000001 s"),
        ("max", "999999999 days 23:59:59.999999"),
    ],
)
@pytest.mark.parametrize(
    "intervalstyle",
    ["sql_standard", "postgres", "postgres_verbose", "iso_8601"],
)
def test_dump_interval(conn, val, expr, intervalstyle):
    cur = conn.cursor()
    cur.execute(f"set IntervalStyle to '{intervalstyle}'")
    cur.execute(f"select '{expr}'::interval = %s", (as_td(val),))
    assert cur.fetchone()[0] is True


#
# Support
#


def as_date(s):
    return (
        dt.date(*map(int, s.split(","))) if "," in s else getattr(dt.date, s)
    )


def as_time(s):
    if "~" in s:
        s, off = s.split("~")
    else:
        off = None

    rv = dt.time(*map(int, s.split(","))) if "," in s else getattr(dt.time, s)
    if off:
        rv = rv.replace(tzinfo=as_tzinfo(off))

    return rv


def as_dt(s):
    if "~" in s:
        s, off = s.split("~")
    else:
        off = None

    if "," in s:
        rv = dt.datetime(*map(int, s.split(",")))
    else:
        rv = getattr(dt.datetime, s)

    if off:
        rv = rv.replace(tzinfo=as_tzinfo(off))

    return rv


def as_tzinfo(s):
    if s.startswith("-"):
        mul = -1
        s = s[1:]
    else:
        mul = 1

    tzoff = mul * dt.timedelta(
        **dict(zip(("hours", "minutes", "seconds"), map(int, s.split(":"))))
    )
    return dt.timezone(tzoff)


def as_td(s):
    if s in ("min", "max"):
        return getattr(dt.timedelta, s)

    suffixes = {"d": "days", "s": "seconds", "m": "microseconds"}
    kwargs = {}
    for part in s.split(","):
        kwargs[suffixes[part[-1]]] = int(part[:-1])

    return dt.timedelta(**kwargs)
