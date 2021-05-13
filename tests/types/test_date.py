import datetime as dt

import pytest

from psycopg3 import DataError, pq, sql
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
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_date(conn, val, expr, fmt_in):
    val = as_date(val)
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::date = %{fmt_in}", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(
        sql.SQL("select {}::date = {}").format(
            sql.Literal(val), sql.Placeholder(format=fmt_in)
        ),
        (val,),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_dump_date_datestyle(conn, datestyle_in):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = ISO, {datestyle_in}")
    cur.execute("select 'epoch'::date + 1 = %t", (dt.date(1970, 1, 2),))
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
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_date(conn, val, expr, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select '{expr}'::date")
    assert cur.fetchone()[0] == as_date(val)


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_datestyle(conn, datestyle_out):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select '2000-01-02'::date")
    assert cur.fetchone()[0] == dt.date(2000, 1, 2)


@pytest.mark.parametrize("val", ["min", "max"])
@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_overflow(conn, val, datestyle_out):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute(
        "select %t + %s::int", (as_date(val), -1 if val == "min" else 1)
    )
    with pytest.raises(DataError):
        cur.fetchone()[0]


@pytest.mark.parametrize("val", ["min", "max"])
def test_load_date_overflow_binary(conn, val):
    cur = conn.cursor(binary=True)
    cur.execute(
        "select %s + %s::int", (as_date(val), -1 if val == "min" else 1)
    )
    with pytest.raises(DataError):
        cur.fetchone()[0]


#
# datetime tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min", "0001-01-01 00:00"),
        ("258,1,8,1,12,32,358261", "0258-1-8 1:12:32.358261"),
        ("1000,1,1,0,0", "1000-01-01 00:00"),
        ("2000,1,1,0,0", "2000-01-01 00:00"),
        ("2000,1,2,3,4,5,6", "2000-01-02 03:04:05.000006"),
        ("2000,1,2,3,4,5,678", "2000-01-02 03:04:05.000678"),
        ("2000,1,2,3,0,0,456789", "2000-01-02 03:00:00.456789"),
        ("2000,1,1,0,0,0,1", "2000-01-01 00:00:00.000001"),
        ("2200,1,1,0,0,0,1", "2200-01-01 00:00:00.000001"),
        ("2300,1,1,0,0,0,1", "2300-01-01 00:00:00.000001"),
        ("7000,1,1,0,0,0,1", "7000-01-01 00:00:00.000001"),
        ("max", "9999-12-31 23:59:59.999999"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_datetime(conn, val, expr, fmt_in):
    cur = conn.cursor()
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select %{fmt_in}", (as_dt(val),))
    print(cur.fetchone()[0])
    cur.execute(f"select '{expr}'::timestamp = %{fmt_in}", (as_dt(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_dump_datetime_datestyle(conn, datestyle_in):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = ISO, {datestyle_in}")
    cur.execute(
        "select 'epoch'::timestamp + '1d 3h 4m 5s'::interval = %t",
        (dt.datetime(1970, 1, 2, 3, 4, 5),),
    )
    assert cur.fetchone()[0] is True


load_datetime_samples = [
    ("min", "0001-01-01"),
    ("1000,1,1", "1000-01-01"),
    ("2000,1,1", "2000-01-01"),
    ("2000,1,2,3,4,5,6", "2000-01-02 03:04:05.000006"),
    ("2000,1,2,3,4,5,678", "2000-01-02 03:04:05.000678"),
    ("2000,1,2,3,0,0,456789", "2000-01-02 03:00:00.456789"),
    ("2000,12,31", "2000-12-31"),
    ("3000,1,1", "3000-01-01"),
    ("max", "9999-12-31 23:59:59.999999"),
]


@pytest.mark.parametrize("val, expr", load_datetime_samples)
@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_load_datetime(conn, val, expr, datestyle_in, datestyle_out):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = {datestyle_out}, {datestyle_in}")
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select '{expr}'::timestamp")
    assert cur.fetchone()[0] == as_dt(val)


@pytest.mark.parametrize("val, expr", load_datetime_samples)
def test_load_datetime_binary(conn, val, expr):
    cur = conn.cursor(binary=True)
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select '{expr}'::timestamp")
    assert cur.fetchone()[0] == as_dt(val)


@pytest.mark.parametrize("val", ["min", "max"])
@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_datetime_overflow(conn, val, datestyle_out):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute(
        "select %t::timestamp + %s * '1s'::interval",
        (as_dt(val), -1 if val == "min" else 1),
    )
    with pytest.raises(DataError):
        cur.fetchone()[0]


@pytest.mark.parametrize("val", ["min", "max"])
def test_load_datetime_overflow_binary(conn, val):
    cur = conn.cursor(binary=True)
    cur.execute(
        "select %t::timestamp + %s * '1s'::interval",
        (as_dt(val), -1 if val == "min" else 1),
    )
    with pytest.raises(DataError):
        cur.fetchone()[0]


#
# datetime+tz tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min~-2", "0001-01-01 00:00-02:00"),
        ("min~-12", "0001-01-01 00:00-12:00"),
        ("258,1,8,1,12,32,358261~1:2:3", "0258-1-8 1:12:32.358261+01:02:03"),
        ("1000,1,1,0,0~2", "1000-01-01 00:00+2"),
        ("2000,1,1,0,0~2", "2000-01-01 00:00+2"),
        ("2000,1,1,0,0~12", "2000-01-01 00:00+12"),
        ("2000,1,1,0,0~-12", "2000-01-01 00:00-12"),
        ("2000,1,1,0,0~01:02:03", "2000-01-01 00:00+01:02:03"),
        ("2000,1,1,0,0~-01:02:03", "2000-01-01 00:00-01:02:03"),
        ("2000,12,31,23,59,59,999999~2", "2000-12-31 23:59:59.999999+2"),
        ("2300,1,1,0,0,0,1~1", "2300-01-01 00:00:00.000001+1"),
        ("3000,1,1,0,0~2", "3000-01-01 00:00+2"),
        ("7000,1,1,0,0,0,1~-1:2:3", "7000-01-01 00:00:00.000001-01:02:03"),
        ("max~2", "9999-12-31 23:59:59.999999"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_datetimetz(conn, val, expr, fmt_in):
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz = %{fmt_in}", (as_dt(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_dump_datetimetz_datestyle(conn, datestyle_in):
    tzinfo = dt.timezone(dt.timedelta(hours=2))
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = ISO, {datestyle_in}")
    cur.execute("set timezone to '-02:00'")
    cur.execute(
        "select 'epoch'::timestamptz + '1d 3h 4m 5.678s'::interval = %t",
        (dt.datetime(1970, 1, 2, 5, 4, 5, 678000, tzinfo=tzinfo),),
    )
    assert cur.fetchone()[0] is True


load_datetimetz_samples = [
    ("2000,1,1~2", "2000-01-01", "-02:00"),
    ("2000,1,2,3,4,5,6~2", "2000-01-02 03:04:05.000006", "-02:00"),
    ("2000,1,2,3,4,5,678~1", "2000-01-02 03:04:05.000678", "Europe/Rome"),
    ("2000,7,2,3,4,5,678~2", "2000-07-02 03:04:05.000678", "Europe/Rome"),
    ("2000,1,2,3,0,0,456789~2", "2000-01-02 03:00:00.456789", "-02:00"),
    ("2000,1,2,3,0,0,456789~-2", "2000-01-02 03:00:00.456789", "+02:00"),
    ("2000,12,31~2", "2000-12-31", "-02:00"),
    ("1900,1,1~05:21:10", "1900-01-01", "Asia/Calcutta"),
]


@pytest.mark.parametrize("val, expr, timezone", load_datetimetz_samples)
@pytest.mark.parametrize("datestyle_out", ["ISO"])
def test_load_datetimetz(conn, val, expr, timezone, datestyle_out):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = {datestyle_out}, DMY")
    cur.execute(f"set timezone to '{timezone}'")
    got = cur.execute(f"select '{expr}'::timestamptz").fetchone()[0]
    assert got == as_dt(val)
    assert got.tzinfo == dt.timezone.utc


@pytest.mark.parametrize("val, expr, timezone", load_datetimetz_samples)
def test_load_datetimetz_binary(conn, val, expr, timezone):
    cur = conn.cursor(binary=True)
    cur.execute(f"set timezone to '{timezone}'")
    got = cur.execute(f"select '{expr}'::timestamptz").fetchone()[0]
    assert got == as_dt(val)
    assert got.tzinfo == dt.timezone.utc


@pytest.mark.xfail  # parse timezone names
@pytest.mark.parametrize("val, expr", [("2000,1,1~2", "2000-01-01")])
@pytest.mark.parametrize("datestyle_out", ["SQL", "Postgres", "German"])
@pytest.mark.parametrize("datestyle_in", ["DMY", "MDY", "YMD"])
def test_load_datetimetz_tzname(conn, val, expr, datestyle_in, datestyle_out):
    cur = conn.cursor(binary=False)
    cur.execute(f"set datestyle = {datestyle_out}, {datestyle_in}")
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz")
    assert cur.fetchone()[0] == as_dt(val)


@pytest.mark.parametrize(
    "val, type",
    [
        ("2000,1,2,3,4,5,6", "timestamp"),
        ("2000,1,2,3,4,5,6~0", "timestamptz"),
        ("2000,1,2,3,4,5,6~2", "timestamptz"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_datetime_tz_or_not_tz(conn, val, type, fmt_in):
    val = as_dt(val)
    cur = conn.cursor()
    cur.execute(
        f"select pg_typeof(%{fmt_in}) = %s::regtype, %{fmt_in}",
        [val, type, val],
    )
    rec = cur.fetchone()
    assert rec[0] is True, type
    assert rec[1] == val


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
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_time(conn, val, expr, fmt_in):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::time = %{fmt_in}", (as_time(val),))
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
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_time(conn, val, expr, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select '{expr}'::time")
    assert cur.fetchone()[0] == as_time(val)


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_time_24(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
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
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_timetz(conn, val, expr, fmt_in):
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timetz = %{fmt_in}", (as_time(val),))
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
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_timetz(conn, val, timezone, expr, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"set timezone to '{timezone}'")
    cur.execute(f"select '{expr}'::timetz")
    assert cur.fetchone()[0] == as_time(val)


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_timetz_24(conn, fmt_out):
    cur = conn.cursor()
    cur.execute("select '24:00'::timetz")
    with pytest.raises(DataError):
        cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, type",
    [
        ("3,4,5,6", "time"),
        ("3,4,5,6~0", "timetz"),
        ("3,4,5,6~2", "timetz"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_time_tz_or_not_tz(conn, val, type, fmt_in):
    val = as_time(val)
    cur = conn.cursor()
    cur.execute(
        f"select pg_typeof(%{fmt_in}) = %s::regtype, %{fmt_in}",
        [val, type, val],
    )
    rec = cur.fetchone()
    assert rec[0] is True, type
    assert rec[1] == val


#
# Interval
#

dump_timedelta_samples = [
    ("min", "-999999999 days"),
    ("1d", "1 day"),
    ("-1d", "-1 day"),
    ("1s", "1 s"),
    ("-1s", "-1 s"),
    ("-1m", "-0.000001 s"),
    ("1m", "0.000001 s"),
    ("max", "999999999 days 23:59:59.999999"),
]


@pytest.mark.parametrize("val, expr", dump_timedelta_samples)
@pytest.mark.parametrize(
    "intervalstyle",
    ["sql_standard", "postgres", "postgres_verbose", "iso_8601"],
)
def test_dump_interval(conn, val, expr, intervalstyle):
    cur = conn.cursor(binary=False)
    cur.execute(f"set IntervalStyle to '{intervalstyle}'")
    cur.execute(f"select '{expr}'::interval = %t", (as_td(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("val, expr", dump_timedelta_samples)
def test_dump_interval_binary(conn, val, expr):
    cur = conn.cursor(binary=True)
    cur.execute(f"select '{expr}'::interval = %t", (as_td(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        ("1s", "1 sec"),
        ("-1s", "-1 sec"),
        ("60s", "1 min"),
        ("3600s", "1 hour"),
        ("1s,1000m", "1.001 sec"),
        ("1s,1m", "1.000001 sec"),
        ("1d", "1 day"),
        ("-10d", "-10 day"),
        ("1d,1s,1m", "1 day 1.000001 sec"),
        ("-86399s,-999999m", "-23:59:59.999999"),
        ("-3723s,-400000m", "-1:2:3.4"),
        ("3723s,400000m", "1:2:3.4"),
        ("86399s,999999m", "23:59:59.999999"),
        ("30d", "30 day"),
        ("365d", "1 year"),
        ("-365d", "-1 year"),
        ("-730d", "-2 years"),
        ("1460d", "4 year"),
        ("30d", "1 month"),
        ("-30d", "-1 month"),
        ("60d", "2 month"),
        ("-90d", "-3 month"),
    ],
)
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_interval(conn, val, expr, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select '{expr}'::interval")
    assert cur.fetchone()[0] == as_td(val)


@pytest.mark.xfail  # weird interval outputs
@pytest.mark.parametrize("val, expr", [("1d,1s", "1 day 1 sec")])
@pytest.mark.parametrize(
    "intervalstyle",
    ["sql_standard", "postgres_verbose", "iso_8601"],
)
def test_load_interval_intervalstyle(conn, val, expr, intervalstyle):
    cur = conn.cursor(binary=False)
    cur.execute(f"set IntervalStyle to '{intervalstyle}'")
    cur.execute(f"select '{expr}'::interval")
    assert cur.fetchone()[0] == as_td(val)


@pytest.mark.parametrize("val", ["min", "max"])
def test_load_interval_overflow(conn, val):
    cur = conn.cursor()
    cur.execute(
        "select %s + %s * '1s'::interval",
        (as_td(val), -1 if val == "min" else 1),
    )
    with pytest.raises(DataError):
        cur.fetchone()[0]


def test_infinity_date_example(conn):
    # NOTE: this is an example in the docs. Make sure it doesn't regress when
    # adding binary datetime adapters
    from psycopg3.oids import postgres_types as builtins
    from psycopg3.types import DateLoader, DateDumper

    class InfDateDumper(DateDumper):
        def dump(self, obj):
            if obj == dt.date.max:
                return b"infinity"
            else:
                return super().dump(obj)

    class InfDateLoader(DateLoader):
        def load(self, data):
            if data == b"infinity":
                return dt.date.max
            else:
                return super().load(data)

    cur = conn.cursor()
    InfDateDumper.register(dt.date, cur)
    InfDateLoader.register(builtins["date"].oid, cur)

    rec = cur.execute(
        "SELECT %s::text, %s::text", [dt.date(2020, 12, 31), dt.date.max]
    ).fetchone()
    assert rec == ("2020-12-31", "infinity")
    rec = cur.execute("select '2020-12-31'::date, 'infinity'::date").fetchone()
    assert rec == (dt.date(2020, 12, 31), dt.date(9999, 12, 31))


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
    if "~" not in s:
        return as_naive_dt(s)

    s, off = s.split("~")
    rv = as_naive_dt(s)
    off = as_tzoffset(off)
    rv = (rv - off).replace(tzinfo=dt.timezone.utc)
    return rv


def as_naive_dt(s):
    if "," in s:
        rv = dt.datetime(*map(int, s.split(",")))
    else:
        rv = getattr(dt.datetime, s)

    return rv


def as_tzoffset(s):
    if s.startswith("-"):
        mul = -1
        s = s[1:]
    else:
        mul = 1

    fields = ("hours", "minutes", "seconds")
    return mul * dt.timedelta(**dict(zip(fields, map(int, s.split(":")))))


def as_tzinfo(s):
    off = as_tzoffset(s)
    return dt.timezone(off)


def as_td(s):
    if s in ("min", "max"):
        return getattr(dt.timedelta, s)

    suffixes = {"d": "days", "s": "seconds", "m": "microseconds"}
    kwargs = {}
    for part in s.split(","):
        kwargs[suffixes[part[-1]]] = int(part[:-1])

    return dt.timedelta(**kwargs)
