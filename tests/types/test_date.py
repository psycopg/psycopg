import datetime as dt
import pytest

from psycopg3.adapt import Format


#
# date tests
#


def as_date(s):
    return (
        dt.date(*map(int, s.split(","))) if "," in s else getattr(dt.date, s)
    )


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
    cur.execute("select '{expr}'::date" % expr)
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
    with pytest.raises(ValueError):
        cur.fetchone()[0]


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_too_large(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select %s + 1", (dt.date.max,))
    with pytest.raises(ValueError):
        cur.fetchone()[0]


#
# datetime tests
#


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
        tzoff = dt.timedelta(
            **dict(
                zip(("hours", "minutes", "seconds"), map(int, off.split(":")))
            )
        )
        rv = rv.replace(tzinfo=dt.timezone(tzoff))

    return rv


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
    cur.execute("select %s = %%b" % expr, (as_dt(val),))
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


#
# datetime+tz tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        ("min~2", "0001-01-01 00:00"),
        ("1000,1,1,0,0~2", "1000-01-01 00:00+2"),
        ("2000,1,1,0,0~2", "2000-01-01 00:00+2"),
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


def as_time(s):
    return (
        dt.time(*map(int, s.split(","))) if "," in s else getattr(dt.time, s)
    )


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
@pytest.mark.parametrize("val, expr", [(dt.time(0, 0), "00:00")])
def test_dump_time_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::time = %b", (val,))
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
    cur.execute("select '{expr}'::time" % expr)
    assert cur.fetchone()[0] == as_time(val)


def test_load_time_24(conn):
    cur = conn.cursor()
    cur.execute("select '24:00'::time")
    with pytest.raises(ValueError):
        cur.fetchone()[0]
