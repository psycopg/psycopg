import datetime as dt
import pytest

from psycopg3.adapt import Format


#
# date tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.date.min, "0001-01-01"),
        (dt.date(1000, 1, 1), "1000-01-01"),
        (dt.date(2000, 1, 1), "2000-01-01"),
        (dt.date(2000, 12, 31), "2000-12-31"),
        (dt.date(3000, 1, 1), "3000-01-01"),
        (dt.date.max, "9999-12-31"),
    ],
)
def test_dump_date(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::date = %s", (val,))
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
        (dt.date.min, "0001-01-01"),
        (dt.date(1000, 1, 1), "1000-01-01"),
        (dt.date(2000, 1, 1), "2000-01-01"),
        (dt.date(2000, 12, 31), "2000-12-31"),
        (dt.date(3000, 1, 1), "3000-01-01"),
        (dt.date.max, "9999-12-31"),
    ],
)
def test_load_date(conn, val, expr):
    cur = conn.cursor()
    cur.execute(f"select '{expr}'::date")
    assert cur.fetchone()[0] == val


@pytest.mark.xfail  # TODO: binary load
@pytest.mark.parametrize("val, expr", [(dt.date(2000, 1, 1), "2000-01-01")])
def test_load_date_binary(conn, val, expr):
    cur = conn.cursor(format=Format.BINARY)
    cur.execute("select '{expr}'::date" % expr)
    assert cur.fetchone()[0] == val


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


@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.datetime.min, "0001-01-01 00:00"),
        (dt.datetime(1000, 1, 1, 0, 0), "1000-01-01 00:00"),
        (dt.datetime(2000, 1, 1, 0, 0), "2000-01-01 00:00"),
        (
            dt.datetime(2000, 12, 31, 23, 59, 59, 999999),
            "2000-12-31 23:59:59.999999",
        ),
        (dt.datetime(3000, 1, 1, 0, 0), "3000-01-01 00:00"),
        (dt.datetime.max, "9999-12-31 23:59:59.999999"),
    ],
)
def test_dump_datetime(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select '{expr}'::timestamp = %s", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize(
    "val, expr",
    [(dt.datetime(2000, 1, 1, 0, 0), "'2000-01-01 00:00'::timestamp")],
)
def test_dump_datetime_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '+02:00'")
    cur.execute("select %s = %%b" % expr, (val,))
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
    val = (
        dt.datetime(*map(int, val.split(",")))
        if "," in val
        else getattr(dt.datetime, val)
    )
    cur.execute("set timezone to '+02:00'")
    cur.execute(f"select '{expr}'::timestamp")
    assert cur.fetchone()[0] == val


#
# datetime+tz tests
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.datetime.min, "0001-01-01 00:00"),
        (dt.datetime(1000, 1, 1, 0, 0), "1000-01-01 00:00+2"),
        (dt.datetime(2000, 1, 1, 0, 0), "2000-01-01 00:00+2"),
        (
            dt.datetime(2000, 12, 31, 23, 59, 59, 999999),
            "2000-12-31 23:59:59.999999+2",
        ),
        (dt.datetime(3000, 1, 1, 0, 0), "3000-01-01 00:00+2"),
        (dt.datetime.max, "9999-12-31 23:59:59.999999"),
    ],
)
def test_dump_datetimetz(conn, val, expr):
    val = val.replace(tzinfo=dt.timezone(dt.timedelta(hours=2)))
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz = %s", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize(
    "val, expr",
    [(dt.datetime(2000, 1, 1, 0, 0), "2000-01-01 00:00")],
)
def test_dump_datetimetz_binary(conn, val, expr):
    val = val.replace(tzinfo=dt.timezone(dt.timedelta(hours=2)))
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute(f"select '{expr}'::timestamptz = %b", (val,))
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
