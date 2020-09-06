import datetime as dt
import pytest

import psycopg3
from psycopg3.adapt import Format


#
# date tests
#

@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.date.min, "'0001-01-01'::date"),
        (dt.date(1000, 1, 1), "'1000-01-01'::date"),
        (dt.date(2000, 1, 1), "'2000-01-01'::date"),
        (dt.date(2000, 12, 31), "'2000-12-31'::date"),
        (dt.date(3000, 1, 1), "'3000-01-01'::date"),
        (dt.date.max, "'9999-12-31'::date"),
    ],
)
def test_dump_date(conn, val, expr):
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize(
    "val, expr", [(dt.date(2000, 1, 1), "'2000-01-01'::date")]
)
def test_dump_date_binary(conn, val, expr):
    cur = conn.cursor()
    cur.execute("select %s = %%b" % expr, (val,))
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
        (dt.date.min, "'0001-01-01'::date"),
        (dt.date(1000, 1, 1), "'1000-01-01'::date"),
        (dt.date(2000, 1, 1), "'2000-01-01'::date"),
        (dt.date(2000, 12, 31), "'2000-12-31'::date"),
        (dt.date(3000, 1, 1), "'3000-01-01'::date"),
        (dt.date.max, "'9999-12-31'::date"),
    ],
)
def test_load_date(conn, val, expr):
    cur = conn.cursor()
    cur.execute("select %s" % expr)
    assert cur.fetchone()[0] == val


@pytest.mark.xfail  # TODO: binary load
@pytest.mark.parametrize(
    "val, expr", [(dt.date(2000, 1, 1), "'2000-01-01'::date")],
)
def test_load_date_binary(conn, val, expr):
    cur = conn.cursor(format=Format.BINARY)
    cur.execute("select %s" % expr)
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
    with pytest.raises(psycopg3.InterfaceError):
        cur.fetchone()[0]


@pytest.mark.parametrize("datestyle_out", ["ISO", "Postgres", "SQL", "German"])
def test_load_date_too_large(conn, datestyle_out):
    cur = conn.cursor()
    cur.execute(f"set datestyle = {datestyle_out}, YMD")
    cur.execute("select %s + 1", (dt.date.max,))
    with pytest.raises(psycopg3.InterfaceError):
        cur.fetchone()[0]


#
# datetime tests
#

@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.datetime.min, "'0001-01-01 00:00'::timestamp"),
        (dt.datetime(1000, 1, 1, 0, 0), "'1000-01-01 00:00'::timestamp"),
        (dt.datetime(2000, 1, 1, 0, 0), "'2000-01-01 00:00'::timestamp"),
        (
            dt.datetime(2000, 12, 31, 23, 59, 59, 999999),
            "'2000-12-31 23:59:59.999999'::timestamp",
        ),
        (dt.datetime(3000, 1, 1, 0, 0), "'3000-01-01 00:00'::timestamp"),
        (dt.datetime.max, "'9999-12-31 23:59:59.999999'::timestamp"),
    ],
)
def test_dump_datetime(conn, val, expr):
    cur = conn.cursor()
    cur.execute("set timezone to '+02:00'")
    cur.execute("select %s = %%s" % expr, (val,))
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


#
# datetime+tz tests
#

@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.datetime.min, "'0001-01-01 00:00'::timestamptz"),
        (dt.datetime(1000, 1, 1, 0, 0), "'1000-01-01 00:00+2'::timestamptz"),
        (dt.datetime(2000, 1, 1, 0, 0), "'2000-01-01 00:00+2'::timestamptz"),
        (
            dt.datetime(2000, 12, 31, 23, 59, 59, 999999),
            "'2000-12-31 23:59:59.999999+2'::timestamptz",
        ),
        (dt.datetime(3000, 1, 1, 0, 0), "'3000-01-01 00:00+2'::timestamptz"),
        (dt.datetime.max, "'9999-12-31 23:59:59.999999'::timestamptz"),
    ],
)
def test_dump_datetimetz(conn, val, expr):
    val = val.replace(tzinfo=dt.timezone(dt.timedelta(hours=2)))
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.xfail  # TODO: binary dump
@pytest.mark.parametrize(
    "val, expr",
    [(dt.datetime(2000, 1, 1, 0, 0), "'2000-01-01 00:00'::timestamptz")],
)
def test_dump_datetimetz_binary(conn, val, expr):
    val = val.replace(tzinfo=dt.timezone(dt.timedelta(hours=2)))
    cur = conn.cursor()
    cur.execute("set timezone to '-02:00'")
    cur.execute("select %s = %%b" % expr, (val,))
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
