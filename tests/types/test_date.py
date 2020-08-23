import datetime as dt
import pytest

import psycopg3
from psycopg3.adapt import Format


@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.date.min, "'0001-01-01'::date"),
        (dt.date(1, 1, 1), "'0001-01-01'::date"),
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
    cur.execute("select '2000-01-02'::date = %s", (dt.date(2000, 1, 2),))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, expr",
    [
        (dt.date(1, 1, 1), "'0001-01-01'::date"),
        (dt.date(1000, 1, 1), "'1000-01-01'::date"),
        (dt.date(2000, 1, 1), "'2000-01-01'::date"),
        (dt.date(2000, 12, 31), "'2000-12-31'::date"),
        (dt.date(3000, 1, 1), "'3000-01-01'::date"),
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
    cur.execute("select '0001-01-01'::date - 1")
    with pytest.raises(psycopg3.InterfaceError):
        cur.fetchone()[0]
