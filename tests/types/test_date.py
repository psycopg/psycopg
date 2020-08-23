import datetime as dt
import pytest


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
