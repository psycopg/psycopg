from decimal import Decimal
from math import isnan, isinf, exp

import pytest

from psycopg3.adapt import TypeCaster, Format
from psycopg3.types import builtins
from psycopg3.types.numeric import cast_float


#
# Tests with int
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::int"),
        (1, "'1'::int"),
        (-1, "'-1'::int"),
        (42, "'42'::int"),
        (-42, "'-42'::int"),
        (int(2 ** 63 - 1), "'9223372036854775807'::bigint"),
        (int(-(2 ** 63)), "'-9223372036854775808'::bigint"),
    ],
)
def test_adapt_int(conn, val, expr):
    assert isinstance(val, int)
    cur = conn.cursor()
    cur.execute("select %s = %%s" % expr, (val,))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, pgtype, want",
    [
        ("0", "integer", 0),
        ("1", "integer", 1),
        ("-1", "integer", -1),
        ("0", "int2", 0),
        ("0", "int4", 0),
        ("0", "int8", 0),
        ("0", "integer", 0),
        ("0", "oid", 0),
        # bounds
        ("-32768", "smallint", -32768),
        ("+32767", "smallint", 32767),
        ("-2147483648", "integer", -2147483648),
        ("+2147483647", "integer", 2147483647),
        ("-9223372036854775808", "bigint", -9223372036854775808),
        ("9223372036854775807", "bigint", 9223372036854775807),
        ("4294967295", "oid", 4294967295),
    ],
)
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_int(conn, val, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    cur.execute("select %%s::%s" % pgtype, (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == builtins[pgtype].oid
    result = cur.fetchone()[0]
    assert result == want
    assert type(result) is type(want)


#
# Tests with float
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (0.0, "'0'"),
        (1.0, "'1'"),
        (-1.0, "'-1'"),
        (float("nan"), "'nan'"),
        (float("inf"), "'inf'"),
        (float("-inf"), "'-inf'"),
    ],
)
def test_adapt_float(conn, val, expr):
    assert isinstance(val, float)
    cur = conn.cursor()
    cur.execute("select %%s = %s::float8" % expr, (val,))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, expr",
    [
        (exp(1), "exp(1)"),
        (-exp(1), "-exp(1)"),
        (1e30, "'1e30'"),
        (1e-30, "1e-30"),
        (-1e30, "'-1e30'"),
        (-1e-30, "-1e-30"),
    ],
)
def test_adapt_float_approx(conn, val, expr):
    assert isinstance(val, float)
    cur = conn.cursor()
    cur.execute(
        "select abs((%s::float8 - %%s) / %s::float8) <= 1e-15" % (expr, expr),
        (val,),
    )
    assert cur.fetchone()[0]
    cur.execute(
        "select abs((%s::float4 - %%s) / %s::float4) <= 1e-6" % (expr, expr),
        (val,),
    )
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "val, pgtype, want",
    [
        ("0", "float4", 0.0),
        ("0.0", "float4", 0.0),
        ("42", "float4", 42.0),
        ("-42", "float4", -42.0),
        ("0.0", "float8", 0.0),
        ("0.0", "real", 0.0),
        ("0.0", "double precision", 0.0),
        ("0.0", "float4", 0.0),
        ("nan", "float4", float("nan")),
        ("inf", "float4", float("inf")),
        ("-inf", "float4", -float("inf")),
        ("nan", "float8", float("nan")),
        ("inf", "float8", float("inf")),
        ("-inf", "float8", -float("inf")),
    ],
)
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_float(conn, val, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    cur.execute("select %%s::%s" % pgtype, (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    result = cur.fetchone()[0]
    assert type(result) is type(want)
    if isnan(want):
        assert isnan(result)
    elif isinf(want):
        assert isinf(result)
        assert (result < 0) is (want < 0)
    else:
        assert result == want


@pytest.mark.parametrize(
    "expr, pgtype, want",
    [
        ("exp(1)", "float4", 2.71828),
        ("-exp(1)", "float4", -2.71828),
        ("exp(1)", "float8", 2.71828182845905),
        ("-exp(1)", "float8", -2.71828182845905),
        ("1.42e10", "float4", 1.42e10),
        ("-1.42e10", "float4", -1.42e10),
        ("1.42e40", "float8", 1.42e40),
        ("-1.42e40", "float8", -1.42e40),
    ],
)
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_float_approx(conn, expr, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    cur.execute("select %s::%s" % (expr, pgtype))
    assert cur.pgresult.fformat(0) == fmt_out
    result = cur.fetchone()[0]
    assert result == pytest.approx(want)


#
# Tests with decimal
#


@pytest.mark.parametrize(
    "val",
    [
        "0",
        "0.0",
        "0.000000000000000000001",
        "-0.000000000000000000001",
        "nan",
    ],
)
def test_roundtrip_numeric(conn, val):
    cur = conn.cursor()
    val = Decimal(val)
    cur.execute("select %s", (val,))
    result = cur.fetchone()[0]
    assert isinstance(result, Decimal)
    if val.is_nan():
        assert result.is_nan()
    else:
        assert result == val


@pytest.mark.parametrize(
    "val",
    [
        "0",
        "0.0",
        "0.000000000000000000001",
        "-0.000000000000000000001",
        "nan",
    ],
)
def test_numeric_as_float(conn, val):
    cur = conn.cursor()
    TypeCaster.register(builtins["numeric"].oid, cast_float, cur)

    val = Decimal(val)
    cur.execute("select %s", (val,))
    result = cur.fetchone()[0]
    assert isinstance(result, float)
    if val.is_nan():
        assert isnan(result)
    else:
        assert result == pytest.approx(float(val))


#
# Mixed tests
#


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("b", [True, False, None])
def test_roundtrip_bool(conn, b, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    result = cur.execute(f"select {ph}", (b,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    assert result is b


@pytest.mark.parametrize("pgtype", [None, "float8", "int8", "numeric"])
def test_minus_minus(conn, pgtype):
    cur = conn.cursor()
    cast = f"::{pgtype}" if pgtype is not None else ""
    cur.execute("select -%%s%s" % cast, [-1])
    result = cur.fetchone()[0]
    assert result == 1
