from decimal import Decimal
from math import isnan, isinf, exp

import pytest

from psycopg3 import pq
from psycopg3 import sql
from psycopg3.adapt import Transformer, Format
from psycopg3.types.numeric import FloatLoader


#
# Tests with int
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::int"),
        (1, "'1'::int"),
        (-1, "'-1'::int"),
        (42, "'42'::smallint"),
        (-42, "'-42'::smallint"),
        (int(2 ** 63 - 1), "'9223372036854775807'::bigint"),
        (int(-(2 ** 63)), "'-9223372036854775808'::bigint"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_dump_int(conn, val, expr, fmt_in):
    assert isinstance(val, int)
    cur = conn.cursor()
    cur.execute(f"select {expr} = %{fmt_in}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::smallint"),
        (1, "'1'::smallint"),
        (-1, "'-1'::smallint"),
        (42, "'42'::smallint"),
        (-42, "'-42'::smallint"),
        (int(2 ** 15 - 1), f"'{2 ** 15 - 1}'::smallint"),
        (int(-(2 ** 15)), f"'{-2 ** 15}'::smallint"),
        (int(2 ** 15), f"'{2 ** 15}'::integer"),
        (int(-(2 ** 15) - 1), f"'{-2 ** 15 - 1}'::integer"),
        (int(2 ** 31 - 1), f"'{2 ** 31 - 1}'::integer"),
        (int(-(2 ** 31)), f"'{-2 ** 31}'::integer"),
        (int(2 ** 31), f"'{2 ** 31}'::bigint"),
        (int(-(2 ** 31) - 1), f"'{-2 ** 31 - 1}'::bigint"),
        (int(2 ** 63 - 1), f"'{2 ** 63 - 1}'::bigint"),
        (int(-(2 ** 63)), f"'{-2 ** 63}'::bigint"),
        (int(2 ** 63), f"'{2 ** 63}'::numeric"),
        (int(-(2 ** 63) - 1), f"'{-2 ** 63 - 1}'::numeric"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_int_subtypes(conn, val, expr, fmt_in):
    if fmt_in in (Format.AUTO, Format.BINARY) and "numeric" in expr:
        pytest.xfail("binary numeric not implemented")
    cur = conn.cursor()
    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in})", (val,))
    assert cur.fetchone()[0] is True
    cur.execute(f"select {expr} = %{fmt_in}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, b"0"),
        (1, b"1"),
        (-1, b" -1"),
        (42, b"42"),
        (-42, b" -42"),
        (int(2 ** 63 - 1), b"9223372036854775807"),
        (int(-(2 ** 63)), b" -9223372036854775808"),
    ],
)
def test_quote_int(conn, val, expr):
    tx = Transformer()
    assert tx.get_dumper(val, Format.TEXT).quote(val) == expr

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}, -{v}").format(v=sql.Literal(val)))
    assert cur.fetchone() == (val, -val)


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
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_int(conn, val, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select %s::{pgtype}", (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == conn.adapters.types[pgtype].oid
    result = cur.fetchone()[0]
    assert result == want
    assert type(result) is type(want)

    # arrays work too
    cur.execute(f"select array[%s::{pgtype}]", (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == conn.adapters.types[pgtype].array_oid
    result = cur.fetchone()[0]
    assert result == [want]
    assert type(result[0]) is type(want)


#
# Tests with float
#


@pytest.mark.parametrize(
    "val, expr",
    [
        (0.0, "'0'"),
        (1.0, "'1'"),
        (-1.0, "'-1'"),
        (float("nan"), "'NaN'"),
        (float("inf"), "'Infinity'"),
        (float("-inf"), "'-Infinity'"),
    ],
)
@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_dump_float(conn, val, expr, fmt_in):
    assert isinstance(val, float)
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in} = {expr}::float8", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0.0, b"0.0"),
        (1.0, b"1.0"),
        (10000000000000000.0, b"1e+16"),
        (1000000.1, b"1000000.1"),
        (-100000.000001, b" -100000.000001"),
        (-1.0, b" -1.0"),
        (float("nan"), b"'NaN'::float8"),
        (float("inf"), b"'Infinity'::float8"),
        (float("-inf"), b"'-Infinity'::float8"),
    ],
)
def test_quote_float(conn, val, expr):
    tx = Transformer()
    assert tx.get_dumper(val, Format.TEXT).quote(val) == expr

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}, -{v}").format(v=sql.Literal(val)))
    r = cur.fetchone()
    if isnan(val):
        assert isnan(r[0]) and isnan(r[1])
    else:
        if isinstance(r[0], Decimal):
            r = tuple(map(float, r))

        assert r == (val, -val)


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
def test_dump_float_approx(conn, val, expr):
    assert isinstance(val, float)
    cur = conn.cursor()
    cur.execute(
        f"select abs(({expr}::float8 - %s) / {expr}::float8) <= 1e-15", (val,)
    )
    assert cur.fetchone()[0] is True

    cur.execute(
        f"select abs(({expr}::float4 - %s) / {expr}::float4) <= 1e-6", (val,)
    )
    assert cur.fetchone()[0] is True


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
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_float(conn, val, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select %s::{pgtype}", (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == conn.adapters.types[pgtype].oid
    result = cur.fetchone()[0]

    def check(result, want):
        assert type(result) is type(want)
        if isnan(want):
            assert isnan(result)
        elif isinf(want):
            assert isinf(result)
            assert (result < 0) is (want < 0)
        else:
            assert result == want

    check(result, want)

    cur.execute(f"select array[%s::{pgtype}]", (val,))
    assert cur.pgresult.fformat(0) == fmt_out
    assert cur.pgresult.ftype(0) == conn.adapters.types[pgtype].array_oid
    result = cur.fetchone()[0]
    assert isinstance(result, list)
    check(result[0], want)


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
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_float_approx(conn, expr, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out)
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
    "val, expr",
    [
        ("0", b"0"),
        ("0.0", b"0.0"),
        ("0.00000000000000001", b"1E-17"),
        ("-0.00000000000000001", b" -1E-17"),
        ("nan", b"'NaN'::numeric"),
    ],
)
def test_quote_numeric(conn, val, expr):
    val = Decimal(val)
    tx = Transformer()
    assert tx.get_dumper(val, Format.TEXT).quote(val) == expr

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}, -{v}").format(v=sql.Literal(val)))
    r = cur.fetchone()

    if val.is_nan():
        assert isnan(r[0]) and isnan(r[1])
    else:
        assert r == (val, -val)


@pytest.mark.xfail
def test_dump_numeric_binary():
    # TODO: numeric binary adaptation
    tx = Transformer()
    n = Decimal(1)
    tx.get_dumper(n, Format.BINARY).dump(n)


@pytest.mark.xfail
def test_load_numeric_binary(conn):
    # TODO: numeric binary casting
    cur = conn.cursor(binary=1)
    res = cur.execute("select 1::numeric").fetchone()[0]
    assert res == Decimal(1)


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
    FloatLoader.register(conn.adapters.types["numeric"].oid, cur)

    val = Decimal(val)
    cur.execute("select %s as val", (val,))
    result = cur.fetchone()[0]
    assert isinstance(result, float)
    if val.is_nan():
        assert isnan(result)
    else:
        assert result == pytest.approx(float(val))

    # the customization works with arrays too
    cur.execute("select %s as arr", ([val],))
    result = cur.fetchone()[0]
    assert isinstance(result, list)
    assert isinstance(result[0], float)
    if val.is_nan():
        assert isnan(result[0])
    else:
        assert result[0] == pytest.approx(float(val))


#
# Mixed tests
#


@pytest.mark.parametrize("pgtype", [None, "float8", "int8", "numeric"])
def test_minus_minus(conn, pgtype):
    cur = conn.cursor()
    cast = f"::{pgtype}" if pgtype is not None else ""
    cur.execute(f"select -%s{cast}", [-1])
    result = cur.fetchone()[0]
    assert result == 1


@pytest.mark.parametrize("pgtype", [None, "float8", "int8", "numeric"])
def test_minus_minus_quote(conn, pgtype):
    cur = conn.cursor()
    cast = f"::{pgtype}" if pgtype is not None else ""
    cur.execute(sql.SQL("select -{}{}").format(sql.Literal(-1), sql.SQL(cast)))
    result = cur.fetchone()[0]
    assert result == 1
