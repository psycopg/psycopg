import enum
from decimal import Decimal
from math import isnan, isinf, exp

import pytest

import psycopg
from psycopg import pq
from psycopg import sql
from psycopg.adapt import Transformer, PyFormat
from psycopg.types.numeric import FloatLoader

from ..fix_crdb import is_crdb

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
        (int(2**63 - 1), "'9223372036854775807'::bigint"),
        (int(-(2**63)), "'-9223372036854775808'::bigint"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_int(conn, val, expr, fmt_in):
    assert isinstance(val, int)
    cur = conn.cursor()
    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::smallint"),
        (1, "'1'::smallint"),
        (-1, "'-1'::smallint"),
        (42, "'42'::smallint"),
        (-42, "'-42'::smallint"),
        (int(2**15 - 1), f"'{2 ** 15 - 1}'::smallint"),
        (int(-(2**15)), f"'{-2 ** 15}'::smallint"),
        (int(2**15), f"'{2 ** 15}'::integer"),
        (int(-(2**15) - 1), f"'{-2 ** 15 - 1}'::integer"),
        (int(2**31 - 1), f"'{2 ** 31 - 1}'::integer"),
        (int(-(2**31)), f"'{-2 ** 31}'::integer"),
        (int(2**31), f"'{2 ** 31}'::bigint"),
        (int(-(2**31) - 1), f"'{-2 ** 31 - 1}'::bigint"),
        (int(2**63 - 1), f"'{2 ** 63 - 1}'::bigint"),
        (int(-(2**63)), f"'{-2 ** 63}'::bigint"),
        (int(2**63), f"'{2 ** 63}'::numeric"),
        (int(-(2**63) - 1), f"'{-2 ** 63 - 1}'::numeric"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_int_subtypes(conn, val, expr, fmt_in):
    cur = conn.cursor()
    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True
    cur.execute(
        f"select {expr} = %(v){fmt_in.value}, {expr}::text, %(v){fmt_in.value}::text",
        {"v": val},
    )
    ok, want, got = cur.fetchone()
    assert got == want
    assert ok


class MyEnum(enum.IntEnum):
    foo = 42


class MyMixinEnum(enum.IntEnum):
    foo = 42000000


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("enum", [MyEnum, MyMixinEnum])
def test_dump_enum(conn, fmt_in, enum):
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value}", (enum.foo,))
    (res,) = cur.fetchone()
    assert res == enum.foo.value


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, b"0"),
        (1, b"1"),
        (-1, b" -1"),
        (42, b"42"),
        (-42, b" -42"),
        (int(2**63 - 1), b"9223372036854775807"),
        (int(-(2**63)), b" -9223372036854775808"),
        (int(2**63), b"9223372036854775808"),
        (int(-(2**63 + 1)), b" -9223372036854775809"),
        (int(2**100), b"1267650600228229401496703205376"),
        (int(-(2**100)), b" -1267650600228229401496703205376"),
    ],
)
def test_quote_int(conn, val, expr):
    tx = Transformer()
    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == expr

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
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_int(conn, val, pgtype, want, fmt_out):
    if pgtype == "integer" and is_crdb(conn):
        pgtype = "int4"  # "integer" is "int8" on crdb
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
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_float(conn, val, expr, fmt_in):
    assert isinstance(val, float)
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value} = {expr}::float8", (val,))
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
    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == expr

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
        (exp(1), "exp(1.0)"),
        (-exp(1), "-exp(1.0)"),
        (1e30, "'1e30'"),
        (1e-30, "1e-30"),
        (-1e30, "'-1e30'"),
        (-1e-30, "-1e-30"),
    ],
)
def test_dump_float_approx(conn, val, expr):
    assert isinstance(val, float)
    cur = conn.cursor()
    cur.execute(f"select abs(({expr}::float8 - %s) / {expr}::float8) <= 1e-15", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select abs(({expr}::float4 - %s) / {expr}::float4) <= 1e-6", (val,))
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
@pytest.mark.parametrize("fmt_out", pq.Format)
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
        ("exp(1.0)", "float4", 2.71828),
        ("-exp(1.0)", "float4", -2.71828),
        ("exp(1.0)", "float8", 2.71828182845905),
        ("-exp(1.0)", "float8", -2.71828182845905),
        ("1.42e10", "float4", 1.42e10),
        ("-1.42e10", "float4", -1.42e10),
        ("1.42e40", "float8", 1.42e40),
        ("-1.42e40", "float8", -1.42e40),
    ],
)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_float_approx(conn, expr, pgtype, want, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute("select %s::%s" % (expr, pgtype))
    assert cur.pgresult.fformat(0) == fmt_out
    result = cur.fetchone()[0]
    assert result == pytest.approx(want)


@pytest.mark.crdb_skip("copy")
def test_load_float_copy(conn):
    cur = conn.cursor(binary=False)
    with cur.copy("copy (select 3.14::float8, 'hi'::text) to stdout;") as copy:
        copy.set_types(["float8", "text"])
        rec = copy.read_row()

    assert rec[0] == pytest.approx(3.14)
    assert rec[1] == "hi"


#
# Tests with decimal
#


@pytest.mark.parametrize(
    "val",
    [
        "0",
        "-0",
        "0.0",
        "0.000000000000000000001",
        "-0.000000000000000000001",
        "nan",
        "snan",
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_roundtrip_numeric(conn, val, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    val = Decimal(val)
    cur.execute(f"select %{fmt_in.value}", (val,))
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
        ("snan", b"'NaN'::numeric"),
    ],
)
def test_quote_numeric(conn, val, expr):
    val = Decimal(val)
    tx = Transformer()
    assert tx.get_dumper(val, PyFormat.TEXT).quote(val) == expr

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}, -{v}").format(v=sql.Literal(val)))
    r = cur.fetchone()

    if val.is_nan():
        assert isnan(r[0]) and isnan(r[1])
    else:
        assert r == (val, -val)


@pytest.mark.crdb_skip("binary decimal")
@pytest.mark.parametrize(
    "expr",
    ["NaN", "1", "1.0", "-1", "0.0", "0.01", "11", "1.1", "1.01", "0", "0.00"]
    + [
        "0.0000000",
        "0.00001",
        "1.00001",
        "-1.00000000000000",
        "-2.00000000000000",
        "1000000000.12345",
        "100.123456790000000000000000",
        "1.0e-1000",
        "1e1000",
        "0.000000000000000000000000001",
        "1.0000000000000000000000001",
        "1000000000000000000000000.001",
        "1000000000000000000000000000.001",
        "9999999999999999999999999999.9",
    ],
)
def test_dump_numeric_binary(conn, expr):
    cur = conn.cursor()
    val = Decimal(expr)
    cur.execute("select %b::text, %s::decimal::text", [val, expr])
    want, got = cur.fetchone()
    assert got == want


@pytest.mark.slow
@pytest.mark.parametrize(
    "fmt_in",
    [
        f
        if f != PyFormat.BINARY
        else pytest.param(f, marks=pytest.mark.crdb_skip("binary decimal"))
        for f in PyFormat
    ],
)
def test_dump_numeric_exhaustive(conn, fmt_in):
    cur = conn.cursor()

    funcs = [
        (lambda i: "1" + "0" * i),
        (lambda i: "1" + "0" * i + "." + "0" * i),
        (lambda i: "-1" + "0" * i),
        (lambda i: "0." + "0" * i + "1"),
        (lambda i: "-0." + "0" * i + "1"),
        (lambda i: "1." + "0" * i + "1"),
        (lambda i: "1." + "0" * i + "10"),
        (lambda i: "1" + "0" * i + ".001"),
        (lambda i: "9" + "9" * i),
        (lambda i: "9" + "." + "9" * i),
        (lambda i: "9" + "9" * i + ".9"),
        (lambda i: "9" + "9" * i + "." + "9" * i),
        (lambda i: "1.1e%s" % i),
        (lambda i: "1.1e-%s" % i),
    ]

    for i in range(100):
        for f in funcs:
            expr = f(i)
            val = Decimal(expr)
            cur.execute(f"select %{fmt_in.value}::text, %s::decimal::text", [val, expr])
            got, want = cur.fetchone()
            assert got == want


@pytest.mark.pg(">= 14")
@pytest.mark.parametrize(
    "val, expr",
    [
        ("inf", "Infinity"),
        ("-inf", "-Infinity"),
    ],
)
def test_dump_numeric_binary_inf(conn, val, expr):
    cur = conn.cursor()
    val = Decimal(val)
    cur.execute("select %b", [val])


@pytest.mark.parametrize(
    "expr",
    ["nan", "0", "1", "-1", "0.0", "0.01"]
    + [
        "0.0000000",
        "-1.00000000000000",
        "-2.00000000000000",
        "1000000000.12345",
        "100.123456790000000000000000",
        "1.0e-1000",
        "1e1000",
        "0.000000000000000000000000001",
        "1.0000000000000000000000001",
        "1000000000000000000000000.001",
        "1000000000000000000000000000.001",
        "9999999999999999999999999999.9",
    ],
)
def test_load_numeric_binary(conn, expr):
    cur = conn.cursor(binary=1)
    res = cur.execute(f"select '{expr}'::numeric").fetchone()[0]
    val = Decimal(expr)
    if val.is_nan():
        assert res.is_nan()
    else:
        assert res == val
        if "e" not in expr:
            assert str(res) == str(val)


@pytest.mark.slow
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_numeric_exhaustive(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)

    funcs = [
        (lambda i: "1" + "0" * i),
        (lambda i: "1" + "0" * i + "." + "0" * i),
        (lambda i: "-1" + "0" * i),
        (lambda i: "0." + "0" * i + "1"),
        (lambda i: "-0." + "0" * i + "1"),
        (lambda i: "1." + "0" * i + "1"),
        (lambda i: "1." + "0" * i + "10"),
        (lambda i: "1" + "0" * i + ".001"),
        (lambda i: "9" + "9" * i),
        (lambda i: "9" + "." + "9" * i),
        (lambda i: "9" + "9" * i + ".9"),
        (lambda i: "9" + "9" * i + "." + "9" * i),
    ]

    for i in range(100):
        for f in funcs:
            snum = f(i)
            want = Decimal(snum)
            got = cur.execute(f"select '{snum}'::decimal").fetchone()[0]
            assert want == got
            assert str(want) == str(got)


@pytest.mark.pg(">= 14")
@pytest.mark.parametrize(
    "val, expr",
    [
        ("inf", "Infinity"),
        ("-inf", "-Infinity"),
    ],
)
def test_load_numeric_binary_inf(conn, val, expr):
    cur = conn.cursor(binary=1)
    res = cur.execute(f"select '{expr}'::numeric").fetchone()[0]
    val = Decimal(val)
    assert res == val


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
    cur.adapters.register_loader("numeric", FloatLoader)

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


@pytest.mark.parametrize("wrapper", "Int2 Int4 Int8 Oid Float4 Float8".split())
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_wrapper(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.numeric, wrapper)
    obj = wrapper(1)
    cur = conn.execute(
        f"select %(obj){fmt_in.value} = 1, %(obj){fmt_in.value}", {"obj": obj}
    )
    rec = cur.fetchone()
    assert rec[0], rec[1]


@pytest.mark.parametrize("wrapper", "Int2 Int4 Int8 Oid Float4 Float8".split())
def test_dump_wrapper_oid(wrapper):
    wrapper = getattr(psycopg.types.numeric, wrapper)
    base = wrapper.__mro__[1]
    assert base in (int, float)
    n = base(3.14)
    assert str(wrapper(n)) == str(n)
    assert repr(wrapper(n)) == f"{wrapper.__name__}({n})"


@pytest.mark.crdb("skip", reason="all types returned as bigint? TODOCRDB")
@pytest.mark.parametrize("wrapper", "Int2 Int4 Int8 Oid Float4 Float8".split())
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_repr_wrapper(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.numeric, wrapper)
    cur = conn.execute(f"select pg_typeof(%{fmt_in.value})::oid", [wrapper(0)])
    oid = cur.fetchone()[0]
    assert oid == psycopg.postgres.types[wrapper.__name__.lower()].oid


@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize(
    "typename",
    "integer int2 int4 int8 float4 float8 numeric".split() + ["double precision"],
)
def test_oid_lookup(conn, typename, fmt_out):
    dumper = conn.adapters.get_dumper_by_oid(conn.adapters.types[typename].oid, fmt_out)
    assert dumper.oid == conn.adapters.types[typename].oid
    assert dumper.format == fmt_out
