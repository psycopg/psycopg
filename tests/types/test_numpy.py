import pytest
from psycopg.adapt import PyFormat

pytest.importorskip("numpy")


import numpy as np  # noqa: E402


pytestmark = [pytest.mark.numpy]


@pytest.mark.parametrize(
    "val, expr",
    [
        (-128, "'-128'::int2"),
        (127, "'127'::int2"),
        (0, "'0'::int2"),
        (45, "'45'::int2"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_int8(conn, val, expr, fmt_in):
    val = np.byte(val)

    assert isinstance(val, np.byte)
    assert np.byte is np.int8

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (-32_768, "'-32768'::int2"),
        (32_767, "'32767'::int2"),
        (0, "'0'::int2"),
        (45, "'45'::int2"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_int16(conn, val, expr, fmt_in):

    val = np.short(val)

    assert isinstance(val, np.short)
    assert np.short is np.int16

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (-2_147_483_648, "'-2147483648'::int4"),
        (2_147_483_647, "'2147483647'::int4"),
        (0, "'0'::int4"),
        (45, "'45'::int4"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_int32(conn, val, expr, fmt_in):

    val = np.intc(val)

    assert isinstance(val, np.intc)
    assert np.intc is np.int32

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (-9_223_372_036_854_775_808, "'-9223372036854775808'::int8"),
        (9_223_372_036_854_775_807, "'9223372036854775807'::int8"),
        (0, "'0'::int8"),
        (45, "'45'::int8"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_int64(conn, val, expr, fmt_in):

    val = np.int_(val)

    assert isinstance(val, np.int_)
    assert np.int_ is np.int64

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (True, "'t'::bool"),
        (False, "'f'::bool"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_bool8(conn, val, expr, fmt_in):

    val = np.bool_(val)

    assert isinstance(val, np.bool_)
    assert np.bool_ is np.bool8

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (bool(val),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [(0, "'0'::int2"), (255, "'255'::int2")],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_uint8(conn, val, expr, fmt_in):

    val = np.ubyte(val)

    assert isinstance(val, np.ubyte)
    assert np.ubyte is np.uint8

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::int4"),
        (65_535, "'65535'::int4"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_uint16(conn, val, expr, fmt_in):

    val = np.ushort(val)

    assert isinstance(val, np.ushort)
    assert np.ushort is np.uint16

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::int8"),
        (4_294_967_295, "'4294967295'::int8"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_uint32(conn, val, expr, fmt_in):

    val = np.uintc(val)

    assert isinstance(val, np.uintc)
    assert np.uintc is np.uint32

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::numeric"),
        (18_446_744_073_709_551_615, "'18446744073709551615'::numeric"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_uint64(conn, val, expr, fmt_in):

    val = np.uint(val)

    assert isinstance(val, np.uint)
    assert np.uint is np.uint64

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val, expr",
    [
        (0, "'0'::numeric"),
        (18_446_744_073_709_551_615, "'18446744073709551615'::numeric"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_ulonglong(conn, val, expr, fmt_in):

    val = np.ulonglong(val)

    assert isinstance(val, np.ulonglong)

    cur = conn.cursor()

    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


# Test float special values
@pytest.mark.parametrize(
    "val, expr",
    [
        (np.PZERO, "'0.0'::float8"),
        (np.NZERO, "'-0.0'::float8"),
        (np.nan, "'NaN'::float8"),
        (np.inf, "'Infinity'::float8"),
        (np.NINF, "'-Infinity'::float8"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_special_values(conn, val, expr, fmt_in):

    if val == np.nan:
        assert np.nan == np.NAN == np.NaN

    if val == np.inf:
        assert np.inf == np.Inf == np.PINF == np.infty

    assert isinstance(val, float)

    cur = conn.cursor()
    cur.execute(f"select pg_typeof({expr}) = pg_typeof(%{fmt_in.value})", (val,))

    assert cur.fetchone()[0] is True

    cur.execute(f"select {expr} = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val",
    [
        "4e4",
        # "4e-4",
        "4000.0",
        # "3.14",
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_float16(conn, val, fmt_in):

    val = np.float16(val)
    cur = conn.cursor()

    cur.execute(f"select pg_typeof({val}::float4) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {val}::float4 = %{fmt_in.value}", (val,))

    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val",
    [
        "256e6",
        "256e-6",
        "2.7182817",
        "3.1415927",
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_float32(conn, val, fmt_in):

    val = np.float32(val)
    cur = conn.cursor()

    cur.execute(f"select pg_typeof({val}::float4) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {val}::float4 = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val",
    [
        "256e12",
        "256e-12",
        "2.718281828459045",
        "3.141592653589793",
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_float64(conn, val, fmt_in):

    val = np.float64(val)
    cur = conn.cursor()

    cur.execute(f"select pg_typeof({val}::float8) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {val}::float8 = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True
