import pytest
from psycopg.adapt import PyFormat

try:
    import numpy as np
except ImportError:
    pass

pytest.importorskip("numpy")

pytestmark = [pytest.mark.numpy]


def test_classes_identities():
    # Check if we know the class identities correctly. Maybe on different
    # platforms they are different.
    assert np.bool_ is np.bool8
    assert np.ubyte is np.uint8
    assert np.ushort is np.uint16
    assert np.uint is np.uint64
    assert np.uintc is np.uint32
    assert np.byte is np.int8
    assert np.short is np.int16
    assert np.intc is np.int32
    assert np.int_ is np.int64


@pytest.mark.parametrize(
    "nptype, val, expr",
    [
        ("int8", -128, "'-128'::int2"),
        ("int8", 127, "'127'::int2"),
        ("int8", 0, "'0'::int2"),
        ("int16", -32_768, "'-32768'::int2"),
        ("int16", 32_767, "'32767'::int2"),
        ("int16", 0, "'0'::int2"),
        ("int32", -(2**31), f"'{-(2**31)}'::int4"),
        ("int32", 2**31 - 1, f"'{2**31 - 1}'::int4"),
        ("int32", 0, "'0'::int4"),
        ("int64", -(2**63), f"'{-(2**63)}'::int8"),
        ("int64", 2**63 - 1, f"'{2**63 - 1}'::int8"),
        ("int64", 0, "'0'::int8"),
        ("longlong", -(2**63), f"'{-(2**63)}'::int8"),
        ("longlong", 2**63 - 1, f"'{2**63 - 1}'::int8"),
        ("bool_", True, "'t'::bool"),
        ("bool_", False, "'f'::bool"),
        ("uint8", 0, "'0'::int2"),
        ("uint8", 255, "'255'::int2"),
        ("uint16", 0, "'0'::int4"),
        ("uint16", 65_535, "'65535'::int4"),
        ("uint32", 0, "'0'::int8"),
        ("uint32", (2**32 - 1), f"'{2**32 - 1}'::int8"),
        ("uint64", 0, "'0'::numeric"),
        ("uint64", (2**64 - 1), f"'{2**64 - 1}'::numeric"),
        ("ulonglong", 0, "'0'::numeric"),
        ("ulonglong", (2**64 - 1), f"'{2**64 - 1}'::numeric"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_int(conn, val, nptype, expr, fmt_in):
    nptype = getattr(np, nptype)
    val = nptype(val)
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


@pytest.mark.parametrize("val", ["4e4", "4e-4", "4000.0", "3.14"])
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_float16(conn, val, fmt_in):

    val = np.float16(val)
    cur = conn.cursor()

    cur.execute(f"select pg_typeof({val}::float4) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {val}::float4, %(obj){fmt_in.value}", {"obj": val})
    rec = cur.fetchone()
    assert rec[0] == pytest.approx(rec[1], 1e-3)


@pytest.mark.parametrize("val", ["256e6", "256e-6", "2.7182817", "3.1415927"])
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_float32(conn, val, fmt_in):

    val = np.float32(val)
    cur = conn.cursor()

    cur.execute(f"select pg_typeof({val}::float4) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {val}::float4 = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "val", ["256e12", "256e-12", "2.718281828459045", "3.141592653589793"]
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_numpy_float64(conn, val, fmt_in):

    val = np.float64(val)
    cur = conn.cursor()

    cur.execute(f"select pg_typeof({val}::float8) = pg_typeof(%{fmt_in.value})", (val,))
    assert cur.fetchone()[0] is True

    cur.execute(f"select {val}::float8 = %{fmt_in.value}", (val,))
    assert cur.fetchone()[0] is True


@pytest.mark.slow
@pytest.mark.parametrize("fmt", PyFormat)
def test_random(conn, faker, fmt):
    faker.types = [t for t in faker.types if issubclass(t, np.generic)]
    faker.format = fmt
    faker.choose_schema(ncols=20)
    faker.make_records(50)

    with conn.cursor() as cur:
        cur.execute(faker.drop_stmt)
        cur.execute(faker.create_stmt)
        with faker.find_insert_problem(conn):
            cur.executemany(faker.insert_stmt, faker.records)

        cur.execute(faker.select_stmt)
        recs = cur.fetchall()

    for got, want in zip(recs, faker.records):
        faker.assert_record(got, want)
