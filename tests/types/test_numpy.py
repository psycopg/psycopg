import struct
from math import isnan

import pytest
from packaging.version import parse as ver  # noqa: F401  # used in skipif

from psycopg.adapt import PyFormat
from psycopg.pq import Format

try:
    import numpy as np
except ImportError:
    pass

pytest.importorskip("numpy")

pytestmark = [pytest.mark.numpy]

skip_numpy2 = pytest.mark.skipif(
    "ver(np.__version__) >= ver('2.0.0')", reason="not available since numpy >= 2"
)


def _get_arch_size() -> int:
    psize = struct.calcsize("P") * 8
    if psize not in (32, 64):
        msg = f"the pointer size {psize} is unusual"
        raise ValueError(msg)
    return psize


def test_integer_types():
    # Check if we know the integer types correctly. Maybe on different
    # platforms they are different.
    size = _get_arch_size()

    assert np.byte(0).dtype == np.int8(0).dtype
    assert np.ubyte(0).dtype == np.uint8(0).dtype

    assert np.short(0).dtype == np.int16(0).dtype
    assert np.ushort(0).dtype == np.uint16(0).dtype

    assert np.uintc(0).dtype == np.uint32(0).dtype
    assert np.intc(0).dtype == np.int32(0).dtype

    if size == 32:
        assert np.uint(0).dtype == np.uint32(0).dtype
        assert np.int_(0).dtype == np.int32(0).dtype
    else:
        assert np.uint(0).dtype == np.uint64(0).dtype
        assert np.int_(0).dtype == np.int64(0).dtype


@pytest.mark.parametrize(
    "name, equiv",
    [
        ("inf", "inf"),
        pytest.param("infty", "inf", marks=skip_numpy2),
        pytest.param("NINF", "-inf", marks=skip_numpy2),
        ("nan", "nan"),
        pytest.param("NaN", "nan", marks=skip_numpy2),
        pytest.param("NAN", "nan", marks=skip_numpy2),
        pytest.param("PZERO", "0.0", marks=skip_numpy2),
        pytest.param("NZERO", "-0.0", marks=skip_numpy2),
    ],
)
def test_special_values(name, equiv):
    obj = getattr(np, name)
    assert isinstance(obj, float)
    if equiv == "nan":
        assert isnan(obj)
    else:
        assert obj == float(equiv)


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

    cur.execute(f"select array[{expr}] = %{fmt_in.value}", ([val],))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "nptype, val, pgtype",
    [
        ("float16", "4e4", "float4"),
        ("float16", "4e-4", "float4"),
        ("float16", "4000.0", "float4"),
        ("float16", "3.14", "float4"),
        ("float32", "256e6", "float4"),
        ("float32", "256e-6", "float4"),
        ("float32", "2.7182817", "float4"),
        ("float32", "3.1415927", "float4"),
        ("float64", "256e12", "float8"),
        ("float64", "256e-12", "float8"),
        ("float64", "2.718281828459045", "float8"),
        ("float64", "3.141592653589793", "float8"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_float(conn, nptype, val, pgtype, fmt_in):
    nptype = getattr(np, nptype)
    val = nptype(val)
    cur = conn.cursor()

    cur.execute(
        f"select pg_typeof('{val}'::{pgtype}) = pg_typeof(%{fmt_in.value})", (val,)
    )
    assert cur.fetchone()[0] is True

    cur.execute(f"select '{val}'::{pgtype}, %(obj){fmt_in.value}", {"obj": val})
    rec = cur.fetchone()
    if nptype is np.float16:
        assert rec[0] == pytest.approx(rec[1], 1e-3)
    else:
        assert rec[0] == rec[1]

    cur.execute(
        f"select array['{val}']::{pgtype}[], %(obj){fmt_in.value}", {"obj": [val]}
    )
    rec = cur.fetchone()
    if nptype is np.float16:
        assert rec[0][0] == pytest.approx(rec[1][0], 1e-3)
    else:
        assert rec[0][0] == rec[1][0]


@pytest.mark.parametrize(
    "nptype, val, pgtypes",
    [
        ("int8", -128, "int2 int4 int8 numeric"),
        ("int8", 127, "int2 int4 int8 numeric"),
        ("int16", -32_768, "int2 int4 int8 numeric"),
        ("int16", 32_767, "int2 int4 int8 numeric"),
        ("int32", -(2**31), "int4 int8 numeric"),
        ("int32", 0, "int2 int4 int8 numeric"),
        ("int32", 2**31 - 1, "int4 int8 numeric"),
        ("int64", -(2**63), "int8 numeric"),
        ("int64", 2**63 - 1, "int8 numeric"),
        ("longlong", -(2**63), "int8"),
        ("longlong", 2**63 - 1, "int8"),
        ("bool_", True, "bool"),
        ("bool_", False, "bool"),
        ("uint8", 0, "int2 int4 int8 numeric"),
        ("uint8", 255, "int2 int4 int8 numeric"),
        ("uint16", 0, "int2 int4 int8 numeric"),
        ("uint16", 65_535, "int4 int8 numeric"),
        ("uint32", 0, "int4 int8 numeric"),
        ("uint32", (2**32 - 1), "int8 numeric"),
        ("uint64", 0, "int8 numeric"),
        ("uint64", (2**64 - 1), "numeric"),
        ("ulonglong", 0, "int8 numeric"),
        ("ulonglong", (2**64 - 1), "numeric"),
    ],
)
@pytest.mark.parametrize("fmt", Format)
@pytest.mark.crdb_skip("copy")
def test_copy_by_oid(conn, val, nptype, pgtypes, fmt):
    nptype = getattr(np, nptype)
    val = nptype(val)
    pgtypes = pgtypes.split()
    cur = conn.cursor()

    fnames = [f"f{t}" for t in pgtypes]
    fields = [f"f{t} {t}" for fname, t in zip(fnames, pgtypes)]
    cur.execute(
        f"create table numpyoid (id serial primary key, {', '.join(fields)})",
    )
    with cur.copy(
        f"copy numpyoid ({', '.join(fnames)}) from stdin (format {fmt.name})"
    ) as copy:
        copy.set_types(pgtypes)
        copy.write_row((val,) * len(fnames))

    cur.execute(f"select {', '.join(fnames)} from numpyoid")
    rec = cur.fetchone()
    assert rec == (int(val),) * len(fnames)


@pytest.mark.slow
@pytest.mark.parametrize("fmt", PyFormat)
def test_random(conn, faker, fmt):
    faker.types = [t for t in faker.types if issubclass(t, (list, np.generic))]
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
