import pickle
import datetime as dt
from decimal import Decimal

import pytest

from psycopg import pq, sql
from psycopg import errors as e
from psycopg.adapt import PyFormat
from psycopg.types.range import Range
from psycopg.types import multirange
from psycopg.types.multirange import Multirange, MultirangeInfo
from psycopg.types.multirange import register_multirange

from ..utils import eur
from .test_range import create_test_range

pytestmark = [
    pytest.mark.pg(">= 14"),
    pytest.mark.crdb_skip("range"),
]


class TestMultirangeObject:
    def test_empty(self):
        mr = Multirange[int]()
        assert not mr
        assert len(mr) == 0

        mr = Multirange([])
        assert not mr
        assert len(mr) == 0

    def test_sequence(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        assert mr
        assert len(mr) == 3
        assert mr[2] == Range(50, 60)
        assert mr[-2] == Range(30, 40)

    def test_bad_type(self):
        with pytest.raises(TypeError):
            Multirange(Range(10, 20))  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            Multirange([10])  # type: ignore[arg-type]

        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])

        with pytest.raises(TypeError):
            mr[0] = "foo"  # type: ignore[call-overload]

        with pytest.raises(TypeError):
            mr[0:1] = "foo"  # type: ignore[assignment]

        with pytest.raises(TypeError):
            mr[0:1] = ["foo"]  # type: ignore[list-item]

        with pytest.raises(TypeError):
            mr.insert(0, "foo")  # type: ignore[arg-type]

    def test_setitem(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        mr[1] = Range(31, 41)
        assert mr == Multirange([Range(10, 20), Range(31, 41), Range(50, 60)])

    def test_setitem_slice(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        mr[1:3] = [Range(31, 41), Range(51, 61)]
        assert mr == Multirange([Range(10, 20), Range(31, 41), Range(51, 61)])

        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        with pytest.raises(TypeError, match="can only assign an iterable"):
            mr[1:3] = Range(31, 41)  # type: ignore[call-overload]

        mr[1:3] = [Range(31, 41)]
        assert mr == Multirange([Range(10, 20), Range(31, 41)])

    def test_delitem(self):
        mr = Multirange([Range(10, 20), Range(30, 40), Range(50, 60)])
        del mr[1]
        assert mr == Multirange([Range(10, 20), Range(50, 60)])

        del mr[-2]
        assert mr == Multirange([Range(50, 60)])

    def test_insert(self):
        mr = Multirange([Range(10, 20), Range(50, 60)])
        mr.insert(1, Range(31, 41))
        assert mr == Multirange([Range(10, 20), Range(31, 41), Range(50, 60)])

    def test_relations(self):
        mr1 = Multirange([Range(10, 20), Range(30, 40)])
        mr2 = Multirange([Range(11, 20), Range(30, 40)])
        mr3 = Multirange([Range(9, 20), Range(30, 40)])
        assert mr1 <= mr1
        assert not mr1 < mr1
        assert mr1 >= mr1
        assert not mr1 > mr1
        assert mr1 < mr2
        assert mr1 <= mr2
        assert mr1 > mr3
        assert mr1 >= mr3
        assert mr1 != mr2
        assert not mr1 == mr2

    def test_pickling(self):
        r = Multirange([Range(0, 4)])
        assert pickle.loads(pickle.dumps(r)) == r

    def test_str(self):
        mr = Multirange([Range(10, 20), Range(30, 40)])
        assert str(mr) == "{[10, 20), [30, 40)}"

    def test_repr(self):
        mr = Multirange([Range(10, 20), Range(30, 40)])
        expected = "Multirange([Range(10, 20, '[)'), Range(30, 40, '[)')])"
        assert repr(mr) == expected


tzinfo = dt.timezone(dt.timedelta(hours=2))

samples = [
    ("int4multirange", [Range(None, None, "()")]),
    ("int4multirange", [Range(10, 20), Range(30, 40)]),
    ("int8multirange", [Range(None, None, "()")]),
    ("int8multirange", [Range(10, 20), Range(30, 40)]),
    (
        "nummultirange",
        [
            Range(None, Decimal(-100)),
            Range(Decimal(100), Decimal("100.123")),
        ],
    ),
    (
        "datemultirange",
        [Range(dt.date(2000, 1, 1), dt.date(2020, 1, 1))],
    ),
    (
        "tsmultirange",
        [
            Range(
                dt.datetime(2000, 1, 1, 00, 00),
                dt.datetime(2020, 1, 1, 23, 59, 59, 999999),
            )
        ],
    ),
    (
        "tstzmultirange",
        [
            Range(
                dt.datetime(2000, 1, 1, 00, 00, tzinfo=tzinfo),
                dt.datetime(2020, 1, 1, 23, 59, 59, 999999, tzinfo=tzinfo),
            ),
            Range(
                dt.datetime(2030, 1, 1, 00, 00, tzinfo=tzinfo),
                dt.datetime(2040, 1, 1, 23, 59, 59, 999999, tzinfo=tzinfo),
            ),
        ],
    ),
]

mr_names = """
    int4multirange int8multirange nummultirange
    datemultirange tsmultirange tstzmultirange""".split()

mr_classes = """
    Int4Multirange Int8Multirange NumericMultirange
    DateMultirange TimestampMultirange TimestamptzMultirange""".split()


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty(conn, pgtype, fmt_in):
    mr = Multirange()  # type: ignore[var-annotated]
    cur = conn.execute(f"select '{{}}'::{pgtype} = %{fmt_in.value}", (mr,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("wrapper", mr_classes)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_empty_wrapper(conn, wrapper, fmt_in):
    dumper = getattr(multirange, wrapper + "Dumper")
    wrapper = getattr(multirange, wrapper)
    mr = wrapper()
    rec = conn.execute(
        f"""
        select '{{}}' = %(mr){fmt_in.value},
            %(mr){fmt_in.value}::text,
            pg_typeof(%(mr){fmt_in.value})::oid
        """,
        {"mr": mr},
    ).fetchone()
    assert rec[0] is True, rec[1]
    assert rec[2] == dumper.oid


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize(
    "fmt_in",
    [
        PyFormat.AUTO,
        PyFormat.TEXT,
        # There are many ways to work around this (use text, use a cast on the
        # placeholder, use specific Range subclasses).
        pytest.param(
            PyFormat.BINARY,
            marks=pytest.mark.xfail(
                reason="can't dump array of untypes binary multirange without cast"
            ),
        ),
    ],
)
def test_dump_builtin_array(conn, pgtype, fmt_in):
    mr1 = Multirange()  # type: ignore[var-annotated]
    mr2 = Multirange([Range(bounds="()")])  # type: ignore[var-annotated]
    cur = conn.execute(
        f"select array['{{}}'::{pgtype}, '{{(,)}}'::{pgtype}] = %{fmt_in.value}",
        ([mr1, mr2],),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_array_with_cast(conn, pgtype, fmt_in):
    mr1 = Multirange()  # type: ignore[var-annotated]
    mr2 = Multirange([Range(bounds="()")])  # type: ignore[var-annotated]
    cur = conn.execute(
        f"""
        select array['{{}}'::{pgtype},
            '{{(,)}}'::{pgtype}] = %{fmt_in.value}::{pgtype}[]
        """,
        ([mr1, mr2],),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("wrapper", mr_classes)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_array_wrapper(conn, wrapper, fmt_in):
    wrapper = getattr(multirange, wrapper)
    mr1 = Multirange()  # type: ignore[var-annotated]
    mr2 = Multirange([Range(bounds="()")])  # type: ignore[var-annotated]
    cur = conn.execute(
        f"""select '{{"{{}}","{{(,)}}"}}' = %{fmt_in.value}""", ([mr1, mr2],)
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype, ranges", samples)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_builtin_multirange(conn, pgtype, ranges, fmt_in):
    mr = Multirange(ranges)
    rname = pgtype.replace("multi", "")
    phs = ", ".join([f"%s::{rname}"] * len(ranges))
    cur = conn.execute(f"select {pgtype}({phs}) = %{fmt_in.value}", ranges + [mr])
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_empty(conn, pgtype, fmt_out):
    mr = Multirange()  # type: ignore[var-annotated]
    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute(f"select '{{}}'::{pgtype}").fetchone()
    assert type(got) is Multirange
    assert got == mr


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_array(conn, pgtype, fmt_out):
    mr1 = Multirange()  # type: ignore[var-annotated]
    mr2 = Multirange([Range(bounds="()")])  # type: ignore[var-annotated]
    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute(
        f"select array['{{}}'::{pgtype}, '{{(,)}}'::{pgtype}]"
    ).fetchone()
    assert got == [mr1, mr2]


@pytest.mark.parametrize("pgtype, ranges", samples)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_builtin_range(conn, pgtype, ranges, fmt_out):
    mr = Multirange(ranges)
    rname = pgtype.replace("multi", "")
    phs = ", ".join([f"%s::{rname}"] * len(ranges))
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select {pgtype}({phs})", ranges)
    assert cur.fetchone()[0] == mr


@pytest.mark.parametrize(
    "min, max, bounds",
    [
        ("2000,1,1", "2001,1,1", "[)"),
        ("2000,1,1", None, "[)"),
        (None, "2001,1,1", "()"),
        (None, None, "()"),
        (None, None, "empty"),
    ],
)
@pytest.mark.parametrize("format", pq.Format)
def test_copy_in(conn, min, max, bounds, format):
    cur = conn.cursor()
    cur.execute("create table copymr (id serial primary key, mr datemultirange)")

    if bounds != "empty":
        min = dt.date(*map(int, min.split(","))) if min else None
        max = dt.date(*map(int, max.split(","))) if max else None
        r = Range[dt.date](min, max, bounds)
    else:
        r = Range(empty=True)

    mr = Multirange([r])
    try:
        with cur.copy(f"copy copymr (mr) from stdin (format {format.name})") as copy:
            copy.write_row([mr])
    except e.InternalError_:
        if not min and not max and format == pq.Format.BINARY:
            pytest.xfail("TODO: add annotation to dump multirange with no type info")
        else:
            raise

    rec = cur.execute("select mr from copymr order by id").fetchone()
    if not r.isempty:
        assert rec[0] == mr
    else:
        assert rec[0] == Multirange()


@pytest.mark.parametrize("wrapper", mr_classes)
@pytest.mark.parametrize("format", pq.Format)
def test_copy_in_empty_wrappers(conn, wrapper, format):
    cur = conn.cursor()
    cur.execute("create table copymr (id serial primary key, mr datemultirange)")

    mr = getattr(multirange, wrapper)()

    with cur.copy(f"copy copymr (mr) from stdin (format {format.name})") as copy:
        copy.write_row([mr])

    rec = cur.execute("select mr from copymr order by id").fetchone()
    assert rec[0] == mr


@pytest.mark.parametrize("pgtype", mr_names)
@pytest.mark.parametrize("format", pq.Format)
def test_copy_in_empty_set_type(conn, pgtype, format):
    cur = conn.cursor()
    cur.execute(f"create table copymr (id serial primary key, mr {pgtype})")

    mr = Multirange()  # type: ignore[var-annotated]

    with cur.copy(f"copy copymr (mr) from stdin (format {format.name})") as copy:
        copy.set_types([pgtype])
        copy.write_row([mr])

    rec = cur.execute("select mr from copymr order by id").fetchone()
    assert rec[0] == mr


@pytest.fixture(scope="session")
def testmr(svcconn):
    create_test_range(svcconn)


fetch_cases = [
    ("testmultirange", "text"),
    ("testschema.testmultirange", "float8"),
    (sql.Identifier("testmultirange"), "text"),
    (sql.Identifier("testschema", "testmultirange"), "float8"),
]


@pytest.mark.parametrize("name, subtype", fetch_cases)
def test_fetch_info(conn, testmr, name, subtype):
    info = MultirangeInfo.fetch(conn, name)
    assert info.name == "testmultirange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == conn.adapters.types[subtype].oid


def test_fetch_info_not_found(conn):
    assert MultirangeInfo.fetch(conn, "nosuchrange") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("name, subtype", fetch_cases)
async def test_fetch_info_async(aconn, testmr, name, subtype):  # noqa: F811
    info = await MultirangeInfo.fetch(aconn, name)
    assert info.name == "testmultirange"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert info.subtype_oid == aconn.adapters.types[subtype].oid


@pytest.mark.asyncio
async def test_fetch_info_not_found_async(aconn):
    assert await MultirangeInfo.fetch(aconn, "nosuchrange") is None


def test_dump_custom_empty(conn, testmr):
    info = MultirangeInfo.fetch(conn, "testmultirange")
    register_multirange(info, conn)

    r = Multirange()  # type: ignore[var-annotated]
    cur = conn.execute("select '{}'::testmultirange = %s", (r,))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_custom_empty(conn, testmr, fmt_out):
    info = MultirangeInfo.fetch(conn, "testmultirange")
    register_multirange(info, conn)

    cur = conn.cursor(binary=fmt_out)
    (got,) = cur.execute("select '{}'::testmultirange").fetchone()
    assert isinstance(got, Multirange)
    assert not got


@pytest.mark.parametrize("name", ["a-b", f"{eur}"])
def test_literal_invalid_name(conn, name):
    conn.execute("set client_encoding to utf8")
    conn.execute(f'create type "{name}" as range (subtype = text)')
    info = MultirangeInfo.fetch(conn, f'"{name}_multirange"')
    register_multirange(info, conn)
    obj = Multirange([Range("a", "z", "[]")])
    assert sql.Literal(obj).as_string(conn) == f"'{{[a,z]}}'::\"{name}_multirange\""
    cur = conn.execute(sql.SQL("select {}").format(obj))
    assert cur.fetchone()[0] == obj
