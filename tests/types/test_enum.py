from enum import Enum, auto

import pytest

from psycopg import pq, sql, errors as e
from psycopg.adapt import PyFormat
from psycopg.types import TypeInfo
from psycopg.types.enum import EnumInfo, register_enum

from ..fix_crdb import crdb_encoding


class PureTestEnum(Enum):
    FOO = auto()
    BAR = auto()
    BAZ = auto()


class StrTestEnum(str, Enum):
    ONE = "ONE"
    TWO = "TWO"
    THREE = "THREE"


NonAsciiEnum = Enum(
    "NonAsciiEnum",
    {"X\xe0": "x\xe0", "X\xe1": "x\xe1", "COMMA": "foo,bar"},
    type=str,
)


class IntTestEnum(int, Enum):
    ONE = 1
    TWO = 2
    THREE = 3


enum_cases = [PureTestEnum, StrTestEnum, IntTestEnum]
encodings = ["utf8", crdb_encoding("latin1")]


@pytest.fixture(scope="session", autouse=True)
def make_test_enums(request, svcconn):
    for enum in enum_cases + [NonAsciiEnum]:
        ensure_enum(enum, svcconn)


def ensure_enum(enum, conn):
    name = enum.__name__.lower()
    labels = list(enum.__members__)
    conn.execute(
        sql.SQL(
            """
            drop type if exists {name};
            create type {name} as enum ({labels});
            """
        ).format(name=sql.Identifier(name), labels=sql.SQL(",").join(labels))
    )
    return name, enum, labels


def test_fetch_info(conn):
    info = EnumInfo.fetch(conn, "StrTestEnum")
    assert info.name == "strtestenum"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.labels) == len(StrTestEnum)
    assert info.labels == list(StrTestEnum.__members__)


@pytest.mark.asyncio
async def test_fetch_info_async(aconn):
    info = await EnumInfo.fetch(aconn, "PureTestEnum")
    assert info.name == "puretestenum"
    assert info.oid > 0
    assert info.oid != info.array_oid > 0
    assert len(info.labels) == len(PureTestEnum)
    assert info.labels == list(PureTestEnum.__members__)


def test_register_makes_a_type(conn):
    info = EnumInfo.fetch(conn, "IntTestEnum")
    assert info
    assert info.enum is None
    register_enum(info, context=conn)
    assert info.enum is not None
    assert [e.name for e in info.enum] == list(IntTestEnum.__members__)


@pytest.mark.parametrize("enum", enum_cases)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_loader(conn, enum, fmt_in, fmt_out):
    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum=enum)

    for label in info.labels:
        cur = conn.execute(
            f"select %{fmt_in.value}::{enum.__name__}", [label], binary=fmt_out
        )
        assert cur.fetchone()[0] == enum[label]


@pytest.mark.parametrize("encoding", encodings)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_loader_nonascii(conn, encoding, fmt_in, fmt_out):
    enum = NonAsciiEnum
    conn.execute(f"set client_encoding to {encoding}")

    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum=enum)

    for label in info.labels:
        cur = conn.execute(
            f"select %{fmt_in.value}::{info.name}", [label], binary=fmt_out
        )
        assert cur.fetchone()[0] == enum[label]


@pytest.mark.crdb_skip("encoding")
@pytest.mark.parametrize("enum", enum_cases)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_loader_sqlascii(conn, enum, fmt_in, fmt_out):
    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum)
    conn.execute("set client_encoding to sql_ascii")

    for label in info.labels:
        cur = conn.execute(
            f"select %{fmt_in.value}::{info.name}", [label], binary=fmt_out
        )
        assert cur.fetchone()[0] == enum[label]


@pytest.mark.parametrize("enum", enum_cases)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_dumper(conn, enum, fmt_in, fmt_out):
    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum)

    for item in enum:
        cur = conn.execute(f"select %{fmt_in.value}", [item], binary=fmt_out)
        assert cur.fetchone()[0] == item


@pytest.mark.parametrize("encoding", encodings)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_dumper_nonascii(conn, encoding, fmt_in, fmt_out):
    enum = NonAsciiEnum
    conn.execute(f"set client_encoding to {encoding}")

    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum)

    for item in enum:
        cur = conn.execute(f"select %{fmt_in.value}", [item], binary=fmt_out)
        assert cur.fetchone()[0] == item


@pytest.mark.crdb_skip("encoding")
@pytest.mark.parametrize("enum", enum_cases)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_dumper_sqlascii(conn, enum, fmt_in, fmt_out):
    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum)
    conn.execute("set client_encoding to sql_ascii")

    for item in enum:
        cur = conn.execute(f"select %{fmt_in.value}", [item], binary=fmt_out)
        assert cur.fetchone()[0] == item


@pytest.mark.parametrize("enum", enum_cases)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_generic_enum_dumper(conn, enum, fmt_in, fmt_out):
    for item in enum:
        if enum is PureTestEnum:
            want = item.name
        else:
            want = item.value

        cur = conn.execute(f"select %{fmt_in.value}", [item], binary=fmt_out)
        assert cur.fetchone()[0] == want


@pytest.mark.parametrize("encoding", encodings)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_generic_enum_dumper_nonascii(conn, encoding, fmt_in, fmt_out):
    conn.execute(f"set client_encoding to {encoding}")
    for item in NonAsciiEnum:
        cur = conn.execute(f"select %{fmt_in.value}", [item.value], binary=fmt_out)
        assert cur.fetchone()[0] == item.value


@pytest.mark.parametrize("enum", enum_cases)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_generic_enum_loader(conn, enum, fmt_in, fmt_out):
    for label in enum.__members__:
        cur = conn.execute(
            f"select %{fmt_in.value}::{enum.__name__}", [label], binary=fmt_out
        )
        want = enum[label].name
        if fmt_out == pq.Format.BINARY:
            want = want.encode()
        assert cur.fetchone()[0] == want


@pytest.mark.parametrize("encoding", encodings)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_generic_enum_loader_nonascii(conn, encoding, fmt_in, fmt_out):
    conn.execute(f"set client_encoding to {encoding}")

    for label in NonAsciiEnum.__members__:
        cur = conn.execute(
            f"select %{fmt_in.value}::nonasciienum", [label], binary=fmt_out
        )
        if fmt_out == pq.Format.TEXT:
            assert cur.fetchone()[0] == label
        else:
            assert cur.fetchone()[0] == label.encode(encoding)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_array_loader(conn, fmt_in, fmt_out):
    enum = PureTestEnum
    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum)

    labels = list(enum.__members__)
    cur = conn.execute(
        f"select %{fmt_in.value}::{info.name}[]", [labels], binary=fmt_out
    )
    assert cur.fetchone()[0] == list(enum)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_array_dumper(conn, fmt_in, fmt_out):
    enum = StrTestEnum
    info = EnumInfo.fetch(conn, enum.__name__)
    register_enum(info, conn, enum)

    cur = conn.execute(f"select %{fmt_in.value}::text[]", [list(enum)], binary=fmt_out)
    assert cur.fetchone()[0] == list(enum.__members__)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_generic_enum_array_loader(conn, fmt_in, fmt_out):
    enum = IntTestEnum
    info = TypeInfo.fetch(conn, enum.__name__)
    info.register(conn)
    labels = list(enum.__members__)
    cur = conn.execute(
        f"select %{fmt_in.value}::{info.name}[]", [labels], binary=fmt_out
    )
    if fmt_out == pq.Format.TEXT:
        assert cur.fetchone()[0] == labels
    else:
        assert cur.fetchone()[0] == [item.encode() for item in labels]


def test_enum_error(conn):
    conn.autocommit = True

    info = EnumInfo.fetch(conn, "puretestenum")
    register_enum(info, conn, StrTestEnum)

    with pytest.raises(e.DataError):
        conn.execute("select %s::text", [StrTestEnum.ONE]).fetchone()
    with pytest.raises(e.DataError):
        conn.execute("select 'BAR'::puretestenum").fetchone()


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize(
    "mapping",
    [
        {StrTestEnum.ONE: "FOO", StrTestEnum.TWO: "BAR", StrTestEnum.THREE: "BAZ"},
        [
            (StrTestEnum.ONE, "FOO"),
            (StrTestEnum.TWO, "BAR"),
            (StrTestEnum.THREE, "BAZ"),
        ],
    ],
)
def test_remap(conn, fmt_in, fmt_out, mapping):
    info = EnumInfo.fetch(conn, "puretestenum")
    register_enum(info, conn, StrTestEnum, mapping=mapping)

    for member, label in [("ONE", "FOO"), ("TWO", "BAR"), ("THREE", "BAZ")]:
        cur = conn.execute(f"select %{fmt_in.value}::text", [StrTestEnum[member]])
        assert cur.fetchone()[0] == label
        cur = conn.execute(f"select '{label}'::puretestenum", binary=fmt_out)
        assert cur.fetchone()[0] is StrTestEnum[member]


def test_remap_rename(conn):
    enum = Enum("RenamedEnum", "FOO BAR QUX")
    info = EnumInfo.fetch(conn, "puretestenum")
    register_enum(info, conn, enum, mapping={enum.QUX: "BAZ"})

    for member, label in [("FOO", "FOO"), ("BAR", "BAR"), ("QUX", "BAZ")]:
        cur = conn.execute("select %s::text", [enum[member]])
        assert cur.fetchone()[0] == label
        cur = conn.execute(f"select '{label}'::puretestenum")
        assert cur.fetchone()[0] is enum[member]


def test_remap_more_python(conn):
    enum = Enum("LargerEnum", "FOO BAR BAZ QUX QUUX QUUUX")
    info = EnumInfo.fetch(conn, "puretestenum")
    mapping = {enum[m]: "BAZ" for m in ["QUX", "QUUX", "QUUUX"]}
    register_enum(info, conn, enum, mapping=mapping)

    for member, label in [("FOO", "FOO"), ("BAZ", "BAZ"), ("QUUUX", "BAZ")]:
        cur = conn.execute("select %s::text", [enum[member]])
        assert cur.fetchone()[0] == label

    for member, label in [("FOO", "FOO"), ("QUUUX", "BAZ")]:
        cur = conn.execute(f"select '{label}'::puretestenum")
        assert cur.fetchone()[0] is enum[member]


def test_remap_more_postgres(conn):
    enum = Enum("SmallerEnum", "FOO")
    info = EnumInfo.fetch(conn, "puretestenum")
    mapping = [(enum.FOO, "BAR"), (enum.FOO, "BAZ")]
    register_enum(info, conn, enum, mapping=mapping)

    cur = conn.execute("select %s::text", [enum.FOO])
    assert cur.fetchone()[0] == "BAZ"

    for label in PureTestEnum.__members__:
        cur = conn.execute(f"select '{label}'::puretestenum")
        assert cur.fetchone()[0] is enum.FOO


def test_remap_by_value(conn):
    enum = Enum(  # type: ignore
        "ByValue",
        {m.lower(): m for m in PureTestEnum.__members__},
    )
    info = EnumInfo.fetch(conn, "puretestenum")
    register_enum(info, conn, enum, mapping={m: m.value for m in enum})

    for label in PureTestEnum.__members__:
        cur = conn.execute("select %s::text", [enum[label.lower()]])
        assert cur.fetchone()[0] == label

        cur = conn.execute(f"select '{label}'::puretestenum")
        assert cur.fetchone()[0] is enum[label.lower()]
