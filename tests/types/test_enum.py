from enum import Enum

import pytest

from psycopg import pq
from psycopg.adapt import PyFormat
from psycopg.types.enum import EnumInfo, register_enum


class _TestEnum(str, Enum):
    ONE = "ONE"
    TWO = "TWO"
    THREE = "THREE"


@pytest.fixture(scope="session")
def testenum(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        drop type if exists testenum cascade;
        create type testenum as enum('ONE', 'TWO', 'THREE');
        """
    )
    return EnumInfo.fetch(svcconn, "testenum")


@pytest.fixture(scope="session")
def nonasciienum(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        drop type if exists nonasciienum cascade;
        create type nonasciienum as enum ('x\xe0');
        """
    )
    return EnumInfo.fetch(svcconn, "nonasciienum")


def test_fetch_info(conn, testenum):
    assert testenum.name == "testenum"
    assert testenum.oid > 0
    assert testenum.oid != testenum.array_oid > 0
    assert len(testenum.enum_labels) == 3
    assert testenum.enum_labels == ["ONE", "TWO", "THREE"]


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_enum_insert_generic(conn, testenum, fmt_in):
    # No regstration, test for generic enum
    conn.execute("create table test_enum_insert (id int primary key, val testenum)")
    cur = conn.cursor()
    cur.executemany(
        f"insert into test_enum_insert (id, val) values (%s, %{fmt_in})",
        list(enumerate(_TestEnum)),
    )
    cur.execute("select id, val from test_enum_insert order by id")
    recs = cur.fetchall()
    assert recs == [(0, "ONE"), (1, "TWO"), (2, "THREE")]


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_enum_dumper(conn, testenum, fmt_in):
    register_enum(testenum, _TestEnum, conn)

    cur = conn.execute(f"select %{fmt_in}", [_TestEnum.ONE])
    assert cur.fetchone()[0] is _TestEnum.ONE


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_loader(conn, testenum, fmt_out):
    register_enum(testenum, _TestEnum, conn)

    cur = conn.execute("select 'ONE'::testenum", binary=fmt_out)
    assert cur.fetchone()[0] == _TestEnum.ONE


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_array_loader(conn, testenum, fmt_out):
    register_enum(testenum, _TestEnum, conn)

    cur = conn.execute(
        "select ARRAY['ONE'::testenum, 'TWO'::testenum]::testenum[]",
        binary=fmt_out,
    )
    assert cur.fetchone()[0] == [_TestEnum.ONE, _TestEnum.TWO]


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_loader_generated(conn, testenum, fmt_out):
    register_enum(testenum, context=conn)

    cur = conn.execute("select 'ONE'::testenum", binary=fmt_out)
    assert cur.fetchone()[0] == testenum.python_type.ONE


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_array_loader_generated(conn, testenum, fmt_out):
    register_enum(testenum, context=conn)

    cur = conn.execute(
        "select ARRAY['ONE'::testenum, 'TWO'::testenum]::testenum[]",
        binary=fmt_out,
    )
    assert cur.fetchone()[0] == [testenum.python_type.ONE, testenum.python_type.TWO]


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum(conn, testenum, fmt_out):
    register_enum(testenum, _TestEnum, conn)

    cur = conn.cursor()
    cur.execute(
        """
        drop table if exists testenumtable;
        create table testenumtable (id serial primary key, value testenum);
        """
    )

    cur.execute(
        "insert into testenumtable (value) values (%s), (%s), (%s)",
        (
            _TestEnum.ONE,
            _TestEnum.TWO,
            _TestEnum.THREE,
        ),
    )

    cur = conn.execute("select value from testenumtable order by id", binary=fmt_out)
    assert cur.fetchone()[0] == _TestEnum.ONE
    assert cur.fetchone()[0] == _TestEnum.TWO
    assert cur.fetchone()[0] == _TestEnum.THREE


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_enum_array(conn, testenum, fmt_out):
    register_enum(testenum, _TestEnum, conn)

    cur = conn.cursor()
    cur.execute(
        """
        drop table if exists testenumtable;
        create table testenumtable (id serial primary key, values testenum[]);
        """
    )

    cur.execute(
        "insert into testenumtable (values) values (%s), (%s), (%s)",
        (
            [_TestEnum.ONE, _TestEnum.TWO],
            [_TestEnum.TWO, _TestEnum.THREE],
            [_TestEnum.THREE, _TestEnum.ONE],
        ),
    )

    cur = conn.execute("select values from testenumtable order by id", binary=fmt_out)
    assert cur.fetchone()[0] == [_TestEnum.ONE, _TestEnum.TWO]
    assert cur.fetchone()[0] == [_TestEnum.TWO, _TestEnum.THREE]
    assert cur.fetchone()[0] == [_TestEnum.THREE, _TestEnum.ONE]


@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("encoding", ["utf8", "latin1"])
def test_non_ascii_enum(conn, nonasciienum, fmt_out, fmt_in, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    info = EnumInfo.fetch(conn, "nonasciienum")
    register_enum(info, context=conn)
    assert [x.name for x in info.python_type] == ["x\xe0"]
    val = list(info.python_type)[0]

    cur = conn.execute("select 'x\xe0'::nonasciienum", binary=fmt_out)
    assert cur.fetchone()[0] is val

    cur = conn.execute(f"select %{fmt_in}", [val])
    assert cur.fetchone()[0] is val
