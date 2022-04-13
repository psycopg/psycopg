from enum import Enum

import pytest
from psycopg.types.enum import EnumInfo, register_enum

from psycopg import pq


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


def test_fetch_info(conn, testenum):
    assert testenum.name == "testenum"
    assert testenum.oid > 0
    assert testenum.oid != testenum.array_oid > 0
    assert len(testenum.enum_labels) == 3
    assert testenum.enum_labels == ["ONE", "TWO", "THREE"]


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
