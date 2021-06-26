import pytest

from psycopg3 import pq
from psycopg3 import sql
from psycopg3.oids import postgres_types as builtins
from psycopg3.adapt import Transformer, Format


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("b", [True, False])
def test_roundtrip_bool(conn, b, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    result = cur.execute(f"select %{fmt_in}", (b,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    if b is not None:
        assert cur.pgresult.ftype(0) == builtins["bool"].oid
    assert result is b

    result = cur.execute(f"select %{fmt_in}", ([b],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    if b is not None:
        assert cur.pgresult.ftype(0) == builtins["bool"].array_oid
    assert result[0] is b


@pytest.mark.parametrize("val", [True, False])
def test_quote_bool(conn, val):

    tx = Transformer()
    assert tx.get_dumper(val, Format.TEXT).quote(val) == str(
        val
    ).lower().encode("ascii")

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}").format(v=sql.Literal(val)))
    assert cur.fetchone()[0] is val


def test_quote_none(conn):

    tx = Transformer()
    assert tx.get_dumper(None, Format.TEXT).quote(None) == b"NULL"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}").format(v=sql.Literal(None)))
    assert cur.fetchone()[0] is None
