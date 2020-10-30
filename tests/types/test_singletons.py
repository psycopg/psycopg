import pytest

from psycopg3 import sql
from psycopg3.oids import builtins
from psycopg3.adapt import Transformer, Format


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("b", [True, False, None])
def test_roundtrip_bool(conn, b, fmt_in, fmt_out):
    cur = conn.cursor(format=fmt_out)
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    cast = "" if conn.pgconn.server_version > 100000 else "::bool"
    result = cur.execute(f"select {ph}{cast}", (b,)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    if b is not None:
        assert cur.pgresult.ftype(0) == builtins["bool"].oid
    assert result is b

    result = cur.execute(f"select {ph}", ([b],)).fetchone()[0]
    assert cur.pgresult.fformat(0) == fmt_out
    if b is not None:
        assert cur.pgresult.ftype(0) == builtins["bool"].array_oid
    assert result[0] is b


@pytest.mark.parametrize("val", [True, False])
def test_quote_bool(conn, val):

    tx = Transformer()
    assert tx.get_dumper(val, 0).quote(val) == str(val).lower().encode("ascii")

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}").format(v=sql.Literal(val)))
    assert cur.fetchone()[0] is val


def test_quote_none(conn):

    tx = Transformer()
    assert tx.get_dumper(None, 0).quote(None) == b"NULL"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}").format(v=sql.Literal(None)))
    assert cur.fetchone()[0] is None
