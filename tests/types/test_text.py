import pytest

import psycopg3
from psycopg3 import pq
from psycopg3 import sql
from psycopg3.adapt import Format

eur = "\u20ac"


#
# tests with text
#


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_1char(conn, fmt_in):
    cur = conn.cursor()
    for i in range(1, 256):
        cur.execute(f"select %{fmt_in} = chr(%s::int)", (chr(i), i))
        assert cur.fetchone()[0] is True, chr(i)


def test_quote_1char(conn):
    cur = conn.cursor()
    query = sql.SQL("select {ch} = chr(%s::int)")
    for i in range(1, 256):
        if chr(i) == "%":
            continue
        cur.execute(query.format(ch=sql.Literal(chr(i))), (i,))
        assert cur.fetchone()[0] is True, chr(i)


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_zero(conn, fmt_in):
    cur = conn.cursor()
    s = "foo\x00bar"
    with pytest.raises(psycopg3.DataError):
        cur.execute(f"select %{fmt_in}::text", (s,))


def test_quote_zero(conn):
    cur = conn.cursor()
    s = "foo\x00bar"
    with pytest.raises(psycopg3.DataError):
        cur.execute(sql.SQL("select {}").format(sql.Literal(s)))


# the only way to make this pass is to reduce %% -> % every time
# not only when there are query arguments
# see https://github.com/psycopg/psycopg2/issues/825
@pytest.mark.xfail
def test_quote_percent(conn):
    cur = conn.cursor()
    cur.execute(sql.SQL("select {ch}").format(ch=sql.Literal("%")))
    assert cur.fetchone()[0] == "%"

    cur.execute(
        sql.SQL("select {ch} = chr(%s::int)").format(ch=sql.Literal("%")),
        (ord("%"),),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_1char(conn, typename, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 256):
        cur.execute(f"select chr(%s::int)::{typename}", (i,))
        res = cur.fetchone()[0]
        assert res == chr(i)

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("encoding", ["utf8", "latin9", "ascii"])
def test_dump_enc(conn, fmt_in, encoding):
    cur = conn.cursor()

    conn.client_encoding = encoding
    (res,) = cur.execute(f"select ascii(%{fmt_in})", (eur,)).fetchone()
    assert res == ord(eur)


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_badenc(conn, fmt_in):
    cur = conn.cursor()

    conn.client_encoding = "latin1"
    with pytest.raises(UnicodeEncodeError):
        cur.execute(f"select %{fmt_in}::bytea", (eur,))


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_dump_utf8_badenc(conn, fmt_in):
    cur = conn.cursor()

    conn.client_encoding = "utf-8"
    with pytest.raises(UnicodeEncodeError):
        cur.execute(f"select %{fmt_in}", ("\uddf8",))


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_enc(conn, typename, encoding, fmt_out):
    cur = conn.cursor(binary=fmt_out)

    conn.client_encoding = encoding
    (res,) = cur.execute(
        f"select chr(%s::int)::{typename}", (ord(eur),)
    ).fetchone()
    assert res == eur

    stmt = sql.SQL("copy (select chr({}::int)) to stdout (format {})").format(
        ord(eur), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([typename])
        (res,) = copy.read_row()

    assert res == eur


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_badenc(conn, typename, fmt_out):
    conn.autocommit = True
    cur = conn.cursor(binary=fmt_out)

    conn.client_encoding = "latin1"
    with pytest.raises(psycopg3.DataError):
        cur.execute(f"select chr(%s::int)::{typename}", (ord(eur),))

    stmt = sql.SQL("copy (select chr({}::int)) to stdout (format {})").format(
        ord(eur), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([typename])
        with pytest.raises(psycopg3.DataError):
            copy.read_row()


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_ascii(conn, typename, fmt_out):
    cur = conn.cursor(binary=fmt_out)

    conn.client_encoding = "ascii"
    cur.execute(f"select chr(%s::int)::{typename}", (ord(eur),))
    assert cur.fetchone()[0] == eur.encode("utf8")

    stmt = sql.SQL("copy (select chr({}::int)) to stdout (format {})").format(
        ord(eur), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([typename])
        (res,) = copy.read_row()

    assert res == eur.encode("utf8")


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_text_array(conn, typename, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    a = list(map(chr, range(1, 256))) + [eur]

    (res,) = cur.execute(f"select %{fmt_in}::{typename}[]", (a,)).fetchone()
    assert res == a


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_text_array_ascii(conn, fmt_in, fmt_out):
    conn.client_encoding = "ascii"
    cur = conn.cursor(binary=fmt_out)
    a = list(map(chr, range(1, 256))) + [eur]
    exp = [s.encode("utf8") for s in a]
    (res,) = cur.execute(f"select %{fmt_in}::text[]", (a,)).fetchone()
    assert res == exp


#
# tests with bytea
#


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("pytype", [bytes, bytearray, memoryview])
def test_dump_1byte(conn, fmt_in, pytype):
    cur = conn.cursor()
    for i in range(0, 256):
        obj = pytype(bytes([i]))
        cur.execute(f"select %{fmt_in} = %s::bytea", (obj, fr"\x{i:02x}"))
        assert cur.fetchone()[0] is True, i


def test_quote_1byte(conn):
    cur = conn.cursor()
    query = sql.SQL("select {ch} = %s::bytea")
    for i in range(0, 256):
        cur.execute(query.format(ch=sql.Literal(bytes([i]))), (fr"\x{i:02x}",))
        assert cur.fetchone()[0] is True, i


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_load_1byte(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(0, 256):
        cur.execute("select %s::bytea", (fr"\x{i:02x}",))
        assert cur.fetchone()[0] == bytes([i])

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_bytea_array(conn, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    a = [bytes(range(0, 256))]
    (res,) = cur.execute(f"select %{fmt_in}::bytea[]", (a,)).fetchone()
    assert res == a
