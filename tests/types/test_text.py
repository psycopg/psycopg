import pytest

import psycopg3
from psycopg3.adapt import Format

eur = "\u20ac"


#
# tests with text
#


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_adapt_1char(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    for i in range(1, 256):
        cur.execute(f"select {ph} = chr(%s::int)", (chr(i), i))
        assert cur.fetchone()[0], chr(i)


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_1char(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    for i in range(1, 256):
        cur.execute("select chr(%s::int)", (i,))
        assert cur.fetchone()[0] == chr(i)

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_adapt_enc(conn, fmt_in, encoding):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"

    conn.encoding = encoding
    (res,) = cur.execute(f"select {ph}::bytea", (eur,)).fetchone()
    assert res == eur.encode("utf8")


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_adapt_ascii(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"

    conn.encoding = "sql_ascii"
    (res,) = cur.execute(f"select ascii({ph})", (eur,)).fetchone()
    assert res == ord(eur)


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_adapt_badenc(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"

    conn.encoding = "latin1"
    with pytest.raises(UnicodeEncodeError):
        cur.execute(f"select {ph}::bytea", (eur,))


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_cast_enc(conn, fmt_out, encoding):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)

    conn.encoding = encoding
    (res,) = cur.execute("select chr(%s::int)", (ord(eur),)).fetchone()
    assert res == eur


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_badenc(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)

    conn.encoding = "latin1"
    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("select chr(%s::int)", (ord(eur),))


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_ascii(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)

    conn.encoding = "sql_ascii"
    (res,) = cur.execute("select chr(%s::int)", (ord(eur),)).fetchone()
    assert res == eur.encode("utf8")


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_text_array(conn, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    a = list(map(chr, range(1, 256))) + [eur]

    (res,) = cur.execute(f"select {ph}::text[]", (a,)).fetchone()
    assert res == a


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_text_array_ascii(conn, fmt_in, fmt_out):
    conn.encoding = "sql_ascii"
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    a = list(map(chr, range(1, 256))) + [eur]
    exp = [s.encode("utf8") for s in a]
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    (res,) = cur.execute(f"select {ph}::text[]", (a,)).fetchone()
    assert res == exp


#
# tests with bytea
#


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_adapt_1byte(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    for i in range(0, 256):
        cur.execute(f"select {ph} = %s::bytea", (bytes([i]), fr"\x{i:02x}"))
        assert cur.fetchone()[0], i


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_1byte(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    for i in range(0, 256):
        cur.execute("select %s::bytea", (fr"\x{i:02x}",))
        assert cur.fetchone()[0] == bytes([i])

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_bytea_array(conn, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    a = [bytes(range(0, 256))]
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    (res,) = cur.execute(f"select {ph}::bytea[]", (a,)).fetchone()
    assert res == a
