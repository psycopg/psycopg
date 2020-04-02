import pytest

from psycopg3.adapt import Format


#
# tests with text
#


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_adapt_1char(conn, format):
    cur = conn.cursor()
    query = "select %s = chr(%%s::int)" % (
        "%s" if format == Format.TEXT else "%b"
    )
    for i in range(1, 256):
        cur.execute(query, (chr(i), i))
        assert cur.fetchone()[0], chr(i)


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_cast_1char(conn, format):
    cur = conn.cursor(binary=format == Format.BINARY)
    for i in range(1, 256):
        cur.execute("select chr(%s::int)", (i,))
        assert cur.fetchone()[0] == chr(i)

    assert cur.pgresult.fformat(0) == format


#
# tests with bytea
#


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_adapt_1byte(conn, format):
    cur = conn.cursor()
    query = "select %s = %%s::bytea" % (
        "%s" if format == Format.TEXT else "%b"
    )
    for i in range(0, 256):
        cur.execute(query, (bytes([i]), fr"\x{i:02x}"))
        assert cur.fetchone()[0], i


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_cast_1byte(conn, format):
    cur = conn.cursor(binary=format == Format.BINARY)
    for i in range(0, 256):
        cur.execute("select %s::bytea", (fr"\x{i:02x}",))
        assert cur.fetchone()[0] == bytes([i])

    assert cur.pgresult.fformat(0) == format
