import pytest

import psycopg
from psycopg import pq
from psycopg import sql
from psycopg import errors as e
from psycopg.adapt import PyFormat
from psycopg import Binary

eur = "\u20ac"

#
# tests with text
#


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_1char(conn, fmt_in):
    cur = conn.cursor()
    for i in range(1, 256):
        cur.execute(f"select %{fmt_in} = chr(%s)", (chr(i), i))
        assert cur.fetchone()[0] is True, chr(i)


@pytest.mark.parametrize("scs", ["on", "off"])
def test_quote_1char(conn, scs):
    messages = []
    conn.add_notice_handler(lambda msg: messages.append(msg.message_primary))
    conn.execute(f"set standard_conforming_strings to {scs}")
    conn.execute("set escape_string_warning to on")

    cur = conn.cursor()
    query = sql.SQL("select {ch} = chr(%s)")
    for i in range(1, 256):
        if chr(i) == "%":
            continue
        cur.execute(query.format(ch=sql.Literal(chr(i))), (i,))
        assert cur.fetchone()[0] is True, chr(i)

    # No "nonstandard use of \\ in a string literal" warning
    assert not messages


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_zero(conn, fmt_in):
    cur = conn.cursor()
    s = "foo\x00bar"
    with pytest.raises(psycopg.DataError):
        cur.execute(f"select %{fmt_in}::text", (s,))


def test_quote_zero(conn):
    cur = conn.cursor()
    s = "foo\x00bar"
    with pytest.raises(psycopg.DataError):
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
        sql.SQL("select {ch} = chr(%s)").format(ch=sql.Literal("%")),
        (ord("%"),),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize(
    "typename", ["text", "varchar", "name", "bpchar", '"char"']
)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_1char(conn, typename, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 256):
        if typename == '"char"' and i > 127:
            # for char > 128 the client receives only 194 or 195.
            continue

        cur.execute(f"select chr(%s)::{typename}", (i,))
        res = cur.fetchone()[0]
        assert res == chr(i)

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("encoding", ["utf8", "latin9", "sql_ascii"])
def test_dump_enc(conn, fmt_in, encoding):
    cur = conn.cursor()

    conn.execute(f"set client_encoding to {encoding}")
    (res,) = cur.execute(f"select ascii(%{fmt_in})", (eur,)).fetchone()
    assert res == ord(eur)


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_badenc(conn, fmt_in):
    cur = conn.cursor()

    conn.execute("set client_encoding to latin1")
    with pytest.raises(UnicodeEncodeError):
        cur.execute(f"select %{fmt_in}::bytea", (eur,))


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump_utf8_badenc(conn, fmt_in):
    cur = conn.cursor()

    conn.execute("set client_encoding to utf8")
    with pytest.raises(UnicodeEncodeError):
        cur.execute(f"select %{fmt_in}", ("\uddf8",))


@pytest.mark.parametrize("fmt_in", [PyFormat.AUTO, PyFormat.TEXT])
def test_dump_enum(conn, fmt_in):
    from enum import Enum

    class MyEnum(str, Enum):
        foo = "foo"
        bar = "bar"

    cur = conn.cursor()
    cur.execute("create type myenum as enum ('foo', 'bar')")
    cur.execute("create table with_enum (e myenum)")
    cur.execute(f"insert into with_enum (e) values (%{fmt_in})", (MyEnum.foo,))
    (res,) = cur.execute("select e from with_enum").fetchone()
    assert res == "foo"


@pytest.mark.parametrize("fmt_in", [PyFormat.AUTO, PyFormat.TEXT])
def test_dump_text_oid(conn, fmt_in):
    conn.autocommit = True

    with pytest.raises(e.IndeterminateDatatype):
        conn.execute(f"select concat(%{fmt_in}, %{fmt_in})", ["foo", "bar"])
    conn.adapters.register_dumper(str, psycopg.types.string.StrDumper)
    cur = conn.execute(f"select concat(%{fmt_in}, %{fmt_in})", ["foo", "bar"])
    assert cur.fetchone()[0] == "foobar"


@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_enc(conn, typename, encoding, fmt_out):
    cur = conn.cursor(binary=fmt_out)

    conn.execute(f"set client_encoding to {encoding}")
    (res,) = cur.execute(f"select chr(%s)::{typename}", [ord(eur)]).fetchone()
    assert res == eur

    stmt = sql.SQL("copy (select chr({})) to stdout (format {})").format(
        ord(eur), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([typename])
        (res,) = copy.read_row()

    assert res == eur


@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_badenc(conn, typename, fmt_out):
    conn.autocommit = True
    cur = conn.cursor(binary=fmt_out)

    conn.execute("set client_encoding to latin1")
    with pytest.raises(psycopg.DataError):
        cur.execute(f"select chr(%s)::{typename}", [ord(eur)])

    stmt = sql.SQL("copy (select chr({})) to stdout (format {})").format(
        ord(eur), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([typename])
        with pytest.raises(psycopg.DataError):
            copy.read_row()


@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_ascii(conn, typename, fmt_out):
    cur = conn.cursor(binary=fmt_out)

    conn.execute("set client_encoding to sql_ascii")
    cur.execute(f"select chr(%s)::{typename}", [ord(eur)])
    assert cur.fetchone()[0] == eur.encode()

    stmt = sql.SQL("copy (select chr({})) to stdout (format {})").format(
        ord(eur), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([typename])
        (res,) = copy.read_row()

    assert res == eur.encode()


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_text_array(conn, typename, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    a = list(map(chr, range(1, 256))) + [eur]

    (res,) = cur.execute(f"select %{fmt_in}::{typename}[]", (a,)).fetchone()
    assert res == a


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_text_array_ascii(conn, fmt_in, fmt_out):
    conn.execute("set client_encoding to sql_ascii")
    cur = conn.cursor(binary=fmt_out)
    a = list(map(chr, range(1, 256))) + [eur]
    exp = [s.encode() for s in a]
    (res,) = cur.execute(f"select %{fmt_in}::text[]", (a,)).fetchone()
    assert res == exp


#
# tests with bytea
#


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("pytype", [bytes, bytearray, memoryview, Binary])
def test_dump_1byte(conn, fmt_in, pytype):
    cur = conn.cursor()
    for i in range(0, 256):
        obj = pytype(bytes([i]))
        cur.execute(f"select %{fmt_in} = set_byte('x', 0, %s)", (obj, i))
        assert cur.fetchone()[0] is True, i

    cur.execute(f"select %{fmt_in} = array[set_byte('x', 0, %s)]", ([obj], i))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("scs", ["on", "off"])
@pytest.mark.parametrize("pytype", [bytes, bytearray, memoryview, Binary])
def test_quote_1byte(conn, scs, pytype):
    messages = []
    conn.add_notice_handler(lambda msg: messages.append(msg.message_primary))
    conn.execute(f"set standard_conforming_strings to {scs}")
    conn.execute("set escape_string_warning to on")

    cur = conn.cursor()
    query = sql.SQL("select {ch} = set_byte('x', 0, %s)")
    for i in range(0, 256):
        obj = pytype(bytes([i]))
        cur.execute(query.format(ch=sql.Literal(obj)), (i,))
        assert cur.fetchone()[0] is True, i

    # No "nonstandard use of \\ in a string literal" warning
    assert not messages


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_1byte(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(0, 256):
        cur.execute("select set_byte('x', 0, %s)", (i,))
        assert cur.fetchone()[0] == bytes([i])

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_bytea_array(conn, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    a = [bytes(range(0, 256))]
    (res,) = cur.execute(f"select %{fmt_in}::bytea[]", (a,)).fetchone()
    assert res == a
