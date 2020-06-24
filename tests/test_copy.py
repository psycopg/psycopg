import pytest

from psycopg3 import pq
from psycopg3 import errors as e
from psycopg3.adapt import Format
from psycopg3.types import builtins


sample_records = [(10, 20, "hello"), (40, None, "world")]

sample_values = "values (10::int, 20::int, 'hello'::text), (40, NULL, 'world')"

sample_tabledef = "col1 int primary key, col2 int, data text"

sample_text = b"""\
10\t20\thello
40\t\\N\tworld
"""

sample_binary = """
5047 434f 5059 0aff 0d0a 00
00 0000 0000 0000 00
00 0300 0000 0400 0000 0a00 0000 0400 0000 1400 0000 0568 656c 6c6f

0003 0000 0004 0000 0028 ffff ffff 0000 0005 776f 726c 64

ff ff
"""

sample_binary_rows = [
    bytes.fromhex("".join(row.split())) for row in sample_binary.split("\n\n")
]

sample_binary = b"".join(sample_binary_rows)


def set_sample_attributes(res, format):
    attrs = [
        pq.PGresAttDesc(b"col1", 0, 0, format, builtins["int4"].oid, 0, 0),
        pq.PGresAttDesc(b"col2", 0, 0, format, builtins["int4"].oid, 0, 0),
        pq.PGresAttDesc(b"data", 0, 0, format, builtins["text"].oid, 0, 0),
    ]
    res.set_attributes(attrs)


@pytest.mark.xfail
@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_load_noinfo(conn, format, buffer):
    from psycopg3.copy import Copy

    copy = Copy(context=None, result=None, format=format)
    records = copy.load(globals()[buffer])
    assert records == as_bytes(sample_records)


@pytest.mark.xfail
@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_load(conn, format, buffer):
    from psycopg3.copy import Copy

    res = conn.pgconn.make_empty_result(pq.ExecStatus.COPY_OUT)
    set_sample_attributes(res, format)

    copy = Copy(context=None, result=res, format=format)
    records = copy.load(globals()[buffer])
    assert records == sample_records


@pytest.mark.xfail
@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_dump(conn, format, buffer):
    from psycopg3.copy import Copy

    res = conn.pgconn.make_empty_result(pq.ExecStatus.COPY_OUT)
    set_sample_attributes(res, format)

    copy = Copy(context=None, result=res, format=format)
    assert copy.get_buffer() is None
    for row in sample_records:
        copy.dump(row)
    assert copy.get_buffer() == globals()[buffer]
    assert copy.get_buffer() is None


@pytest.mark.xfail
@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_buffers(format, buffer):
    from psycopg3.copy import Copy

    copy = Copy(format=format)
    assert list(copy.buffers(sample_records)) == [globals()[buffer]]


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_out_read(conn, format):
    cur = conn.cursor()
    copy = cur.copy(f"copy ({sample_values}) to stdout (format {format.name})")

    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    for row in want:
        got = copy.read()
        assert got == row

    assert copy.read() is None
    assert copy.read() is None


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_out_iter(conn, format):
    cur = conn.cursor()
    copy = cur.copy(f"copy ({sample_values}) to stdout (format {format.name})")
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows
    assert list(copy) == want


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_copy_in_buffers(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    copy = cur.copy(f"copy copy_in from stdin (format {format.name})")
    copy.write(globals()[buffer])
    copy.finish()
    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_copy_in_buffers_pg_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    copy = cur.copy("copy copy_in from stdin (format text)")
    copy.write(sample_text)
    copy.write(sample_text)
    with pytest.raises(e.UniqueViolation):
        copy.finish()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_copy_in_buffers_with(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        copy.write(globals()[buffer])

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_copy_in_str(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy("copy copy_in from stdin (format text)") as copy:
        copy.write(sample_text.decode("utf8"))

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_copy_in_str_binary(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled):
        with cur.copy("copy copy_in from stdin (format binary)") as copy:
            copy.write(sample_text.decode("utf8"))

    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR


def test_copy_in_buffers_with_pg_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        with cur.copy("copy copy_in from stdin (format text)") as copy:
            copy.write(sample_text)
            copy.write(sample_text)

    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR


def test_copy_in_buffers_with_py_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        with cur.copy("copy copy_in from stdin (format text)") as copy:
            copy.write(sample_text)
            raise Exception("nuttengoggenio")

    assert "nuttengoggenio" in str(exc.value)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR


@pytest.mark.xfail
@pytest.mark.parametrize(
    "format", [(Format.TEXT,), (Format.BINARY,)],
)
def test_copy_in_records(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        for row in sample_records:
            copy.write(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def ensure_table(cur, tabledef, name="copy_in"):
    cur.execute(f"drop table if exists {name}")
    cur.execute(f"create table {name} ({tabledef})")


def as_bytes(records):
    out = []
    for rin in records:
        rout = []
        for v in rin:
            if v is None or isinstance(v, bytes):
                rout.append(v)
                continue
            if not isinstance(v, str):
                v = str(v)
            if isinstance(v, str):
                v = v.encode("utf8")
            rout.append(v)
        out.append(tuple(rout))
    return out
