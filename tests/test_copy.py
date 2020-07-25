import pytest

from psycopg3 import pq
from psycopg3 import errors as e
from psycopg3.adapt import Format

eur = "\u20ac"

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


def test_copy_bad_result(conn):
    conn.autocommit = True

    cur = conn.cursor()

    with pytest.raises(e.SyntaxError):
        cur.copy("wat")

    with pytest.raises(e.ProgrammingError):
        cur.copy("select 1")

    with pytest.raises(e.ProgrammingError):
        cur.copy("reset timezone")


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


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_in_records(conn, format):
    if format == Format.BINARY:
        pytest.skip("TODO: implement int binary adapter")

    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        for row in sample_records:
            copy.write_row(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_in_records_binary(conn, format):
    if format == Format.TEXT:
        pytest.skip("TODO: remove after implementing int binary adapter")

    cur = conn.cursor()
    ensure_table(cur, "col1 serial primary key, col2 int, data text")

    with cur.copy(
        f"copy copy_in (col2, data) from stdin (format {format.name})"
    ) as copy:
        for row in sample_records:
            copy.write_row((None, row[2]))

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == [(1, None, "hello"), (2, None, "world")]


def test_copy_in_allchars(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    conn.encoding = "utf8"
    with cur.copy("copy copy_in from stdin (format text)") as copy:
        for i in range(1, 256):
            copy.write_row((i, None, chr(i)))
        copy.write_row((ord(eur), None, eur))

    data = cur.execute(
        """
select col1 = ascii(data), col2 is null, length(data), count(*)
from copy_in group by 1, 2, 3
"""
    ).fetchall()
    assert data == [(True, True, 1, 256)]


def ensure_table(cur, tabledef, name="copy_in"):
    cur.execute(f"drop table if exists {name}")
    cur.execute(f"create table {name} ({tabledef})")
