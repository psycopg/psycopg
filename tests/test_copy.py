import pytest

sample_records = [(10, 20, "hello"), (40, None, "world")]

sample_values = "values (10::int, 20::int, 'hello'::text), (40, NULL, 'world')"

sample_tabledef = "col1 int primary key, col2 int, date text"

sample_text = b"""
10\t20\thello
40\t\\N\tworld
"""

sample_binary = """
5047 434f 5059 0aff 0d0a 0000 0000 0000
0000 0000 0300 0000 0400 0000 0a00 0000
0400 0000 1400 0000 0568 656c 6c6f 0003
0000 0004 0000 0028 ffff ffff 0000 0005
776f 726c 64ff ff
"""


@pytest.mark.parametrize(
    "format, block", [("text", sample_text), ("binary", sample_binary)]
)
def test_load(format, block):
    from psycopg3.copy import Copy

    copy = Copy(format=format)
    records = copy.load(block)
    assert records == sample_records


@pytest.mark.parametrize(
    "format, block", [("text", sample_text), ("binary", sample_binary)]
)
def test_dump(format, block):
    from psycopg3.copy import Copy

    copy = Copy(format=format)
    assert copy.get_buffer() is None
    for row in sample_records:
        copy.dump(row)
    assert copy.get_buffer() == block
    assert copy.get_buffer() is None


@pytest.mark.parametrize(
    "format, block", [("text", sample_text), ("binary", sample_binary)]
)
def test_buffers(format, block):
    from psycopg3.copy import Copy

    copy = Copy(format=format)
    assert list(copy.buffers(sample_records)) == [block]


@pytest.mark.parametrize(
    "format, want", [("text", sample_text), ("binary", sample_binary)]
)
def test_copy_out_read(conn, format, want):
    cur = conn.cursor()
    copy = cur.copy(f"copy ({sample_values}) to stdout (format {format})")
    assert copy.read() == want
    assert copy.read() is None
    assert copy.read() is None


@pytest.mark.parametrize("format", ["text", "binary"])
def test_iter(conn, format):
    cur = conn.cursor()
    copy = cur.copy(f"copy ({sample_values}) to stdout (format {format})")
    assert list(copy) == sample_records


@pytest.mark.parametrize(
    "format, buffer", [("text", sample_text), ("binary", sample_binary)]
)
def test_copy_in_buffers(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    copy = cur.copy(f"copy copy_in from stdin (format {format})")
    copy.write(buffer)
    copy.end()
    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize(
    "format, buffer", [("text", sample_text), ("binary", sample_binary)]
)
def test_copy_in_buffers_with(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin (format {format})") as copy:
        copy.write(buffer)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize(
    "format, buffer", [("text", sample_text), ("binary", sample_binary)]
)
def test_copy_in_records(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin (format {format})") as copy:
        for row in sample_records:
            copy.write(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def ensure_table(cur, tabledef, name="copy_in"):
    cur.execute(f"drop table if exists {name}")
    cur.execute(f"create table {name} ({tabledef})")
