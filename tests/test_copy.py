import gc
import string
import hashlib
from io import BytesIO, StringIO
from itertools import cycle

import pytest

import psycopg3
from psycopg3 import pq
from psycopg3 import sql
from psycopg3 import errors as e
from psycopg3.pq import Format
from psycopg3.adapt import Format as PgFormat
from psycopg3.types.numeric import Int4

eur = "\u20ac"

sample_records = [(Int4(10), Int4(20), "hello"), (Int4(40), None, "world")]

sample_values = "values (10::int, 20::int, 'hello'::text), (40, NULL, 'world')"

sample_tabledef = "col1 serial primary key, col2 int, data text"

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
def test_copy_out_read(conn, format):
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    cur = conn.cursor()
    with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        for row in want:
            got = copy.read()
            assert got == row
            assert (
                conn.pgconn.transaction_status == conn.TransactionStatus.ACTIVE
            )

        assert copy.read() == b""
        assert copy.read() == b""

    assert copy.read() == b""
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_out_iter(conn, format):
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows
    cur = conn.cursor()
    with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        assert list(copy) == want

    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("typetype", ["names", "oids"])
def test_read_rows(conn, format, typetype):
    cur = conn.cursor()
    with cur.copy(
        f"""copy (
            select 10::int4, 'hello'::text, '{{0.0,1.0}}'::float8[]
        ) to stdout (format {format.name})"""
    ) as copy:
        copy.set_types(["int4", "text", "float8[]"])
        row = copy.read_row()
        assert copy.read_row() is None

    assert row == (10, "hello", [0.0, 1.0])
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_rows(conn, format):
    cur = conn.cursor()
    with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        copy.set_types(["int4", "int4", "text"])
        rows = list(copy.rows())

    assert rows == sample_records
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_out_allchars(conn, format):
    cur = conn.cursor()
    chars = list(map(chr, range(1, 256))) + [eur]
    conn.client_encoding = "utf8"
    rows = []
    query = sql.SQL(
        "copy (select unnest({}::text[])) to stdout (format {})"
    ).format(chars, sql.SQL(format.name))
    with cur.copy(query) as copy:
        copy.set_types(["text"])
        while 1:
            row = copy.read_row()
            if not row:
                break
            assert len(row) == 1
            rows.append(row[0])

    assert rows == chars


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_read_row_notypes(conn, format):
    cur = conn.cursor()
    with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        rows = []
        while 1:
            row = copy.read_row()
            if not row:
                break
            rows.append(row)

    ref = [
        tuple(py_to_raw(i, format) for i in record)
        for record in sample_records
    ]
    assert rows == ref


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_rows_notypes(conn, format):
    cur = conn.cursor()
    with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        rows = list(copy.rows())
    ref = [
        tuple(py_to_raw(i, format) for i in record)
        for record in sample_records
    ]
    assert rows == ref


@pytest.mark.parametrize("err", [-1, 1])
@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_out_badntypes(conn, format, err):
    cur = conn.cursor()
    with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        copy.set_types([0] * (len(sample_records[0]) + err))
        with pytest.raises(e.ProgrammingError):
            copy.read_row()


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_copy_in_buffers(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        copy.write(globals()[buffer])

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_copy_in_buffers_pg_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        with cur.copy("copy copy_in from stdin (format text)") as copy:
            copy.write(sample_text)
            copy.write(sample_text)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR


def test_copy_bad_result(conn):
    conn.autocommit = True

    cur = conn.cursor()

    with pytest.raises(e.SyntaxError):
        with cur.copy("wat"):
            pass

    with pytest.raises(e.ProgrammingError):
        with cur.copy("select 1"):
            pass

    with pytest.raises(e.ProgrammingError):
        with cur.copy("reset timezone"):
            pass


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


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_in_empty(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin (format {format.name})"):
        pass

    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    assert cur.rowcount == 0


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_subclass_adapter(conn, format):
    if format == Format.TEXT:
        from psycopg3.types import StringDumper as BaseDumper
    else:
        from psycopg3.types import StringBinaryDumper as BaseDumper

    class MyStringDumper(BaseDumper):
        def dump(self, obj):
            return super().dump(obj) * 2

    MyStringDumper.register(str, conn)

    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(
        f"copy copy_in (data) from stdin (format {format.name})"
    ) as copy:
        copy.write_row(("hello",))

    rec = cur.execute("select data from copy_in").fetchone()
    assert rec[0] == "hellohello"


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_in_error_empty(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        with cur.copy(f"copy copy_in from stdin (format {format.name})"):
            raise Exception("mannaggiamiseria")

    assert "mannaggiamiseria" in str(exc.value)
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
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        for row in sample_records:
            copy.write_row(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
def test_copy_in_records_binary(conn, format):
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

    conn.client_encoding = "utf8"
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


@pytest.mark.slow
def test_copy_from_to(conn):
    # Roundtrip from file to database to file blockwise
    gen = DataGenerator(conn, nrecs=1024, srec=10 * 1024)
    gen.ensure_table()
    cur = conn.cursor()
    with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            copy.write(block)

    gen.assert_data()

    f = BytesIO()
    with cur.copy("copy copy_in to stdout") as copy:
        for block in copy:
            f.write(block)

    f.seek(0)
    assert gen.sha(f) == gen.sha(gen.file())


@pytest.mark.slow
def test_copy_from_to_bytes(conn):
    # Roundtrip from file to database to file blockwise
    gen = DataGenerator(conn, nrecs=1024, srec=10 * 1024)
    gen.ensure_table()
    cur = conn.cursor()
    with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            copy.write(block.encode("utf8"))

    gen.assert_data()

    f = BytesIO()
    with cur.copy("copy copy_in to stdout") as copy:
        for block in copy:
            f.write(block)

    f.seek(0)
    assert gen.sha(f) == gen.sha(gen.file())


@pytest.mark.slow
def test_copy_from_insane_size(conn):
    # Trying to trigger a "would block" error
    gen = DataGenerator(
        conn, nrecs=4 * 1024, srec=10 * 1024, block_size=20 * 1024 * 1024
    )
    gen.ensure_table()
    cur = conn.cursor()
    with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            copy.write(block)

    gen.assert_data()


def test_copy_rowcount(conn):
    gen = DataGenerator(conn, nrecs=3, srec=10)
    gen.ensure_table()

    cur = conn.cursor()
    with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            copy.write(block)
    assert cur.rowcount == 3

    gen = DataGenerator(conn, nrecs=2, srec=10, offset=3)
    with cur.copy("copy copy_in from stdin") as copy:
        for rec in gen.records():
            copy.write_row(rec)
    assert cur.rowcount == 2

    with cur.copy("copy copy_in to stdout") as copy:
        for block in copy:
            pass
    assert cur.rowcount == 5

    with pytest.raises(e.BadCopyFileFormat):
        with cur.copy("copy copy_in (id) from stdin") as copy:
            for rec in gen.records():
                copy.write_row(rec)
    assert cur.rowcount == -1


def test_copy_query(conn):
    cur = conn.cursor()
    with cur.copy("copy (select 1) to stdout") as copy:
        assert cur.query == b"copy (select 1) to stdout"
        assert cur.params is None
        list(copy)


def test_cant_reenter(conn):
    cur = conn.cursor()
    with cur.copy("copy (select 1) to stdout") as copy:
        list(copy)

    with pytest.raises(TypeError):
        with copy:
            list(copy)


def test_str(conn):
    cur = conn.cursor()
    with cur.copy("copy (select 1) to stdout") as copy:
        assert "[ACTIVE]" in str(copy)
        list(copy)

    assert "[INTRANS]" in str(copy)


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_worker_life(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        assert not copy._worker
        copy.write(globals()[buffer])
        assert copy._worker

    assert not copy._worker
    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.slow
@pytest.mark.parametrize("fmt", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("method", ["read", "iter", "row", "rows"])
def test_copy_to_leaks(dsn, faker, fmt, method):
    faker.format = PgFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    n = []
    for i in range(3):
        with psycopg3.connect(dsn) as conn:
            with conn.cursor(binary=fmt) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)
                cur.executemany(faker.insert_stmt, faker.records)

                stmt = sql.SQL(
                    "copy (select {} from {} order by id) to stdout (format {})"
                ).format(
                    sql.SQL(", ").join(faker.fields_names),
                    faker.table_name,
                    sql.SQL(fmt.name),
                )

                with cur.copy(stmt) as copy:
                    types = [
                        t.as_string(conn).replace('"', "")
                        for t in faker.types_names
                    ]
                    copy.set_types(types)

                    if method == "read":
                        while 1:
                            tmp = copy.read()
                            if not tmp:
                                break
                    elif method == "iter":
                        list(copy)
                    elif method == "row":
                        while 1:
                            tmp = copy.read_row()
                            if tmp is None:
                                break
                    elif method == "rows":
                        list(copy.rows())

                    tmp = None

        del cur, conn
        gc.collect()
        gc.collect()
        n.append(len(gc.get_objects()))

    assert (
        n[0] == n[1] == n[2]
    ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


@pytest.mark.slow
@pytest.mark.parametrize("fmt", [Format.TEXT, Format.BINARY])
def test_copy_from_leaks(dsn, faker, fmt):
    faker.format = PgFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    n = []
    for i in range(3):
        with psycopg3.connect(dsn) as conn:
            with conn.cursor(binary=fmt) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)

                stmt = sql.SQL("copy {} ({}) from stdin (format {})").format(
                    faker.table_name,
                    sql.SQL(", ").join(faker.fields_names),
                    sql.SQL(fmt.name),
                )
                with cur.copy(stmt) as copy:
                    for row in faker.records:
                        copy.write_row(row)

                cur.execute(faker.select_stmt)
                recs = cur.fetchall()

                for got, want in zip(recs, faker.records):
                    faker.assert_record(got, want)

                del recs

        del cur, conn
        gc.collect()
        gc.collect()
        n.append(len(gc.get_objects()))

    assert (
        n[0] == n[1] == n[2]
    ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


def py_to_raw(item, fmt):
    """Convert from Python type to the expected result from the db"""
    if fmt == Format.TEXT:
        if isinstance(item, int):
            return str(item)
    else:
        if isinstance(item, int):
            return bytes([0, 0, 0, item])
        elif isinstance(item, str):
            return item.encode("utf8")
    return item


def ensure_table(cur, tabledef, name="copy_in"):
    cur.execute(f"drop table if exists {name}")
    cur.execute(f"create table {name} ({tabledef})")


class DataGenerator:
    def __init__(self, conn, nrecs, srec, offset=0, block_size=8192):
        self.conn = conn
        self.nrecs = nrecs
        self.srec = srec
        self.offset = offset
        self.block_size = block_size

    def ensure_table(self):
        cur = self.conn.cursor()
        ensure_table(cur, "id integer primary key, data text")

    def records(self):
        for i, c in zip(range(self.nrecs), cycle(string.ascii_letters)):
            s = c * self.srec
            yield (i + self.offset, s)

    def file(self):
        f = StringIO()
        for i, s in self.records():
            f.write("%s\t%s\n" % (i, s))

        f.seek(0)
        return f

    def blocks(self):
        f = self.file()
        while True:
            block = f.read(self.block_size)
            if not block:
                break
            yield block

    def assert_data(self):
        cur = self.conn.cursor()
        cur.execute("select id, data from copy_in order by id")
        for record in self.records():
            assert record == cur.fetchone()

        assert cur.fetchone() is None

    def sha(self, f):
        m = hashlib.sha256()
        while 1:
            block = f.read()
            if not block:
                break
            if isinstance(block, str):
                block = block.encode("utf8")
            m.update(block)
        return m.hexdigest()
