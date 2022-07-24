import gc
import string
import struct
import hashlib
from io import BytesIO, StringIO
from random import choice, randrange
from itertools import cycle

import pytest

import psycopg
from psycopg import pq
from psycopg import sql
from psycopg import errors as e
from psycopg.pq import Format
from psycopg.copy import Copy, LibpqWriter, QueuedLibpqDriver, FileWriter
from psycopg.adapt import PyFormat
from psycopg.types import TypeInfo
from psycopg.types.hstore import register_hstore
from psycopg.types.numeric import Int4

from .utils import eur, gc_collect

pytestmark = pytest.mark.crdb_skip("copy")

sample_records = [(40010, 40020, "hello"), (40040, None, "world")]
sample_values = "values (40010::int, 40020::int, 'hello'::text), (40040, NULL, 'world')"
sample_tabledef = "col1 serial primary key, col2 int, data text"

sample_text = b"""\
40010\t40020\thello
40040\t\\N\tworld
"""

sample_binary_str = """
5047 434f 5059 0aff 0d0a 00
00 0000 0000 0000 00
00 0300 0000 0400 009c 4a00 0000 0400 009c 5400 0000 0568 656c 6c6f

0003 0000 0004 0000 9c68 ffff ffff 0000 0005 776f 726c 64

ff ff
"""

sample_binary_rows = [
    bytes.fromhex("".join(row.split())) for row in sample_binary_str.split("\n\n")
]
sample_binary = b"".join(sample_binary_rows)

special_chars = {8: "b", 9: "t", 10: "n", 11: "v", 12: "f", 13: "r", ord("\\"): "\\"}


@pytest.mark.parametrize("format", Format)
def test_copy_out_read(conn, format):
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    cur = conn.cursor()
    with cur.copy(f"copy ({sample_values}) to stdout (format {format.name})") as copy:
        for row in want:
            got = copy.read()
            assert got == row
            assert conn.info.transaction_status == conn.TransactionStatus.ACTIVE

        assert copy.read() == b""
        assert copy.read() == b""

    assert copy.read() == b""
    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", Format)
def test_copy_out_iter(conn, format):
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    cur = conn.cursor()
    with cur.copy(f"copy ({sample_values}) to stdout (format {format.name})") as copy:
        assert list(copy) == want

    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("ph, params", [("%s", (10,)), ("%(n)s", {"n": 10})])
def test_copy_out_param(conn, ph, params):
    cur = conn.cursor()
    with cur.copy(
        f"copy (select * from generate_series(1, {ph})) to stdout", params
    ) as copy:
        copy.set_types(["int4"])
        assert list(copy.rows()) == [(i + 1,) for i in range(10)]

    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", Format)
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
    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", Format)
def test_rows(conn, format):
    cur = conn.cursor()
    with cur.copy(f"copy ({sample_values}) to stdout (format {format.name})") as copy:
        copy.set_types(["int4", "int4", "text"])
        rows = list(copy.rows())

    assert rows == sample_records
    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS


def test_set_custom_type(conn, hstore):
    command = """copy (select '"a"=>"1", "b"=>"2"'::hstore) to stdout"""
    cur = conn.cursor()

    with cur.copy(command) as copy:
        rows = list(copy.rows())

    assert rows == [('"a"=>"1", "b"=>"2"',)]

    register_hstore(TypeInfo.fetch(conn, "hstore"), cur)
    with cur.copy(command) as copy:
        copy.set_types(["hstore"])
        rows = list(copy.rows())

    assert rows == [({"a": "1", "b": "2"},)]


@pytest.mark.parametrize("format", Format)
def test_copy_out_allchars(conn, format):
    cur = conn.cursor()
    chars = list(map(chr, range(1, 256))) + [eur]
    conn.execute("set client_encoding to utf8")
    rows = []
    query = sql.SQL("copy (select unnest({}::text[])) to stdout (format {})").format(
        chars, sql.SQL(format.name)
    )
    with cur.copy(query) as copy:
        copy.set_types(["text"])
        while True:
            row = copy.read_row()
            if not row:
                break
            assert len(row) == 1
            rows.append(row[0])

    assert rows == chars


@pytest.mark.parametrize("format", Format)
def test_read_row_notypes(conn, format):
    cur = conn.cursor()
    with cur.copy(f"copy ({sample_values}) to stdout (format {format.name})") as copy:
        rows = []
        while True:
            row = copy.read_row()
            if not row:
                break
            rows.append(row)

    ref = [tuple(py_to_raw(i, format) for i in record) for record in sample_records]
    assert rows == ref


@pytest.mark.parametrize("format", Format)
def test_rows_notypes(conn, format):
    cur = conn.cursor()
    with cur.copy(f"copy ({sample_values}) to stdout (format {format.name})") as copy:
        rows = list(copy.rows())
    ref = [tuple(py_to_raw(i, format) for i in record) for record in sample_records]
    assert rows == ref


@pytest.mark.parametrize("err", [-1, 1])
@pytest.mark.parametrize("format", Format)
def test_copy_out_badntypes(conn, format, err):
    cur = conn.cursor()
    with cur.copy(f"copy ({sample_values}) to stdout (format {format.name})") as copy:
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
    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


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

    with pytest.raises(e.ProgrammingError):
        with cur.copy("copy (select 1) to stdout; select 1") as copy:
            list(copy)

    with pytest.raises(e.ProgrammingError):
        with cur.copy("select 1; copy (select 1) to stdout"):
            pass


def test_copy_in_str(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy("copy copy_in from stdin (format text)") as copy:
        copy.write(sample_text.decode())

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_copy_in_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled):
        with cur.copy("copy copy_in from stdin (format binary)") as copy:
            copy.write(sample_text.decode())

    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
def test_copy_in_empty(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin (format {format.name})"):
        pass

    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS
    assert cur.rowcount == 0


@pytest.mark.slow
def test_copy_big_size_record(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    data = "".join(chr(randrange(1, 256)) for i in range(10 * 1024 * 1024))
    with cur.copy("copy copy_in (data) from stdin") as copy:
        copy.write_row([data])

    cur.execute("select data from copy_in limit 1")
    assert cur.fetchone()[0] == data


@pytest.mark.slow
@pytest.mark.parametrize("pytype", [str, bytes, bytearray, memoryview])
def test_copy_big_size_block(conn, pytype):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    data = "".join(choice(string.ascii_letters) for i in range(10 * 1024 * 1024))
    copy_data = data + "\n" if pytype is str else pytype(data.encode() + b"\n")
    with cur.copy("copy copy_in (data) from stdin") as copy:
        copy.write(copy_data)

    cur.execute("select data from copy_in limit 1")
    assert cur.fetchone()[0] == data


@pytest.mark.parametrize("format", Format)
def test_subclass_adapter(conn, format):
    if format == Format.TEXT:
        from psycopg.types.string import StrDumper as BaseDumper
    else:
        from psycopg.types.string import (  # type: ignore[no-redef]
            StrBinaryDumper as BaseDumper,
        )

    class MyStrDumper(BaseDumper):
        def dump(self, obj):
            return super().dump(obj) * 2

    conn.adapters.register_dumper(str, MyStrDumper)

    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in (data) from stdin (format {format.name})") as copy:
        copy.write_row(("hello",))

    rec = cur.execute("select data from copy_in").fetchone()
    assert rec[0] == "hellohello"


@pytest.mark.parametrize("format", Format)
def test_copy_in_error_empty(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        with cur.copy(f"copy copy_in from stdin (format {format.name})"):
            raise Exception("mannaggiamiseria")

    assert "mannaggiamiseria" in str(exc.value)
    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


def test_copy_in_buffers_with_pg_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        with cur.copy("copy copy_in from stdin (format text)") as copy:
            copy.write(sample_text)
            copy.write(sample_text)

    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


def test_copy_in_buffers_with_py_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        with cur.copy("copy copy_in from stdin (format text)") as copy:
            copy.write(sample_text)
            raise Exception("nuttengoggenio")

    assert "nuttengoggenio" in str(exc.value)
    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


def test_copy_out_error_with_copy_finished(conn):
    cur = conn.cursor()
    with pytest.raises(ZeroDivisionError):
        with cur.copy("copy (select generate_series(1, 2)) to stdout") as copy:
            copy.read_row()
            1 / 0

    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS


def test_copy_out_error_with_copy_not_finished(conn):
    cur = conn.cursor()
    with pytest.raises(ZeroDivisionError):
        with cur.copy("copy (select generate_series(1, 1000000)) to stdout") as copy:
            copy.read_row()
            1 / 0

    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


def test_copy_out_server_error(conn):
    cur = conn.cursor()
    with pytest.raises(e.DivisionByZero):
        with cur.copy(
            "copy (select 1/n from generate_series(-10, 10) x(n)) to stdout"
        ) as copy:
            for block in copy:
                pass

    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
def test_copy_in_records(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        for row in sample_records:
            if format == Format.BINARY:
                row = tuple(
                    Int4(i) if isinstance(i, int) else i for i in row
                )  # type: ignore[assignment]
            copy.write_row(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", Format)
def test_copy_in_records_set_types(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin (format {format.name})") as copy:
        copy.set_types(["int4", "int4", "text"])
        for row in sample_records:
            copy.write_row(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", Format)
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

    conn.execute("set client_encoding to utf8")
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


def test_copy_in_format(conn):
    file = BytesIO()
    conn.execute("set client_encoding to utf8")
    cur = conn.cursor()
    with Copy(cur, writer=FileWriter(file)) as copy:
        for i in range(1, 256):
            copy.write_row((i, chr(i)))

    file.seek(0)
    rows = file.read().split(b"\n")
    assert not rows[-1]
    del rows[-1]

    for i, row in enumerate(rows, start=1):
        fields = row.split(b"\t")
        assert len(fields) == 2
        assert int(fields[0].decode()) == i
        if i in special_chars:
            assert fields[1].decode() == f"\\{special_chars[i]}"
        else:
            assert fields[1].decode() == chr(i)


@pytest.mark.parametrize(
    "format, buffer", [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")]
)
def test_file_writer(conn, format, buffer):
    file = BytesIO()
    conn.execute("set client_encoding to utf8")
    cur = conn.cursor()
    with Copy(cur, binary=format, writer=FileWriter(file)) as copy:
        for record in sample_records:
            copy.write_row(record)

    file.seek(0)
    want = globals()[buffer]
    got = file.read()
    assert got == want


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
@pytest.mark.parametrize("pytype", [bytes, bytearray, memoryview])
def test_copy_from_to_bytes(conn, pytype):
    # Roundtrip from file to database to file blockwise
    gen = DataGenerator(conn, nrecs=1024, srec=10 * 1024)
    gen.ensure_table()
    cur = conn.cursor()
    with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            copy.write(pytype(block.encode()))

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
        assert cur._query.query == b"copy (select 1) to stdout"
        assert not cur._query.params
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


def test_description(conn):
    with conn.cursor() as cur:
        with cur.copy("copy (select 'This', 'Is', 'Text') to stdout") as copy:
            len(cur.description) == 3
            assert cur.description[0].name == "column_1"
            assert cur.description[2].name == "column_3"
            list(copy.rows())

        len(cur.description) == 3
        assert cur.description[0].name == "column_1"
        assert cur.description[2].name == "column_3"


@pytest.mark.parametrize(
    "format, buffer", [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")]
)
def test_worker_life(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(
        f"copy copy_in from stdin (format {format.name})", writer=QueuedLibpqDriver(cur)
    ) as copy:
        assert not copy.writer._worker
        copy.write(globals()[buffer])
        assert copy.writer._worker

    assert not copy.writer._worker
    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_worker_error_propagated(conn, monkeypatch):
    def copy_to_broken(pgconn, buffer):
        raise ZeroDivisionError
        yield

    monkeypatch.setattr(psycopg.copy, "copy_to", copy_to_broken)
    cur = conn.cursor()
    cur.execute("create temp table wat (a text, b text)")
    with pytest.raises(ZeroDivisionError):
        with cur.copy("copy wat from stdin", writer=QueuedLibpqDriver(cur)) as copy:
            copy.write("a,b")


@pytest.mark.parametrize(
    "format, buffer", [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")]
)
def test_connection_writer(conn, format, buffer):
    cur = conn.cursor()
    writer = LibpqWriter(cur)

    ensure_table(cur, sample_tabledef)
    with cur.copy(
        f"copy copy_in from stdin (format {format.name})", writer=writer
    ) as copy:
        assert copy.writer is writer
        copy.write(globals()[buffer])

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.slow
@pytest.mark.parametrize(
    "fmt, set_types",
    [(Format.TEXT, True), (Format.TEXT, False), (Format.BINARY, True)],
)
@pytest.mark.parametrize("method", ["read", "iter", "row", "rows"])
def test_copy_to_leaks(conn_cls, dsn, faker, fmt, set_types, method):
    faker.format = PyFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    def work():
        with conn_cls.connect(dsn) as conn:
            with conn.cursor(binary=fmt) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)
                with faker.find_insert_problem(conn):
                    cur.executemany(faker.insert_stmt, faker.records)

                stmt = sql.SQL(
                    "copy (select {} from {} order by id) to stdout (format {})"
                ).format(
                    sql.SQL(", ").join(faker.fields_names),
                    faker.table_name,
                    sql.SQL(fmt.name),
                )

                with cur.copy(stmt) as copy:
                    if set_types:
                        copy.set_types(faker.types_names)

                    if method == "read":
                        while True:
                            tmp = copy.read()
                            if not tmp:
                                break
                    elif method == "iter":
                        list(copy)
                    elif method == "row":
                        while True:
                            tmp = copy.read_row()
                            if tmp is None:
                                break
                    elif method == "rows":
                        list(copy.rows())

    gc_collect()
    n = []
    for i in range(3):
        work()
        gc_collect()
        n.append(len(gc.get_objects()))

    assert n[0] == n[1] == n[2], f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


@pytest.mark.slow
@pytest.mark.parametrize(
    "fmt, set_types",
    [(Format.TEXT, True), (Format.TEXT, False), (Format.BINARY, True)],
)
def test_copy_from_leaks(conn_cls, dsn, faker, fmt, set_types):
    faker.format = PyFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    def work():
        with conn_cls.connect(dsn) as conn:
            with conn.cursor(binary=fmt) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)

                stmt = sql.SQL("copy {} ({}) from stdin (format {})").format(
                    faker.table_name,
                    sql.SQL(", ").join(faker.fields_names),
                    sql.SQL(fmt.name),
                )
                with cur.copy(stmt) as copy:
                    if set_types:
                        copy.set_types(faker.types_names)
                    for row in faker.records:
                        copy.write_row(row)

                cur.execute(faker.select_stmt)
                recs = cur.fetchall()

                for got, want in zip(recs, faker.records):
                    faker.assert_record(got, want)

    gc_collect()
    n = []
    for i in range(3):
        work()
        gc_collect()
        n.append(len(gc.get_objects()))

    assert n[0] == n[1] == n[2], f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


@pytest.mark.slow
@pytest.mark.parametrize("mode", ["row", "block", "binary"])
def test_copy_table_across(conn_cls, dsn, faker, mode):
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    with conn_cls.connect(dsn) as conn1, conn_cls.connect(dsn) as conn2:
        faker.table_name = sql.Identifier("copy_src")
        conn1.execute(faker.drop_stmt)
        conn1.execute(faker.create_stmt)
        conn1.cursor().executemany(faker.insert_stmt, faker.records)

        faker.table_name = sql.Identifier("copy_tgt")
        conn2.execute(faker.drop_stmt)
        conn2.execute(faker.create_stmt)

        fmt = "(format binary)" if mode == "binary" else ""
        with conn1.cursor().copy(f"copy copy_src to stdout {fmt}") as copy1:
            with conn2.cursor().copy(f"copy copy_tgt from stdin {fmt}") as copy2:
                if mode == "row":
                    for row in copy1.rows():
                        copy2.write_row(row)
                else:
                    for data in copy1:
                        copy2.write(data)

        recs = conn2.execute(faker.select_stmt).fetchall()
        for got, want in zip(recs, faker.records):
            faker.assert_record(got, want)


def py_to_raw(item, fmt):
    """Convert from Python type to the expected result from the db"""
    if fmt == Format.TEXT:
        if isinstance(item, int):
            return str(item)
    else:
        if isinstance(item, int):
            # Assume int4
            return struct.pack("!i", item)
        elif isinstance(item, str):
            return item.encode()
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
        while True:
            block = f.read()
            if not block:
                break
            if isinstance(block, str):
                block = block.encode()
            m.update(block)
        return m.hexdigest()
