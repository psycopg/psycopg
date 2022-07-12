import gc
import pytest
import string
from random import randrange, choice

from psycopg import sql, errors as e
from psycopg.pq import Format
from psycopg.adapt import PyFormat
from psycopg.types.numeric import Int4

from ..utils import eur, gc_collect
from ..test_copy import sample_text, sample_binary  # noqa
from ..test_copy import ensure_table, sample_records
from ..test_copy import sample_tabledef as sample_tabledef_pg

# CRDB int/serial are int8
sample_tabledef = sample_tabledef_pg.replace("int", "int4").replace("serial", "int4")

pytestmark = pytest.mark.crdb


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
def test_copy_in_buffers(conn, format, buffer):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin {copyopt(format)}") as copy:
        copy.write(globals()[buffer])

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


def test_copy_in_buffers_pg_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        with cur.copy("copy copy_in from stdin") as copy:
            copy.write(sample_text)
            copy.write(sample_text)
    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


def test_copy_in_str(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy("copy copy_in from stdin") as copy:
        copy.write(sample_text.decode())

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.xfail(reason="bad sqlstate - CRDB #81559")
def test_copy_in_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled):
        with cur.copy("copy copy_in from stdin with binary") as copy:
            copy.write(sample_text.decode())

    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
def test_copy_in_empty(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with cur.copy(f"copy copy_in from stdin {copyopt(format)}"):
        pass

    assert conn.info.transaction_status == conn.TransactionStatus.INTRANS
    assert cur.rowcount == 0


@pytest.mark.slow
def test_copy_big_size_record(conn):
    cur = conn.cursor()
    ensure_table(cur, "id serial primary key, data text")
    data = "".join(chr(randrange(1, 256)) for i in range(10 * 1024 * 1024))
    with cur.copy("copy copy_in (data) from stdin") as copy:
        copy.write_row([data])

    cur.execute("select data from copy_in limit 1")
    assert cur.fetchone()[0] == data


@pytest.mark.slow
def test_copy_big_size_block(conn):
    cur = conn.cursor()
    ensure_table(cur, "id serial primary key, data text")
    data = "".join(choice(string.ascii_letters) for i in range(10 * 1024 * 1024))
    copy_data = data + "\n"
    with cur.copy("copy copy_in (data) from stdin") as copy:
        copy.write(copy_data)

    cur.execute("select data from copy_in limit 1")
    assert cur.fetchone()[0] == data


def test_copy_in_buffers_with_pg_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        with cur.copy("copy copy_in from stdin") as copy:
            copy.write(sample_text)
            copy.write(sample_text)

    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
def test_copy_in_records(conn, format):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)

    with cur.copy(f"copy copy_in from stdin {copyopt(format)}") as copy:
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

    with cur.copy(f"copy copy_in from stdin {copyopt(format)}") as copy:
        copy.set_types(["int4", "int4", "text"])
        for row in sample_records:
            copy.write_row(row)

    data = cur.execute("select * from copy_in order by 1").fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", Format)
def test_copy_in_records_binary(conn, format):
    cur = conn.cursor()
    ensure_table(cur, "col1 serial primary key, col2 int4, data text")

    with cur.copy(f"copy copy_in (col2, data) from stdin {copyopt(format)}") as copy:
        for row in sample_records:
            copy.write_row((None, row[2]))

    data = cur.execute("select col2, data from copy_in order by 2").fetchall()
    assert data == [(None, "hello"), (None, "world")]


@pytest.mark.crdb_skip("copy canceled")
def test_copy_in_buffers_with_py_error(conn):
    cur = conn.cursor()
    ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        with cur.copy("copy copy_in from stdin") as copy:
            copy.write(sample_text)
            raise Exception("nuttengoggenio")

    assert "nuttengoggenio" in str(exc.value)
    assert conn.info.transaction_status == conn.TransactionStatus.INERROR


def test_copy_in_allchars(conn):
    cur = conn.cursor()
    ensure_table(cur, "col1 int primary key, col2 int, data text")

    with cur.copy("copy copy_in from stdin") as copy:
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
@pytest.mark.parametrize(
    "fmt, set_types",
    [(Format.TEXT, True), (Format.TEXT, False), (Format.BINARY, True)],
)
@pytest.mark.crdb_skip("copy array")
def test_copy_from_leaks(conn_cls, dsn, faker, fmt, set_types):
    faker.format = PyFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    def work():
        with conn_cls.connect(dsn) as conn:
            with conn.cursor(binary=fmt) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)

                stmt = sql.SQL("copy {} ({}) from stdin {}").format(
                    faker.table_name,
                    sql.SQL(", ").join(faker.fields_names),
                    sql.SQL("with binary" if fmt else ""),
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


def copyopt(format):
    return "with binary" if format == Format.BINARY else ""
