import gc
import string
import hashlib
from io import BytesIO, StringIO
from itertools import cycle

import pytest

import psycopg
from psycopg import pq
from psycopg import sql
from psycopg import errors as e
from psycopg.pq import Format
from psycopg.types import TypeInfo
from psycopg.adapt import PyFormat
from psycopg.types.hstore import register_hstore
from psycopg.types.numeric import Int4

from .utils import gc_collect
from .test_copy import sample_text, sample_binary, sample_binary_rows  # noqa
from .test_copy import eur, sample_values, sample_records, sample_tabledef
from .test_copy import py_to_raw

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("format", Format)
async def test_copy_out_read(aconn, format):
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    cur = aconn.cursor()
    async with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        for row in want:
            got = await copy.read()
            assert got == row
            assert (
                aconn.pgconn.transaction_status
                == aconn.TransactionStatus.ACTIVE
            )

        assert await copy.read() == b""
        assert await copy.read() == b""

    assert await copy.read() == b""
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", Format)
async def test_copy_out_iter(aconn, format):
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    cur = aconn.cursor()
    async with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        assert await alist(copy) == want

    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", Format)
@pytest.mark.parametrize("typetype", ["names", "oids"])
async def test_read_rows(aconn, format, typetype):
    cur = aconn.cursor()
    async with cur.copy(
        f"""copy (
            select 10::int4, 'hello'::text, '{{0.0,1.0}}'::float8[]
        ) to stdout (format {format.name})"""
    ) as copy:
        copy.set_types(["int4", "text", "float8[]"])
        row = await copy.read_row()
        assert (await copy.read_row()) is None

    assert row == (10, "hello", [0.0, 1.0])
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


@pytest.mark.parametrize("format", Format)
async def test_rows(aconn, format):
    cur = aconn.cursor()
    async with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        copy.set_types("int4 int4 text".split())
        rows = await alist(copy.rows())

    assert rows == sample_records
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS


async def test_set_custom_type(aconn, hstore):
    command = """copy (select '"a"=>"1", "b"=>"2"'::hstore) to stdout"""
    cur = aconn.cursor()

    async with cur.copy(command) as copy:
        rows = await alist(copy.rows())

    assert rows == [('"a"=>"1", "b"=>"2"',)]

    register_hstore(await TypeInfo.fetch(aconn, "hstore"), cur)
    async with cur.copy(command) as copy:
        copy.set_types(["hstore"])
        rows = await alist(copy.rows())

    assert rows == [({"a": "1", "b": "2"},)]


@pytest.mark.parametrize("format", Format)
async def test_copy_out_allchars(aconn, format):
    cur = aconn.cursor()
    chars = list(map(chr, range(1, 256))) + [eur]
    await aconn.execute("set client_encoding to utf8")
    rows = []
    query = sql.SQL(
        "copy (select unnest({}::text[])) to stdout (format {})"
    ).format(chars, sql.SQL(format.name))
    async with cur.copy(query) as copy:
        copy.set_types(["text"])
        while 1:
            row = await copy.read_row()
            if not row:
                break
            assert len(row) == 1
            rows.append(row[0])

    assert rows == chars


@pytest.mark.parametrize("format", Format)
async def test_read_row_notypes(aconn, format):
    cur = aconn.cursor()
    async with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        rows = []
        while 1:
            row = await copy.read_row()
            if not row:
                break
            rows.append(row)

    ref = [
        tuple(py_to_raw(i, format) for i in record)
        for record in sample_records
    ]
    assert rows == ref


@pytest.mark.parametrize("format", Format)
async def test_rows_notypes(aconn, format):
    cur = aconn.cursor()
    async with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        rows = await alist(copy.rows())
    ref = [
        tuple(py_to_raw(i, format) for i in record)
        for record in sample_records
    ]
    assert rows == ref


@pytest.mark.parametrize("err", [-1, 1])
@pytest.mark.parametrize("format", Format)
async def test_copy_out_badntypes(aconn, format, err):
    cur = aconn.cursor()
    async with cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    ) as copy:
        copy.set_types([0] * (len(sample_records[0]) + err))
        with pytest.raises(e.ProgrammingError):
            await copy.read_row()


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_copy_in_buffers(aconn, format, buffer):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy(
        f"copy copy_in from stdin (format {format.name})"
    ) as copy:
        await copy.write(globals()[buffer])

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def test_copy_in_buffers_pg_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        async with cur.copy("copy copy_in from stdin (format text)") as copy:
            await copy.write(sample_text)
            await copy.write(sample_text)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_bad_result(aconn):
    await aconn.set_autocommit(True)

    cur = aconn.cursor()

    with pytest.raises(e.SyntaxError):
        async with cur.copy("wat"):
            pass

    with pytest.raises(e.ProgrammingError):
        async with cur.copy("select 1"):
            pass

    with pytest.raises(e.ProgrammingError):
        async with cur.copy("reset timezone"):
            pass

    with pytest.raises(e.ProgrammingError):
        async with cur.copy("copy (select 1) to stdout; select 1") as copy:
            await alist(copy)

    with pytest.raises(e.ProgrammingError):
        async with cur.copy("select 1; copy (select 1) to stdout"):
            pass


async def test_copy_in_str(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy("copy copy_in from stdin (format text)") as copy:
        await copy.write(sample_text.decode())

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def test_copy_in_str_binary(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled):
        async with cur.copy("copy copy_in from stdin (format binary)") as copy:
            await copy.write(sample_text.decode())

    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
async def test_copy_in_empty(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy(f"copy copy_in from stdin (format {format.name})"):
        pass

    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INTRANS
    assert cur.rowcount == 0


@pytest.mark.parametrize("format", Format)
async def test_subclass_adapter(aconn, format):
    if format == Format.TEXT:
        from psycopg.types.string import StrDumper as BaseDumper
    else:
        from psycopg.types.string import (  # type: ignore[no-redef]
            StrBinaryDumper as BaseDumper,
        )

    class MyStrDumper(BaseDumper):
        def dump(self, obj):
            return super().dump(obj) * 2

    aconn.adapters.register_dumper(str, MyStrDumper)

    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    async with cur.copy(
        f"copy copy_in (data) from stdin (format {format.name})"
    ) as copy:
        await copy.write_row(("hello",))

    await cur.execute("select data from copy_in")
    rec = await cur.fetchone()
    assert rec[0] == "hellohello"


@pytest.mark.parametrize("format", Format)
async def test_copy_in_error_empty(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        async with cur.copy(f"copy copy_in from stdin (format {format.name})"):
            raise Exception("mannaggiamiseria")

    assert "mannaggiamiseria" in str(exc.value)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_in_buffers_with_pg_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        async with cur.copy("copy copy_in from stdin (format text)") as copy:
            await copy.write(sample_text)
            await copy.write(sample_text)

    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_in_buffers_with_py_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        async with cur.copy("copy copy_in from stdin (format text)") as copy:
            await copy.write(sample_text)
            raise Exception("nuttengoggenio")

    assert "nuttengoggenio" in str(exc.value)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
async def test_copy_in_records(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    async with cur.copy(
        f"copy copy_in from stdin (format {format.name})"
    ) as copy:
        for row in sample_records:
            if format == Format.BINARY:
                row = tuple(
                    Int4(i) if isinstance(i, int) else i for i in row
                )  # type: ignore[assignment]
            await copy.write_row(row)

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", Format)
async def test_copy_in_records_set_types(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    async with cur.copy(
        f"copy copy_in from stdin (format {format.name})"
    ) as copy:
        copy.set_types(["int4", "int4", "text"])
        for row in sample_records:
            await copy.write_row(row)

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", Format)
async def test_copy_in_records_binary(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, "col1 serial primary key, col2 int, data text")

    async with cur.copy(
        f"copy copy_in (col2, data) from stdin (format {format.name})"
    ) as copy:
        for row in sample_records:
            await copy.write_row((None, row[2]))

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == [(1, None, "hello"), (2, None, "world")]


async def test_copy_in_allchars(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    await aconn.execute("set client_encoding to utf8")
    async with cur.copy("copy copy_in from stdin (format text)") as copy:
        for i in range(1, 256):
            await copy.write_row((i, None, chr(i)))
        await copy.write_row((ord(eur), None, eur))

    await cur.execute(
        """
select col1 = ascii(data), col2 is null, length(data), count(*)
from copy_in group by 1, 2, 3
"""
    )
    data = await cur.fetchall()
    assert data == [(True, True, 1, 256)]


@pytest.mark.slow
async def test_copy_from_to(aconn):
    # Roundtrip from file to database to file blockwise
    gen = DataGenerator(aconn, nrecs=1024, srec=10 * 1024)
    await gen.ensure_table()
    cur = aconn.cursor()
    async with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            await copy.write(block)

    await gen.assert_data()

    f = BytesIO()
    async with cur.copy("copy copy_in to stdout") as copy:
        async for block in copy:
            f.write(block)

    f.seek(0)
    assert gen.sha(f) == gen.sha(gen.file())


@pytest.mark.slow
async def test_copy_from_to_bytes(aconn):
    # Roundtrip from file to database to file blockwise
    gen = DataGenerator(aconn, nrecs=1024, srec=10 * 1024)
    await gen.ensure_table()
    cur = aconn.cursor()
    async with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            await copy.write(block.encode())

    await gen.assert_data()

    f = BytesIO()
    async with cur.copy("copy copy_in to stdout") as copy:
        async for block in copy:
            f.write(block)

    f.seek(0)
    assert gen.sha(f) == gen.sha(gen.file())


@pytest.mark.slow
async def test_copy_from_insane_size(aconn):
    # Trying to trigger a "would block" error
    gen = DataGenerator(
        aconn, nrecs=4 * 1024, srec=10 * 1024, block_size=20 * 1024 * 1024
    )
    await gen.ensure_table()
    cur = aconn.cursor()
    async with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            await copy.write(block)

    await gen.assert_data()


async def test_copy_rowcount(aconn):
    gen = DataGenerator(aconn, nrecs=3, srec=10)
    await gen.ensure_table()

    cur = aconn.cursor()
    async with cur.copy("copy copy_in from stdin") as copy:
        for block in gen.blocks():
            await copy.write(block)
    assert cur.rowcount == 3

    gen = DataGenerator(aconn, nrecs=2, srec=10, offset=3)
    async with cur.copy("copy copy_in from stdin") as copy:
        for rec in gen.records():
            await copy.write_row(rec)
    assert cur.rowcount == 2

    async with cur.copy("copy copy_in to stdout") as copy:
        async for block in copy:
            pass
    assert cur.rowcount == 5

    with pytest.raises(e.BadCopyFileFormat):
        async with cur.copy("copy copy_in (id) from stdin") as copy:
            for rec in gen.records():
                await copy.write_row(rec)
    assert cur.rowcount == -1


async def test_copy_query(aconn):
    cur = aconn.cursor()
    async with cur.copy("copy (select 1) to stdout") as copy:
        assert cur._query.query == b"copy (select 1) to stdout"
        assert not cur._query.params
        await alist(copy)


async def test_cant_reenter(aconn):
    cur = aconn.cursor()
    async with cur.copy("copy (select 1) to stdout") as copy:
        await alist(copy)

    with pytest.raises(TypeError):
        async with copy:
            await alist(copy)


async def test_str(aconn):
    cur = aconn.cursor()
    async with cur.copy("copy (select 1) to stdout") as copy:
        assert "[ACTIVE]" in str(copy)
        await alist(copy)

    assert "[INTRANS]" in str(copy)


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_worker_life(aconn, format, buffer):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy(
        f"copy copy_in from stdin (format {format.name})"
    ) as copy:
        assert not copy._worker
        await copy.write(globals()[buffer])
        assert copy._worker

    assert not copy._worker
    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


@pytest.mark.slow
@pytest.mark.parametrize(
    "fmt, set_types",
    [(Format.TEXT, True), (Format.TEXT, False), (Format.BINARY, True)],
)
@pytest.mark.parametrize("method", ["read", "iter", "row", "rows"])
async def test_copy_to_leaks(dsn, faker, fmt, set_types, method, retries):
    faker.format = PyFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    async def work():
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            async with conn.cursor(binary=fmt) as cur:
                await cur.execute(faker.drop_stmt)
                await cur.execute(faker.create_stmt)
                async with faker.find_insert_problem_async(conn):
                    await cur.executemany(faker.insert_stmt, faker.records)

                stmt = sql.SQL(
                    "copy (select {} from {} order by id) to stdout (format {})"
                ).format(
                    sql.SQL(", ").join(faker.fields_names),
                    faker.table_name,
                    sql.SQL(fmt.name),
                )

                async with cur.copy(stmt) as copy:
                    if set_types:
                        copy.set_types(faker.types_names)

                    if method == "read":
                        while 1:
                            tmp = await copy.read()
                            if not tmp:
                                break
                    elif method == "iter":
                        await alist(copy)
                    elif method == "row":
                        while 1:
                            tmp = await copy.read_row()  # type: ignore[assignment]
                            if tmp is None:
                                break
                    elif method == "rows":
                        await alist(copy.rows())

    gc_collect()
    async for retry in retries:
        with retry:
            n = []
            for i in range(3):
                await work()
                gc_collect()
                n.append(len(gc.get_objects()))

            assert (
                n[0] == n[1] == n[2]
            ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


@pytest.mark.slow
@pytest.mark.parametrize(
    "fmt, set_types",
    [(Format.TEXT, True), (Format.TEXT, False), (Format.BINARY, True)],
)
async def test_copy_from_leaks(dsn, faker, fmt, set_types, retries):
    faker.format = PyFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    async def work():
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            async with conn.cursor(binary=fmt) as cur:
                await cur.execute(faker.drop_stmt)
                await cur.execute(faker.create_stmt)

                stmt = sql.SQL("copy {} ({}) from stdin (format {})").format(
                    faker.table_name,
                    sql.SQL(", ").join(faker.fields_names),
                    sql.SQL(fmt.name),
                )
                async with cur.copy(stmt) as copy:
                    if set_types:
                        copy.set_types(faker.types_names)
                    for row in faker.records:
                        await copy.write_row(row)

                await cur.execute(faker.select_stmt)
                recs = await cur.fetchall()

                for got, want in zip(recs, faker.records):
                    faker.assert_record(got, want)

    gc_collect()
    async for retry in retries:
        with retry:
            n = []
            for i in range(3):
                await work()
                gc_collect()
                n.append(len(gc.get_objects()))

            assert (
                n[0] == n[1] == n[2]
            ), f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"


async def ensure_table(cur, tabledef, name="copy_in"):
    await cur.execute(f"drop table if exists {name}")
    await cur.execute(f"create table {name} ({tabledef})")


class DataGenerator:
    def __init__(self, conn, nrecs, srec, offset=0, block_size=8192):
        self.conn = conn
        self.nrecs = nrecs
        self.srec = srec
        self.offset = offset
        self.block_size = block_size

    async def ensure_table(self):
        cur = self.conn.cursor()
        await ensure_table(cur, "id integer primary key, data text")

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

    async def assert_data(self):
        cur = self.conn.cursor()
        await cur.execute("select id, data from copy_in order by id")
        for record in self.records():
            assert record == await cur.fetchone()

        assert await cur.fetchone() is None

    def sha(self, f):
        m = hashlib.sha256()
        while 1:
            block = f.read()
            if not block:
                break
            if isinstance(block, str):
                block = block.encode()
            m.update(block)
        return m.hexdigest()


async def alist(it):
    return [i async for i in it]
