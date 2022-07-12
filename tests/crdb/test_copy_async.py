import gc
import pytest
import string
from random import randrange, choice

from psycopg.pq import Format
from psycopg import sql, errors as e
from psycopg.adapt import PyFormat
from psycopg.types.numeric import Int4

from ..utils import eur, gc_collect
from ..test_copy import sample_text, sample_binary  # noqa
from ..test_copy import sample_records
from ..test_copy_async import ensure_table
from .test_copy import sample_tabledef, copyopt

pytestmark = [pytest.mark.crdb, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_copy_in_buffers(aconn, format, buffer):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy(f"copy copy_in from stdin {copyopt(format)}") as copy:
        await copy.write(globals()[buffer])

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def test_copy_in_buffers_pg_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        async with cur.copy("copy copy_in from stdin") as copy:
            await copy.write(sample_text)
            await copy.write(sample_text)
    assert aconn.info.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_in_str(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy("copy copy_in from stdin") as copy:
        await copy.write(sample_text.decode())

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


@pytest.mark.xfail(reason="bad sqlstate - CRDB #81559")
async def test_copy_in_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled):
        async with cur.copy("copy copy_in from stdin with binary") as copy:
            await copy.write(sample_text.decode())

    assert aconn.info.transaction_status == aconn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
async def test_copy_in_empty(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with cur.copy(f"copy copy_in from stdin {copyopt(format)}"):
        pass

    assert aconn.info.transaction_status == aconn.TransactionStatus.INTRANS
    assert cur.rowcount == 0


@pytest.mark.slow
async def test_copy_big_size_record(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, "id serial primary key, data text")
    data = "".join(chr(randrange(1, 256)) for i in range(10 * 1024 * 1024))
    async with cur.copy("copy copy_in (data) from stdin") as copy:
        await copy.write_row([data])

    await cur.execute("select data from copy_in limit 1")
    assert (await cur.fetchone())[0] == data


@pytest.mark.slow
async def test_copy_big_size_block(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, "id serial primary key, data text")
    data = "".join(choice(string.ascii_letters) for i in range(10 * 1024 * 1024))
    copy_data = data + "\n"
    async with cur.copy("copy copy_in (data) from stdin") as copy:
        await copy.write(copy_data)

    await cur.execute("select data from copy_in limit 1")
    assert (await cur.fetchone())[0] == data


async def test_copy_in_buffers_with_pg_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        async with cur.copy("copy copy_in from stdin") as copy:
            await copy.write(sample_text)
            await copy.write(sample_text)

    assert aconn.info.transaction_status == aconn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", Format)
async def test_copy_in_records(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    async with cur.copy(f"copy copy_in from stdin {copyopt(format)}") as copy:
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

    async with cur.copy(f"copy copy_in from stdin {copyopt(format)}") as copy:
        copy.set_types(["int4", "int4", "text"])
        for row in sample_records:
            await copy.write_row(row)

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", Format)
async def test_copy_in_records_binary(aconn, format):
    cur = aconn.cursor()
    await ensure_table(cur, "col1 serial primary key, col2 int4, data text")

    async with cur.copy(
        f"copy copy_in (col2, data) from stdin {copyopt(format)}"
    ) as copy:
        for row in sample_records:
            await copy.write_row((None, row[2]))

    await cur.execute("select col2, data from copy_in order by 2")
    data = await cur.fetchall()
    assert data == [(None, "hello"), (None, "world")]


@pytest.mark.crdb_skip("copy canceled")
async def test_copy_in_buffers_with_py_error(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        async with cur.copy("copy copy_in from stdin") as copy:
            await copy.write(sample_text)
            raise Exception("nuttengoggenio")

    assert "nuttengoggenio" in str(exc.value)
    assert aconn.info.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_in_allchars(aconn):
    cur = aconn.cursor()
    await ensure_table(cur, "col1 int primary key, col2 int, data text")

    async with cur.copy("copy copy_in from stdin") as copy:
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
@pytest.mark.parametrize(
    "fmt, set_types",
    [(Format.TEXT, True), (Format.TEXT, False), (Format.BINARY, True)],
)
@pytest.mark.crdb_skip("copy array")
async def test_copy_from_leaks(aconn_cls, dsn, faker, fmt, set_types):
    faker.format = PyFormat.from_pq(fmt)
    faker.choose_schema(ncols=20)
    faker.make_records(20)

    async def work():
        async with await aconn_cls.connect(dsn) as conn:
            async with conn.cursor(binary=fmt) as cur:
                await cur.execute(faker.drop_stmt)
                await cur.execute(faker.create_stmt)

                stmt = sql.SQL("copy {} ({}) from stdin {}").format(
                    faker.table_name,
                    sql.SQL(", ").join(faker.fields_names),
                    sql.SQL("with binary" if fmt else ""),
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
    n = []
    for i in range(3):
        await work()
        gc_collect()
        n.append(len(gc.get_objects()))

    assert n[0] == n[1] == n[2], f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"
