import pytest

from psycopg3 import pq
from psycopg3 import errors as e
from psycopg3.adapt import Format

from .test_copy import sample_text, sample_binary, sample_binary_rows  # noqa
from .test_copy import eur, sample_values, sample_records, sample_tabledef

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
async def test_copy_out_read(aconn, format):
    cur = await aconn.cursor()
    copy = await cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    )

    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows

    for row in want:
        got = await copy.read()
        assert got == row

    assert await copy.read() is None
    assert await copy.read() is None


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
async def test_copy_out_iter(aconn, format):
    cur = await aconn.cursor()
    copy = await cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    )
    if format == pq.Format.TEXT:
        want = [row + b"\n" for row in sample_text.splitlines()]
    else:
        want = sample_binary_rows
    got = []
    async for row in copy:
        got.append(row)
    assert got == want


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_copy_in_buffers(aconn, format, buffer):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    copy = await cur.copy(f"copy copy_in from stdin (format {format.name})")
    await copy.write(globals()[buffer])
    await copy.finish()
    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def test_copy_in_buffers_pg_error(aconn):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    copy = await cur.copy("copy copy_in from stdin (format text)")
    await copy.write(sample_text)
    await copy.write(sample_text)
    with pytest.raises(e.UniqueViolation):
        await copy.finish()
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_bad_result(aconn):
    await aconn.set_autocommit(True)

    cur = await aconn.cursor()

    with pytest.raises(e.SyntaxError):
        await cur.copy("wat")

    with pytest.raises(e.ProgrammingError):
        await cur.copy("select 1")

    with pytest.raises(e.ProgrammingError):
        await cur.copy("reset timezone")


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_copy_in_buffers_with(aconn, format, buffer):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with (
        await cur.copy(f"copy copy_in from stdin (format {format.name})")
    ) as copy:
        await copy.write(globals()[buffer])

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def test_copy_in_str(aconn):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    async with (
        await cur.copy("copy copy_in from stdin (format text)")
    ) as copy:
        await copy.write(sample_text.decode("utf8"))

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def test_copy_in_str_binary(aconn):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled):
        async with (
            await cur.copy("copy copy_in from stdin (format binary)")
        ) as copy:
            await copy.write(sample_text.decode("utf8"))

    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_in_buffers_with_pg_error(aconn):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.UniqueViolation):
        async with (
            await cur.copy("copy copy_in from stdin (format text)")
        ) as copy:
            await copy.write(sample_text)
            await copy.write(sample_text)

    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


async def test_copy_in_buffers_with_py_error(aconn):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    with pytest.raises(e.QueryCanceled) as exc:
        async with (
            await cur.copy("copy copy_in from stdin (format text)")
        ) as copy:
            await copy.write(sample_text)
            raise Exception("nuttengoggenio")

    assert "nuttengoggenio" in str(exc.value)
    assert aconn.pgconn.transaction_status == aconn.TransactionStatus.INERROR


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
async def test_copy_in_records(aconn, format):
    if format == Format.BINARY:
        pytest.skip("TODO: implement int binary adapter")

    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    async with (
        await cur.copy(f"copy copy_in from stdin (format {format.name})")
    ) as copy:
        for row in sample_records:
            await copy.write_row(row)

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


@pytest.mark.parametrize("format", [Format.TEXT, Format.BINARY])
async def test_copy_in_records_binary(aconn, format):
    if format == Format.TEXT:
        pytest.skip("TODO: remove after implementing int binary adapter")

    cur = await aconn.cursor()
    await ensure_table(cur, "col1 serial primary key, col2 int, data text")

    async with (
        await cur.copy(
            f"copy copy_in (col2, data) from stdin (format {format.name})"
        )
    ) as copy:
        for row in sample_records:
            await copy.write_row((None, row[2]))

    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == [(1, None, "hello"), (2, None, "world")]


async def test_copy_in_allchars(aconn):
    cur = await aconn.cursor()
    await ensure_table(cur, sample_tabledef)

    await aconn.set_client_encoding("utf8")
    async with (
        await cur.copy("copy copy_in from stdin (format text)")
    ) as copy:
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


async def ensure_table(cur, tabledef, name="copy_in"):
    await cur.execute(f"drop table if exists {name}")
    await cur.execute(f"create table {name} ({tabledef})")
