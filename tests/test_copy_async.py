import pytest

from psycopg3.adapt import Format

from .test_copy import sample_text, sample_binary  # noqa
from .test_copy import sample_values, sample_records, sample_tabledef

pytestmark = pytest.mark.asyncio


@pytest.mark.xfail
@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_copy_out_read(aconn, format, buffer):
    cur = aconn.cursor()
    copy = await cur.copy(
        f"copy ({sample_values}) to stdout (format {format.name})"
    )
    assert await copy.read() == globals()[buffer]
    assert await copy.read() is None
    assert await copy.read() is None


@pytest.mark.parametrize(
    "format, buffer",
    [(Format.TEXT, "sample_text"), (Format.BINARY, "sample_binary")],
)
async def test_copy_in_buffers(aconn, format, buffer):
    cur = aconn.cursor()
    await ensure_table(cur, sample_tabledef)
    copy = await cur.copy(f"copy copy_in from stdin (format {format.name})")
    await copy.write(globals()[buffer])
    await copy.finish()
    await cur.execute("select * from copy_in order by 1")
    data = await cur.fetchall()
    assert data == sample_records


async def ensure_table(cur, tabledef, name="copy_in"):
    await cur.execute(f"drop table if exists {name}")
    await cur.execute(f"create table {name} ({tabledef})")
