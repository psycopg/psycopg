import struct

from psycopg.pq import Format
from psycopg.copy import AsyncWriter
from psycopg.copy import FileWriter as FileWriter  # noqa: F401

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


def ensure_table(cur, tabledef, name="copy_in"):
    cur.execute(f"drop table if exists {name}")
    cur.execute(f"create table {name} ({tabledef})")


async def ensure_table_async(cur, tabledef, name="copy_in"):
    await cur.execute(f"drop table if exists {name}")
    await cur.execute(f"create table {name} ({tabledef})")


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


class AsyncFileWriter(AsyncWriter):
    def __init__(self, file):
        self.file = file

    async def write(self, data):
        self.file.write(data)
