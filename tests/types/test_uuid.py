import sys
from uuid import UUID
import subprocess as sp

import pytest

from psycopg import pq
from psycopg import sql
from psycopg.adapt import PyFormat


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_uuid_dump(conn, fmt_in):
    val = "12345678123456781234567812345679"
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value} = %s::uuid", (UUID(val), val))
    assert cur.fetchone()[0] is True


@pytest.mark.crdb_skip("copy")
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_uuid_load(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    val = "12345678123456781234567812345679"
    cur.execute("select %s::uuid", (val,))
    assert cur.fetchone()[0] == UUID(val)

    stmt = sql.SQL("copy (select {}::uuid) to stdout (format {})").format(
        val, sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types(["uuid"])
        (res,) = copy.read_row()

    assert res == UUID(val)


@pytest.mark.slow
@pytest.mark.subprocess
def test_lazy_load(dsn):
    script = f"""\
import sys
import psycopg

assert 'uuid' not in sys.modules

conn = psycopg.connect({dsn!r})
with conn.cursor() as cur:
    cur.execute("select repeat('1', 32)::uuid")
    cur.fetchone()

conn.close()
assert 'uuid' in sys.modules
"""

    sp.check_call([sys.executable, "-c", script])
