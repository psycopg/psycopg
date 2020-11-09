import os
import sys
from uuid import UUID
import subprocess as sp

import pytest

from psycopg3.adapt import Format


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_uuid_dump(conn, fmt_in):
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    val = "12345678123456781234567812345679"
    cur = conn.cursor()
    cur.execute(f"select {ph} = %s::uuid", (UUID(val), val))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_uuid_load(conn, fmt_out):
    cur = conn.cursor(format=fmt_out)
    val = "12345678123456781234567812345679"
    cur.execute("select %s::uuid", (val,))
    assert cur.fetchone()[0] == UUID(val)


def test_lazy_load(dsn):
    script = f"""\
import sys
import psycopg3

assert 'uuid' not in sys.modules

conn = psycopg3.connect({dsn!r})
with conn.cursor() as cur:
    cur.execute("select repeat('1', 32)::uuid")
    cur.fetchone()

conn.close()
assert 'uuid' in sys.modules
"""

    # TODO: debug this. Importing c module fails on travis in this scenario
    env = dict(os.environ)
    env.pop("PSYCOPG3_IMPL", None)

    sp.check_call([sys.executable, "-c", script], env=env)
