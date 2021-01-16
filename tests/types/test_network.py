import sys
import ipaddress
import subprocess as sp

import pytest

from psycopg3 import pq
from psycopg3 import sql
from psycopg3.adapt import Format


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("val", ["192.168.0.1", "2001:db8::"])
def test_address_dump(conn, fmt_in, val):
    binary_check(fmt_in)
    cur = conn.cursor()
    cur.execute(
        f"select %{fmt_in} = %s::inet", (ipaddress.ip_address(val), val)
    )
    assert cur.fetchone()[0] is True
    cur.execute(
        f"select %{fmt_in} = array[null, %s]::inet[]",
        ([None, ipaddress.ip_interface(val)], val),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("val", ["127.0.0.1/24", "::ffff:102:300/128"])
def test_interface_dump(conn, fmt_in, val):
    binary_check(fmt_in)
    cur = conn.cursor()
    cur.execute(
        f"select %{fmt_in} = %s::inet", (ipaddress.ip_interface(val), val)
    )
    assert cur.fetchone()[0] is True
    cur.execute(
        f"select %{fmt_in} = array[null, %s]::inet[]",
        ([None, ipaddress.ip_interface(val)], val),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("val", ["127.0.0.0/24", "::ffff:102:300/128"])
def test_network_dump(conn, fmt_in, val):
    binary_check(fmt_in)
    cur = conn.cursor()
    cur.execute(
        f"select %{fmt_in} = %s::cidr", (ipaddress.ip_network(val), val)
    )
    assert cur.fetchone()[0] is True
    cur.execute(
        f"select %{fmt_in} = array[NULL, %s]::cidr[]",
        ([None, ipaddress.ip_network(val)], val),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("val", ["127.0.0.1/32", "::ffff:102:300/128"])
def test_inet_load_address(conn, fmt_out, val):
    binary_check(fmt_out)
    addr = ipaddress.ip_address(val.split("/", 1)[0])
    cur = conn.cursor(binary=fmt_out)

    cur.execute("select %s::inet", (val,))
    assert cur.fetchone()[0] == addr

    cur.execute("select array[null, %s::inet]", (val,))
    assert cur.fetchone()[0] == [None, addr]

    stmt = sql.SQL("copy (select {}::inet) to stdout (format {})").format(
        val, sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types(["inet"])
        (got,) = copy.read_row()

    assert got == addr


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("val", ["127.0.0.1/24", "::ffff:102:300/127"])
def test_inet_load_network(conn, fmt_out, val):
    binary_check(fmt_out)
    pyval = ipaddress.ip_interface(val)
    cur = conn.cursor(binary=fmt_out)

    cur.execute("select %s::inet", (val,))
    assert cur.fetchone()[0] == pyval

    cur.execute("select array[null, %s::inet]", (val,))
    assert cur.fetchone()[0] == [None, pyval]

    stmt = sql.SQL("copy (select {}::inet) to stdout (format {})").format(
        val, sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types(["inet"])
        (got,) = copy.read_row()

    assert got == pyval


@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
@pytest.mark.parametrize("val", ["127.0.0.0/24", "::ffff:102:300/128"])
def test_cidr_load(conn, fmt_out, val):
    binary_check(fmt_out)
    pyval = ipaddress.ip_network(val)
    cur = conn.cursor(binary=fmt_out)

    cur.execute("select %s::cidr", (val,))
    assert cur.fetchone()[0] == pyval

    cur.execute("select array[null, %s::cidr]", (val,))
    assert cur.fetchone()[0] == [None, pyval]

    stmt = sql.SQL("copy (select {}::cidr) to stdout (format {})").format(
        val, sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types(["cidr"])
        (got,) = copy.read_row()

    assert got == pyval


def binary_check(fmt):
    if fmt == Format.BINARY or fmt == pq.Format.BINARY:
        pytest.xfail("inet binary not implemented")


@pytest.mark.subprocess
def test_lazy_load(dsn):
    script = f"""\
import sys
import psycopg3

# In 3.6 it seems already loaded (at least on Travis).
if sys.version_info >= (3, 7):
    assert 'ipaddress' not in sys.modules

conn = psycopg3.connect({dsn!r})
with conn.cursor() as cur:
    cur.execute("select '127.0.0.1'::inet")
    cur.fetchone()

conn.close()
assert 'ipaddress' in sys.modules
"""

    sp.check_call([sys.executable, "-s", "-c", script])
