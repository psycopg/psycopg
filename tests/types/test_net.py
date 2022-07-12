import ipaddress

import pytest

from psycopg import pq
from psycopg import sql
from psycopg.adapt import PyFormat

crdb_skip_cidr = pytest.mark.crdb_skip("cidr")


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("val", ["192.168.0.1", "2001:db8::"])
def test_address_dump(conn, fmt_in, val):
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value} = %s::inet", (ipaddress.ip_address(val), val))
    assert cur.fetchone()[0] is True
    cur.execute(
        f"select %{fmt_in.value} = array[null, %s]::inet[]",
        ([None, ipaddress.ip_interface(val)], val),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("val", ["127.0.0.1/24", "::ffff:102:300/128"])
def test_interface_dump(conn, fmt_in, val):
    cur = conn.cursor()
    rec = cur.execute(
        f"select %(val){fmt_in.value} = %(repr)s::inet,"
        f" %(val){fmt_in.value}, %(repr)s::inet",
        {"val": ipaddress.ip_interface(val), "repr": val},
    ).fetchone()
    assert rec[0] is True, f"{rec[1]} != {rec[2]}"
    cur.execute(
        f"select %{fmt_in.value} = array[null, %s]::inet[]",
        ([None, ipaddress.ip_interface(val)], val),
    )
    assert cur.fetchone()[0] is True


@crdb_skip_cidr
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("val", ["127.0.0.0/24", "::ffff:102:300/128"])
def test_network_dump(conn, fmt_in, val):
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value} = %s::cidr", (ipaddress.ip_network(val), val))
    assert cur.fetchone()[0] is True
    cur.execute(
        f"select %{fmt_in.value} = array[NULL, %s]::cidr[]",
        ([None, ipaddress.ip_network(val)], val),
    )
    assert cur.fetchone()[0] is True


@crdb_skip_cidr
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_network_mixed_size_array(conn, fmt_in):
    val = [
        ipaddress.IPv4Network("192.168.0.1/32"),
        ipaddress.IPv6Network("::1/128"),
    ]
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value}", (val,))
    got = cur.fetchone()[0]
    assert val == got


@pytest.mark.crdb_skip("copy")
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["127.0.0.1/32", "::ffff:102:300/128"])
def test_inet_load_address(conn, fmt_out, val):
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


@pytest.mark.crdb_skip("copy")
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["127.0.0.1/24", "::ffff:102:300/127"])
def test_inet_load_network(conn, fmt_out, val):
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


@crdb_skip_cidr
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("val", ["127.0.0.0/24", "::ffff:102:300/128"])
def test_cidr_load(conn, fmt_out, val):
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
