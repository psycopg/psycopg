import pytest

import psycopg
from psycopg.pq import Format
from psycopg.types import TypeInfo
from psycopg.types.hstore import HstoreBinaryLoader, HstoreLoader
from psycopg.types.hstore import _make_hstore_binary_dumper, register_hstore

pytestmark = pytest.mark.crdb_skip("hstore")


@pytest.mark.parametrize(
    "s, d",
    [
        ("", {}),
        ('"a"=>"1", "b"=>"2"', {"a": "1", "b": "2"}),
        ('"a"  => "1" , "b"  =>  "2"', {"a": "1", "b": "2"}),
        ('"a"=>NULL, "b"=>"2"', {"a": None, "b": "2"}),
        (r'"a"=>"\"", "\""=>"2"', {"a": '"', '"': "2"}),
        ('"a"=>"\'", "\'"=>"2"', {"a": "'", "'": "2"}),
        ('"a"=>"1", "b"=>NULL', {"a": "1", "b": None}),
        (r'"a\\"=>"1"', {"a\\": "1"}),
        (r'"a\""=>"1"', {'a"': "1"}),
        (r'"a\\\""=>"1"', {r"a\"": "1"}),
        (r'"a\\\\\""=>"1"', {r'a\\"': "1"}),
        ('"\xe8"=>"\xe0"', {"\xe8": "\xe0"}),
    ],
)
def test_parse_ok(s, d):
    loader = HstoreLoader(0, None)
    assert loader.load(s.encode()) == d


@pytest.mark.parametrize(
    "d, b",
    [
        ({}, b"\x00\x00\x00\x00"),
        (
            {"a": "1", "b": "2"},
            b"\x00\x00\x00\x02"
            b"\x00\x00\x00\x01a\x00\x00\x00\x011"
            b"\x00\x00\x00\x01b\x00\x00\x00\x012",
        ),
        (
            {"a": None, "b": "2"},
            b"\x00\x00\x00\x02"
            b"\x00\x00\x00\x01a\xff\xff\xff\xff"
            b"\x00\x00\x00\x01b\x00\x00\x00\x012",
        ),
        (
            {"\xe8": "\xe0"},
            b"\x00\x00\x00\x01\x00\x00\x00\x02\xc3\xa8\x00\x00\x00\x02\xc3\xa0",
        ),
        (
            {"a": None, "b": "1" * 300},
            b"\x00\x00\x00\x02"
            b"\x00\x00\x00\x01a\xff\xff\xff\xff"
            b"\x00\x00\x00\x01b\x00\x00\x01," + b"1" * 300,
        ),
    ],
)
def test_binary(d, b):
    dumper = _make_hstore_binary_dumper(0)(dict)
    assert dumper.dump(d) == b
    loader = HstoreBinaryLoader(0)
    assert loader.load(b) == d


@pytest.mark.parametrize(
    "s",
    [
        "a",
        '"a"',
        r'"a\\""=>"1"',
        r'"a\\\\""=>"1"',
        '"a=>"1"',
        '"a"=>"1", "b"=>NUL',
    ],
)
def test_parse_bad(s):
    with pytest.raises(psycopg.DataError):
        loader = HstoreLoader(0, None)
        loader.load(s.encode())


@pytest.mark.parametrize("encoding", ["utf8", "latin1", "sql_ascii"])
def test_register_conn(hstore, conn, encoding):
    conn.execute("select set_config('client_encoding', %s, false)", [encoding])
    info = TypeInfo.fetch(conn, "hstore")
    register_hstore(info, conn)
    assert conn.adapters.types[info.oid].name == "hstore"

    cur = conn.execute("select null::hstore, ''::hstore, 'a => b'::hstore")
    assert cur.fetchone() == (None, {}, {"a": "b"})


def test_register_curs(hstore, conn):
    info = TypeInfo.fetch(conn, "hstore")
    cur = conn.cursor()
    register_hstore(info, cur)
    assert conn.adapters.types.get(info.oid) is None
    assert cur.adapters.types[info.oid].name == "hstore"

    cur.execute("select null::hstore, ''::hstore, 'a => b'::hstore")
    assert cur.fetchone() == (None, {}, {"a": "b"})


def test_register_globally(conn_cls, hstore, dsn, svcconn, global_adapters):
    info = TypeInfo.fetch(svcconn, "hstore")
    register_hstore(info)
    assert psycopg.adapters.types[info.oid].name == "hstore"

    assert svcconn.adapters.types.get(info.oid) is None
    conn = conn_cls.connect(dsn)
    assert conn.adapters.types[info.oid].name == "hstore"

    cur = conn.execute("select null::hstore, ''::hstore, 'a => b'::hstore")
    assert cur.fetchone() == (None, {}, {"a": "b"})
    conn.close()


ab = list(map(chr, range(32, 128)))
samp = [{}, {"a": "b", "c": None}, dict(zip(ab, ab)), {"".join(ab): "".join(ab)}]


@pytest.mark.parametrize("d", samp)
@pytest.mark.parametrize("fmt_out", Format)
def test_roundtrip(hstore, conn, d, fmt_out):
    register_hstore(TypeInfo.fetch(conn, "hstore"), conn)
    d1 = conn.cursor(binary=fmt_out).execute("select %s", [d]).fetchone()[0]
    assert d == d1


@pytest.mark.parametrize("fmt_out", Format)
def test_roundtrip_array(hstore, conn, fmt_out):
    register_hstore(TypeInfo.fetch(conn, "hstore"), conn)
    samp1 = conn.cursor(binary=fmt_out).execute("select %s", (samp,)).fetchone()[0]
    assert samp1 == samp


def test_no_info_error(conn):
    with pytest.raises(TypeError, match="hstore.*extension"):
        register_hstore(None, conn)  # type: ignore[arg-type]
