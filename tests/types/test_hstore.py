import pytest

import psycopg
from psycopg.types import TypeInfo
from psycopg.types.hstore import HstoreLoader, register_hstore

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


def test_register_conn(hstore, conn):
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
samp = [
    {},
    {"a": "b", "c": None},
    dict(zip(ab, ab)),
    {"".join(ab): "".join(ab)},
]


@pytest.mark.parametrize("d", samp)
def test_roundtrip(hstore, conn, d):
    register_hstore(TypeInfo.fetch(conn, "hstore"), conn)
    d1 = conn.execute("select %s", [d]).fetchone()[0]
    assert d == d1


def test_roundtrip_array(hstore, conn):
    register_hstore(TypeInfo.fetch(conn, "hstore"), conn)
    samp1 = conn.execute("select %s", (samp,)).fetchone()[0]
    assert samp1 == samp


def test_no_info_error(conn):
    with pytest.raises(TypeError, match="hstore.*extension"):
        register_hstore(None, conn)  # type: ignore[arg-type]
