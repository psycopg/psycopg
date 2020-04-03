import pytest
from psycopg3.types import builtins
from psycopg3.adapt import Typecaster, UnknownCaster
from psycopg3.types.array import UnknownArrayCaster, ArrayCaster


tests_str = [
    ([], "{}"),
    (["foo", "bar", "baz"], "{foo,bar,baz}"),
    (["foo", None, "baz"], "{foo,null,baz}"),
    (["foo", "null", "", "baz"], '{foo,"null","",baz}'),
    ([["foo", "bar"], ["baz", "qux"]], "{{foo,bar},{baz,qux}}"),
    (
        [[["fo{o", "ba}r"], ['ba"z', "qu'x"], ["qu ux", " "]]],
        r'{{{"fo{o","ba}r"},{"ba\"z",qu\'x},{"qu ux"," "}}}',
    ),
]


@pytest.mark.parametrize("obj, want", tests_str)
def test_adapt_list_str(conn, obj, want):
    cur = conn.cursor()
    cur.execute("select %s::text[] = %s::text[]", (obj, want))
    assert cur.fetchone()[0]


@pytest.mark.parametrize("want, obj", tests_str)
def test_cast_list_str(conn, obj, want):
    cur = conn.cursor()
    cur.execute("select %s::text[]", (obj,))
    assert cur.fetchone()[0] == want


def test_all_chars(conn):
    cur = conn.cursor()
    for i in range(1, 256):
        c = chr(i)
        cur.execute("select %s::text[]", ([c],))
        assert cur.fetchone()[0] == [c]

    a = list(map(chr, range(1, 256)))
    a.append("\u20ac")
    cur.execute("select %s::text[]", (a,))
    assert cur.fetchone()[0] == a

    a = "".join(a)
    cur.execute("select %s::text[]", ([a],))
    assert cur.fetchone()[0] == [a]


tests_int = [
    ([], "{}"),
    ([10, 20, -30], "{10,20,-30}"),
    ([10, None, 30], "{10,null,30}"),
    ([[10, 20], [30, 40]], "{{10,20},{30,40}}"),
]


@pytest.mark.parametrize("obj, want", tests_int)
def test_adapt_list_int(conn, obj, want):
    cur = conn.cursor()
    cur.execute("select %s::int[] = %s::int[]", (obj, want))
    assert cur.fetchone()[0]


@pytest.mark.parametrize("want, obj", tests_int)
def test_cast_list_int(conn, obj, want):
    cur = conn.cursor()
    cur.execute("select %s::int[]", (obj,))
    assert cur.fetchone()[0] == want


def test_unknown(conn):
    # unknown for real
    assert builtins["aclitem"].array_oid not in Typecaster.globals
    Typecaster.register(
        builtins["aclitem"].array_oid, UnknownArrayCaster, context=conn
    )
    cur = conn.cursor()
    cur.execute("select '{postgres=arwdDxt/postgres}'::aclitem[]")
    res = cur.fetchone()[0]
    assert res == ["postgres=arwdDxt/postgres"]


def test_array_register(conn):
    cur = conn.cursor()
    cur.execute("select '{postgres=arwdDxt/postgres}'::aclitem[]")
    res = cur.fetchone()[0]
    assert res == "{postgres=arwdDxt/postgres}"

    ArrayCaster.register(
        builtins["aclitem"].array_oid, UnknownCaster, context=conn
    )
    cur.execute("select '{postgres=arwdDxt/postgres}'::aclitem[]")
    res = cur.fetchone()[0]
    assert res == ["postgres=arwdDxt/postgres"]
