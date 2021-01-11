import pytest
import psycopg3
from psycopg3.oids import builtins
from psycopg3.adapt import Format, Transformer
from psycopg3.types import array


tests_str = [
    ([], "{}"),
    ([[[[[["a"]]]]]], "{{{{{{a}}}}}}"),
    ([[[[[[None]]]]]], "{{{{{{NULL}}}}}}"),
    (["foo", "bar", "baz"], "{foo,bar,baz}"),
    (["foo", None, "baz"], "{foo,null,baz}"),
    (["foo", "null", "", "baz"], '{foo,"null","",baz}'),
    (
        [["foo", "bar"], ["baz", "qux"], ["quux", "quuux"]],
        "{{foo,bar},{baz,qux},{quux,quuux}}",
    ),
    (
        [[["fo{o", "ba}r"], ['ba"z', "qu'x"], ["qu ux", " "]]],
        r'{{{"fo{o","ba}r"},{"ba\"z",qu\'x},{"qu ux"," "}}}',
    ),
]


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("obj, want", tests_str)
def test_dump_list_str(conn, obj, want, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    cur.execute(f"select {ph}::text[] = %s::text[]", (obj, want))
    assert cur.fetchone()[0]


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("want, obj", tests_str)
def test_load_list_str(conn, obj, want, fmt_out):
    cur = conn.cursor(format=fmt_out)
    cur.execute("select %s::text[]", (obj,))
    assert cur.fetchone()[0] == want


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_all_chars(conn, fmt_in, fmt_out):
    cur = conn.cursor(format=fmt_out)
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    for i in range(1, 256):
        c = chr(i)
        cur.execute(f"select {ph}::text[]", ([c],))
        assert cur.fetchone()[0] == [c]

    a = list(map(chr, range(1, 256)))
    a.append("\u20ac")
    cur.execute(f"select {ph}::text[]", (a,))
    assert cur.fetchone()[0] == a

    a = "".join(a)
    cur.execute(f"select {ph}::text[]", ([a],))
    assert cur.fetchone()[0] == [a]


tests_int = [
    ([], "{}"),
    ([10, 20, -30], "{10,20,-30}"),
    ([10, None, 30], "{10,null,30}"),
    ([[10, 20], [30, 40]], "{{10,20},{30,40}}"),
]


@pytest.mark.parametrize("obj, want", tests_int)
def test_dump_list_int(conn, obj, want):
    cur = conn.cursor()
    cur.execute("select %s::int[] = %s::int[]", (obj, want))
    assert cur.fetchone()[0]


@pytest.mark.parametrize(
    "input",
    [
        [["a"], ["b", "c"]],
        [["a"], []],
        [[]],
        [[["a"]], ["b"]],
        # [["a"], [["b"]]],  # todo, but expensive (an isinstance per item)
        # [True, b"a"], # TODO expensive too
    ],
)
def test_bad_binary_array(input):
    tx = Transformer()
    with pytest.raises(psycopg3.DataError):
        tx.get_dumper(input, Format.BINARY).dump(input)


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("want, obj", tests_int)
def test_load_list_int(conn, obj, want, fmt_out):
    cur = conn.cursor(format=fmt_out)
    cur.execute("select %s::int[]", (obj,))
    assert cur.fetchone()[0] == want


def test_array_register(conn):
    cur = conn.cursor()
    cur.execute("create table mytype (data text)")
    cur.execute("""select '(foo)'::mytype, '{"(foo)"}'::mytype[]""")
    res = cur.fetchone()
    assert res[0] == "(foo)"
    assert res[1] == "{(foo)}"

    array.register(
        cur.description[1].type_code, cur.description[0].type_code, context=cur
    )
    cur.execute("""select '(foo)'::mytype, '{"(foo)"}'::mytype[]""")
    res = cur.fetchone()
    assert res[0] == "(foo)"
    assert res[1] == ["(foo)"]


def test_array_of_unknown_builtin(conn):
    # we cannot load this type, but we understand it is an array
    val = "postgres=arwdDxt/postgres"
    cur = conn.cursor()
    cur.execute(f"select '{val}'::aclitem, array['{val}']::aclitem[]")
    res = cur.fetchone()
    assert cur.description[0].type_code == builtins["aclitem"].oid
    assert res[0] == val
    assert cur.description[1].type_code == builtins["aclitem"].array_oid
    assert res[1] == [val]


@pytest.mark.xfail
@pytest.mark.parametrize(
    "array, type", [([1, 32767], "int2"), ([1, 32768], "int4")]
)
def test_array_mixed_numbers(array, type):
    # TODO: must use the type accommodating the largest/highest precision
    tx = Transformer()
    dumper = tx.get_dumper(array, Format.BINARY)
    dumper.dump(array)
    assert dumper.oid == builtins[type].array_oid


@pytest.mark.xfail
@pytest.mark.parametrize("fmt_in", [Format.BINARY])  # TODO: add Format.TEXT
def test_empty_list_mix(conn, fmt_in):
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    objs = list(range(3))
    # pro tip: don't get confused with the types
    f1, f2 = conn.execute(f"select {ph}, {ph}", (objs, [])).fetchone()
    assert f1 == objs
    assert f2 == []
