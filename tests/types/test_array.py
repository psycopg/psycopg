from typing import List, Any
from decimal import Decimal

import pytest

import psycopg
from psycopg import pq
from psycopg import sql
from psycopg.adapt import PyFormat, Transformer, Dumper
from psycopg.types import TypeInfo
from psycopg._compat import prod
from psycopg.postgres import types as builtins


tests_str = [
    ([[[[[["a"]]]]]], "{{{{{{a}}}}}}"),
    ([[[[[[None]]]]]], "{{{{{{NULL}}}}}}"),
    ([[[[[["NULL"]]]]]], '{{{{{{"NULL"}}}}}}'),
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


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("type", ["text", "int4"])
def test_dump_empty_list(conn, fmt_in, type):
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value}::{type}[] = %s::{type}[]", ([], "{}"))
    assert cur.fetchone()[0]


@pytest.mark.crdb_skip("nested array")
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("obj, want", tests_str)
def test_dump_list_str(conn, obj, want, fmt_in):
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value}::text[] = %s::text[]", (obj, want))
    assert cur.fetchone()[0]


@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_empty_list_str(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute("select %s::text[]", ([],))
    assert cur.fetchone()[0] == []


@pytest.mark.crdb_skip("nested array")
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("want, obj", tests_str)
def test_load_list_str(conn, obj, want, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute("select %s::text[]", (obj,))
    assert cur.fetchone()[0] == want


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_all_chars(conn, fmt_in, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 256):
        c = chr(i)
        cur.execute(f"select %{fmt_in.value}::text[]", ([c],))
        assert cur.fetchone()[0] == [c]

    a = list(map(chr, range(1, 256)))
    a.append("\u20ac")
    cur.execute(f"select %{fmt_in.value}::text[]", (a,))
    assert cur.fetchone()[0] == a

    s = "".join(a)
    cur.execute(f"select %{fmt_in.value}::text[]", ([s],))
    assert cur.fetchone()[0] == [s]


tests_int = [
    ([10, 20, -30], "{10,20,-30}"),
    ([10, None, 30], "{10,null,30}"),
    ([[10, 20], [30, 40]], "{{10,20},{30,40}}"),
]


@pytest.mark.crdb_skip("nested array")
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
        [[["a"]], ["b"]],
        # [["a"], [["b"]]],  # todo, but expensive (an isinstance per item)
        # [True, b"a"], # TODO expensive too
    ],
)
def test_bad_binary_array(input):
    tx = Transformer()
    with pytest.raises(psycopg.DataError):
        tx.get_dumper(input, PyFormat.BINARY).dump(input)


@pytest.mark.crdb_skip("nested array")
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("want, obj", tests_int)
def test_load_list_int(conn, obj, want, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute("select %s::int[]", (obj,))
    assert cur.fetchone()[0] == want

    stmt = sql.SQL("copy (select {}::int[]) to stdout (format {})").format(
        obj, sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types(["int4[]"])
        (got,) = copy.read_row()

    assert got == want


@pytest.mark.crdb_skip("composite")
def test_array_register(conn):
    conn.execute("create table mytype (data text)")
    cur = conn.execute("""select '(foo)'::mytype, '{"(foo)"}'::mytype[]""")
    res = cur.fetchone()
    assert res[0] == "(foo)"
    assert res[1] == "{(foo)}"

    info = TypeInfo.fetch(conn, "mytype")
    info.register(conn)

    cur = conn.execute("""select '(foo)'::mytype, '{"(foo)"}'::mytype[]""")
    res = cur.fetchone()
    assert res[0] == "(foo)"
    assert res[1] == ["(foo)"]


@pytest.mark.crdb("skip", reason="aclitem")
def test_array_of_unknown_builtin(conn):
    user = conn.execute("select user").fetchone()[0]
    # we cannot load this type, but we understand it is an array
    val = f"{user}=arwdDxt/{user}"
    cur = conn.execute(f"select '{val}'::aclitem, array['{val}']::aclitem[]")
    res = cur.fetchone()
    assert cur.description[0].type_code == builtins["aclitem"].oid
    assert res[0] == val
    assert cur.description[1].type_code == builtins["aclitem"].array_oid
    assert res[1] == [val]


@pytest.mark.parametrize(
    "num, type",
    [
        (0, "int2"),
        (2**15 - 1, "int2"),
        (-(2**15), "int2"),
        (2**15, "int4"),
        (2**31 - 1, "int4"),
        (-(2**31), "int4"),
        (2**31, "int8"),
        (2**63 - 1, "int8"),
        (-(2**63), "int8"),
        (2**63, "numeric"),
    ],
)
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_numbers_array(num, type, fmt_in):
    for array in ([num], [1, num]):
        tx = Transformer()
        dumper = tx.get_dumper(array, fmt_in)
        dumper.dump(array)
        assert dumper.oid == builtins[type].array_oid


@pytest.mark.parametrize("wrapper", "Int2 Int4 Int8 Float4 Float8 Decimal".split())
@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_list_number_wrapper(conn, wrapper, fmt_in, fmt_out):
    wrapper = getattr(psycopg.types.numeric, wrapper)
    if wrapper is Decimal:
        want_cls = Decimal
    else:
        assert wrapper.__mro__[1] in (int, float)
        want_cls = wrapper.__mro__[1]

    obj = [wrapper(1), wrapper(0), wrapper(-1), None]
    cur = conn.cursor(binary=fmt_out)
    got = cur.execute(f"select %{fmt_in.value}", [obj]).fetchone()[0]
    assert got == obj
    for i in got:
        if i is not None:
            assert type(i) is want_cls


def test_mix_types(conn):
    with pytest.raises(psycopg.DataError):
        conn.execute("select %s", ([1, 0.5],))

    with pytest.raises(psycopg.DataError):
        conn.execute("select %s", ([1, Decimal("0.5")],))


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_empty_list_mix(conn, fmt_in):
    objs = list(range(3))
    conn.execute("create table testarrays (col1 bigint[], col2 bigint[])")
    # pro tip: don't get confused with the types
    f1, f2 = conn.execute(
        f"insert into testarrays values (%{fmt_in.value}, %{fmt_in.value}) returning *",
        (objs, []),
    ).fetchone()
    assert f1 == objs
    assert f2 == []


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_empty_list(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table test (id serial primary key, data date[])")
    with conn.transaction():
        cur.execute(
            f"insert into test (data) values (%{fmt_in.value}) returning id", ([],)
        )
        id = cur.fetchone()[0]
    cur.execute("select data from test")
    assert cur.fetchone() == ([],)

    # test untyped list in a filter
    cur.execute(f"select data from test where id = any(%{fmt_in.value})", ([id],))
    assert cur.fetchone()
    cur.execute(f"select data from test where id = any(%{fmt_in.value})", ([],))
    assert not cur.fetchone()


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_empty_list_after_choice(conn, fmt_in):
    cur = conn.cursor()
    cur.execute("create table test (id serial primary key, data float[])")
    cur.executemany(
        f"insert into test (data) values (%{fmt_in.value})", [([1.0],), ([],)]
    )
    cur.execute("select data from test order by id")
    assert cur.fetchall() == [([1.0],), ([],)]


@pytest.mark.crdb_skip("geometric types")
def test_dump_list_no_comma_separator(conn):
    class Box:
        def __init__(self, x1, y1, x2, y2):
            self.coords = (x1, y1, x2, y2)

    class BoxDumper(Dumper):

        format = pq.Format.TEXT
        oid = psycopg.postgres.types["box"].oid

        def dump(self, box):
            return ("(%s,%s),(%s,%s)" % box.coords).encode()

    conn.adapters.register_dumper(Box, BoxDumper)

    cur = conn.execute("select (%s::box)::text", (Box(1, 2, 3, 4),))
    got = cur.fetchone()[0]
    assert got == "(3,4),(1,2)"

    cur = conn.execute(
        "select (%s::box[])::text", ([Box(1, 2, 3, 4), Box(5, 4, 3, 2)],)
    )
    got = cur.fetchone()[0]
    assert got == "{(3,4),(1,2);(5,4),(3,2)}"


@pytest.mark.crdb_skip("geometric types")
def test_load_array_no_comma_separator(conn):
    cur = conn.execute("select '{(2,2),(1,1);(5,6),(3,4)}'::box[]")
    # Not parsed at the moment, but split ok on ; separator
    assert cur.fetchone()[0] == ["(2,2),(1,1)", "(5,6),(3,4)"]


@pytest.mark.crdb_skip("nested array")
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_nested_array(conn, fmt_out):
    dims = [3, 4, 5, 6]
    a: List[Any] = list(range(prod(dims)))
    for dim in dims[-1:0:-1]:
        a = [a[i : i + dim] for i in range(0, len(a), dim)]

    assert a[2][3][4][5] == prod(dims) - 1

    sa = str(a).replace("[", "{").replace("]", "}")
    got = conn.execute("select %s::int[][][][]", [sa], binary=fmt_out).fetchone()[0]
    assert got == a


@pytest.mark.crdb_skip("nested array")
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize(
    "obj, want",
    [
        ("'[0:1]={a,b}'::text[]", ["a", "b"]),
        ("'[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}'::int[]", [[[1, 2, 3], [4, 5, 6]]]),
    ],
)
def test_array_with_bounds(conn, obj, want, fmt_out):
    got = conn.execute(f"select {obj}", binary=fmt_out).fetchone()[0]
    assert got == want


@pytest.mark.crdb_skip("nested array")
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_all_chars_with_bounds(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    for i in range(1, 256):
        c = chr(i)
        cur.execute("select '[0:1]={a,b}'::text[] || %s::text[]", ([c],))
        assert cur.fetchone()[0] == ["a", "b", c]

    a = list(map(chr, range(1, 256)))
    a.append("\u20ac")
    cur.execute("select '[0:1]={a,b}'::text[] || %s::text[]", (a,))
    assert cur.fetchone()[0] == ["a", "b"] + a

    s = "".join(a)
    cur.execute("select '[0:1]={a,b}'::text[] || %s::text[]", ([s],))
    assert cur.fetchone()[0] == ["a", "b", s]
