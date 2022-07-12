import json
from copy import deepcopy

import pytest

import psycopg.types
from psycopg import pq
from psycopg import sql
from psycopg.adapt import PyFormat
from psycopg.types.json import set_json_dumps, set_json_loads

samples = [
    "null",
    "true",
    '"te\'xt"',
    '"\\u00e0\\u20ac"',
    "123",
    "123.45",
    '["a", 100]',
    '{"a": 100}',
]


@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_wrapper_regtype(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    cur = conn.cursor()
    cur.execute(
        f"select pg_typeof(%{fmt_in.value})::regtype = %s::regtype",
        (wrapper([]), wrapper.__name__.lower()),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_dump(conn, val, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute(
        f"select %{fmt_in.value}::text = %s::{wrapper.__name__.lower()}::text",
        (wrapper(obj), val),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.crdb_skip("json array")
@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
@pytest.mark.parametrize("fmt_in", PyFormat)
def test_array_dump(conn, val, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute(
        f"select %{fmt_in.value}::text = array[%s::{wrapper.__name__.lower()}]::text",
        ([wrapper(obj)], val),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("jtype", ["json", "jsonb"])
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load(conn, val, jtype, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select %s::{jtype}", (val,))
    assert cur.fetchone()[0] == json.loads(val)


@pytest.mark.crdb_skip("json array")
@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("jtype", ["json", "jsonb"])
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_array(conn, val, jtype, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select array[%s::{jtype}]", (val,))
    assert cur.fetchone()[0] == [json.loads(val)]


@pytest.mark.crdb_skip("copy")
@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("jtype", ["json", "jsonb"])
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_load_copy(conn, val, jtype, fmt_out):
    cur = conn.cursor()
    stmt = sql.SQL("copy (select {}::{}) to stdout (format {})").format(
        val, sql.Identifier(jtype), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([jtype])
        (got,) = copy.read_row()

    assert got == json.loads(val)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_dump_customise(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    obj = {"foo": "bar"}
    cur = conn.cursor()

    set_json_dumps(my_dumps)
    try:
        cur.execute(f"select %{fmt_in.value}->>'baz' = 'qux'", (wrapper(obj),))
        assert cur.fetchone()[0] is True
    finally:
        set_json_dumps(json.dumps)


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_dump_customise_context(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    obj = {"foo": "bar"}
    cur1 = conn.cursor()
    cur2 = conn.cursor()

    set_json_dumps(my_dumps, cur2)
    cur1.execute(f"select %{fmt_in.value}->>'baz'", (wrapper(obj),))
    assert cur1.fetchone()[0] is None
    cur2.execute(f"select %{fmt_in.value}->>'baz'", (wrapper(obj),))
    assert cur2.fetchone()[0] == "qux"


@pytest.mark.parametrize("fmt_in", PyFormat)
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_dump_customise_wrapper(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    obj = {"foo": "bar"}
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in.value}->>'baz' = 'qux'", (wrapper(obj, my_dumps),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
def test_load_customise(conn, binary, pgtype):
    cur = conn.cursor(binary=binary)

    set_json_loads(my_loads)
    try:
        cur.execute(f"""select '{{"foo": "bar"}}'::{pgtype}""")
        obj = cur.fetchone()[0]
        assert obj["foo"] == "bar"
        assert obj["answer"] == 42
    finally:
        set_json_loads(json.loads)


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
def test_load_customise_context(conn, binary, pgtype):
    cur1 = conn.cursor(binary=binary)
    cur2 = conn.cursor(binary=binary)

    set_json_loads(my_loads, cur2)
    cur1.execute(f"""select '{{"foo": "bar"}}'::{pgtype}""")
    got = cur1.fetchone()[0]
    assert got["foo"] == "bar"
    assert "answer" not in got

    cur2.execute(f"""select '{{"foo": "bar"}}'::{pgtype}""")
    got = cur2.fetchone()[0]
    assert got["foo"] == "bar"
    assert got["answer"] == 42


def my_dumps(obj):
    obj = deepcopy(obj)
    obj["baz"] = "qux"
    return json.dumps(obj)


def my_loads(data):
    obj = json.loads(data)
    obj["answer"] = 42
    return obj
