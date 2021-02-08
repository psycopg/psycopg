import json
from copy import deepcopy

import pytest

import psycopg3.types
from psycopg3 import pq
from psycopg3 import sql
from psycopg3.types import Json, Jsonb
from psycopg3.adapt import Format
from psycopg3.types import set_json_dumps, set_json_loads

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


@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
def test_json_dump(conn, val, fmt_in):
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute(f"select pg_typeof(%{fmt_in}) = 'json'::regtype", (Json(obj),))
    assert cur.fetchone()[0] is True
    cur.execute(f"select %{fmt_in}::text = %s::json::text", (Json(obj), val))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("val", samples)
def test_jsonb_dump(conn, val, fmt_in):
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in} = %s::jsonb", (Jsonb(obj), val))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("jtype", ["json", "jsonb"])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_json_load(conn, val, jtype, fmt_out):
    cur = conn.cursor(binary=fmt_out)
    cur.execute(f"select %s::{jtype}", (val,))
    assert cur.fetchone()[0] == json.loads(val)


@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("jtype", ["json", "jsonb"])
@pytest.mark.parametrize("fmt_out", [pq.Format.TEXT, pq.Format.BINARY])
def test_json_load_copy(conn, val, jtype, fmt_out):
    cur = conn.cursor()
    stmt = sql.SQL("copy (select {}::{}) to stdout (format {})").format(
        val, sql.Identifier(jtype), sql.SQL(fmt_out.name)
    )
    with cur.copy(stmt) as copy:
        copy.set_types([jtype])
        (got,) = copy.read_row()

    assert got == json.loads(val)


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_json_dump_customise(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg3.types, wrapper)
    obj = {"foo": "bar"}
    cur = conn.cursor()

    set_json_dumps(my_dumps)
    try:
        cur.execute(f"select %{fmt_in}->>'baz' = 'qux'", (wrapper(obj),))
        assert cur.fetchone()[0] is True
    finally:
        set_json_dumps(json.dumps)


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_json_dump_subclass(conn, wrapper, fmt_in):
    JDumper = getattr(
        psycopg3.types,
        f"{wrapper}{'Binary' if fmt_in != Format.TEXT else ''}Dumper",
    )
    wrapper = getattr(psycopg3.types, wrapper)

    class MyJsonDumper(JDumper):
        def get_dumps(self):
            return my_dumps

    obj = {"foo": "bar"}
    cur = conn.cursor()
    MyJsonDumper.register(wrapper, context=cur)
    cur.execute(f"select %{fmt_in}->>'baz' = 'qux'", (wrapper(obj),))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
def test_json_load_customise(conn, binary, pgtype):
    obj = {"foo": "bar"}
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
def test_json_load_subclass(conn, binary, pgtype):
    JLoader = getattr(
        psycopg3.types,
        f"{pgtype.title()}{'Binary' if binary else ''}Loader",
    )

    class MyJsonLoader(JLoader):
        def get_loads(self):
            return my_loads

    cur = conn.cursor(binary=binary)
    MyJsonLoader.register(cur.adapters.types[pgtype].oid, context=cur)
    cur.execute(f"""select '{{"foo": "bar"}}'::{pgtype}""")
    obj = cur.fetchone()[0]
    assert obj["foo"] == "bar"
    assert obj["answer"] == 42


def my_dumps(obj):
    obj = deepcopy(obj)
    obj["baz"] = "qux"
    return json.dumps(obj)


def my_loads(data):
    obj = json.loads(data)
    obj["answer"] = 42
    return obj
