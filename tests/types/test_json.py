import json

import pytest

import psycopg3.types.json
from psycopg3.types.json import Json, JsonB
from psycopg3.adapt import Format

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
@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_json_dump(conn, val, fmt_in):
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute(f"select pg_typeof({ph}) = 'json'::regtype", (Json(obj),))
    assert cur.fetchone()[0] is True
    cur.execute(f"select {ph}::text = %s::json::text", (Json(obj), val))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("val", samples)
def test_jsonb_dump(conn, val, fmt_in):
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute(f"select {ph} = %s::jsonb", (JsonB(obj), val))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("val", samples)
@pytest.mark.parametrize("jtype", ["json", "jsonb"])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_json_load(conn, val, jtype, fmt_out):
    cur = conn.cursor(format=fmt_out)
    cur.execute(f"select %s::{jtype}", (val,))
    assert cur.fetchone()[0] == json.loads(val)


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("wrapper", ["Json", "JsonB"])
def test_json_dump_customise(conn, wrapper, fmt_in):
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    wrapper = getattr(psycopg3.types.json, wrapper)
    obj = {"foo": "bar"}
    cur = conn.cursor()
    cur.execute(
        f"select {ph}->>'baz' = 'qux'", (wrapper(obj, dumps=my_dumps),)
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("wrapper", ["Json", "JsonB"])
def test_json_dump_subclass(conn, wrapper, fmt_in):
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    wrapper = getattr(psycopg3.types.json, wrapper)

    class MyWrapper(wrapper):
        def dumps(self):
            return my_dumps(self.obj)

    obj = {"foo": "bar"}
    cur = conn.cursor()
    cur.execute(f"select {ph}->>'baz' = 'qux'", (MyWrapper(obj),))
    assert cur.fetchone()[0] is True


def my_dumps(obj):
    obj["baz"] = "qux"
    return json.dumps(obj)
