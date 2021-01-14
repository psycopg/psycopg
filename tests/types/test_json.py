import json

import pytest

import psycopg3.types
from psycopg3 import pq
from psycopg3.types import Json, Jsonb
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


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_json_dump_customise(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg3.types, wrapper)
    obj = {"foo": "bar"}
    cur = conn.cursor()
    cur.execute(
        f"select %{fmt_in}->>'baz' = 'qux'", (wrapper(obj, dumps=my_dumps),)
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("fmt_in", [Format.AUTO, Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("wrapper", ["Json", "Jsonb"])
def test_json_dump_subclass(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg3.types, wrapper)

    class MyWrapper(wrapper):
        def dumps(self):
            return my_dumps(self.obj)

    obj = {"foo": "bar"}
    cur = conn.cursor()
    cur.execute(f"select %{fmt_in}->>'baz' = 'qux'", (MyWrapper(obj),))
    assert cur.fetchone()[0] is True


def my_dumps(obj):
    obj["baz"] = "qux"
    return json.dumps(obj)
