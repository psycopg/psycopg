import json

import pytest

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
