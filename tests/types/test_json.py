import json

import pytest

from psycopg3.types.json import Json, JsonB

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
def test_json_dump(conn, val):
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute("select pg_typeof(%s) = 'json'::regtype", (Json(obj),))
    assert cur.fetchone()[0] is True
    cur.execute("select %s::text = %s::json::text", (Json(obj), val))
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("val", samples)
def test_jsonb_dump(conn, val):
    obj = json.loads(val)
    cur = conn.cursor()
    cur.execute("select %s = %s::jsonb", (JsonB(obj), val))
    assert cur.fetchone()[0] is True
