import json
import logging
from copy import deepcopy
from typing import Any

import pytest

import psycopg.types
from psycopg import pq, sql
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


@pytest.mark.parametrize(
    "fmt_in, pgtype, dumper_name",
    [
        ("t", "json", "JsonDumper"),
        ("b", "json", "JsonBinaryDumper"),
        ("t", "jsonb", "JsonbDumper"),
        ("b", "jsonb", "JsonbBinaryDumper"),
    ],
)
def test_dump_dict(conn, fmt_in, pgtype, dumper_name):
    obj = {"foo": "bar"}
    cur = conn.cursor()
    dumper = getattr(psycopg.types.json, dumper_name)

    # Skip json on CRDB as the oid doesn't exist.
    try:
        conn.adapters.types[dumper.oid]
    except KeyError:
        pytest.skip(
            f"{type(conn).__name__} doesn't have the oid {dumper.oid}"
            f" used by {dumper.__name__}"
        )

    cur.adapters.register_dumper(dict, dumper)
    cur.execute(f"select %{fmt_in}", (obj,))
    assert cur.fetchone()[0] == obj
    assert cur.description[0].type_code == conn.adapters.types[pgtype].oid


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
def test_dump_customise_bytes(conn, wrapper, fmt_in):
    wrapper = getattr(psycopg.types.json, wrapper)
    obj = {"foo": "bar"}
    cur = conn.cursor()

    set_json_dumps(my_dumps_bytes)
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


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
def test_dump_leak_with_local_functions(dsn, binary, pgtype, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    # Note: private implementation, it might change
    from psycopg.types.json import _dumpers_cache

    # A function with no closure is cached on the code, so lambdas are not
    # different items.

    def register(conn: psycopg.Connection) -> None:
        set_json_dumps(lambda x: json.dumps(x), conn)

    with psycopg.connect(dsn) as conn1:
        register(conn1)
    assert (size1 := len(_dumpers_cache))

    with psycopg.connect(dsn) as conn2:
        register(conn2)
    size2 = len(_dumpers_cache)

    assert size1 == size2
    assert not caplog.records

    # A function with a closure won't be cached, but will cause a warning

    def register2(conn: psycopg.Connection, skipkeys: bool) -> None:
        def f(x: Any) -> str:
            return json.dumps(x, skipkeys=skipkeys)

        set_json_dumps(f, conn)

    with psycopg.connect(dsn) as conn3:
        register2(conn3, False)
    size3 = len(_dumpers_cache)

    assert size2 == size3
    assert caplog.records


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
@pytest.mark.parametrize("dumps", [str, len])
def test_dumper_warning_builtin(dsn, binary, pgtype, dumps, caplog, recwarn):
    caplog.set_level(logging.WARNING, logger="psycopg")
    recwarn.clear()

    # Note: private implementation, it might change
    from psycopg.types.json import _dumpers_cache

    # A function with no closure is cached on the code, so lambdas are not
    # different items.

    with psycopg.connect(dsn) as conn1:
        set_json_dumps(dumps, conn1)
    assert not recwarn
    assert (size1 := len(_dumpers_cache))

    with psycopg.connect(dsn) as conn2:
        set_json_dumps(dumps, conn2)
    size2 = len(_dumpers_cache)

    assert size1 == size2
    assert not caplog.records
    assert not recwarn


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
def test_load_leak_with_local_functions(dsn, binary, pgtype, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    # Note: private implementation, it might change
    from psycopg.types.json import _loaders_cache

    # A function with no closure is cached on the code, so lambdas are not
    # different items.

    def register(conn: psycopg.Connection) -> None:

        def f(x: "str | bytes") -> Any:
            return json.loads(x)

        set_json_loads(f, conn)

    with psycopg.connect(dsn) as conn1:
        register(conn1)
    assert (size1 := len(_loaders_cache))

    with psycopg.connect(dsn) as conn2:
        register(conn2)
    size2 = len(_loaders_cache)

    assert size1 == size2
    assert not caplog.records

    # A function with a closure won't be cached, but will cause a warning

    def register2(conn: psycopg.Connection, parse_float: Any) -> None:
        set_json_dumps(lambda x: json.dumps(x, parse_float=parse_float), conn)

    with psycopg.connect(dsn) as conn3:
        register2(conn3, None)
    size3 = len(_loaders_cache)

    assert size2 == size3
    assert caplog.records


@pytest.mark.parametrize("binary", [True, False])
@pytest.mark.parametrize("pgtype", ["json", "jsonb"])
@pytest.mark.parametrize("loads", [str, len])
def test_loader_warning_builtin(dsn, binary, pgtype, loads, caplog, recwarn):
    caplog.set_level(logging.WARNING, logger="psycopg")
    recwarn.clear()

    # Note: private implementation, it might change
    from psycopg.types.json import _loaders_cache

    # A function with no closure is cached on the code, so lambdas are not
    # different items.

    with psycopg.connect(dsn) as conn1:
        set_json_loads(loads, conn1)
    assert not recwarn
    assert (size1 := len(_loaders_cache))

    with psycopg.connect(dsn) as conn2:
        set_json_loads(loads, conn2)
    size2 = len(_loaders_cache)

    assert size1 == size2
    assert not caplog.records
    assert not recwarn


def my_dumps(obj):
    obj = deepcopy(obj)
    obj["baz"] = "qux"
    return json.dumps(obj)


def my_dumps_bytes(obj):
    obj = deepcopy(obj)
    obj["baz"] = "qux"
    return json.dumps(obj).encode()


def my_loads(data):
    obj = json.loads(data)
    obj["answer"] = 42
    return obj
