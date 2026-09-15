from __future__ import annotations

import sys

import pytest

if sys.platform != "emscripten":
    pytest.skip("Pyodide-specific tests", allow_module_level=True)

import psycopg


def test_pyodide_connection(dsn):
    with psycopg.connect(dsn) as conn:
        value = conn.execute("select %s::int", (42,)).fetchone()
        assert value == (42,)
