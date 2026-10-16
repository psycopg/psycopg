import sys
import json
import subprocess as sp
from pathlib import Path

import pytest

import psycopg

pytestmark = pytest.mark.crdb_skip("server-side cursor")


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("autocommit", [False, True])
@pytest.mark.parametrize("scenario", ["row", "notice"])
def test_close_from_gc(dsn, scenario, autocommit, raw):
    run_case(dsn, scenario, autocommit, raw)


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize(
    "scenario",
    ["query_error", "stale_portal", "cross_thread"],
)
def test_deferred_close_lifecycle(dsn, scenario, raw):
    run_case(dsn, scenario, True, raw)


@pytest.mark.pipeline
@pytest.mark.skipif(
    not psycopg.Pipeline.is_supported(), reason="pipeline mode not supported"
)
@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("scenario", ["pipeline", "pipeline_error"])
def test_deferred_close_pipeline(dsn, scenario, raw):
    run_case(dsn, scenario, True, raw)


def run_case(dsn, scenario, autocommit, raw):
    # A broken implementation deadlocks inside cyclic garbage collection.
    # Isolate it so a failed regression cannot hang the whole test suite.
    script = Path(__file__).parent / "scripts" / "server_cursor_gc.py"
    params = dict(dsn=dsn, scenario=scenario, autocommit=autocommit, raw=raw)
    result = sp.run(
        [sys.executable, str(script), json.dumps(params)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
