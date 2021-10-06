import pytest

import psycopg
from psycopg import pq
from psycopg.errors import ProgrammingError

pytestmark = pytest.mark.libpq(">= 14")


def test_pipeline_status(conn):
    with conn.pipeline():
        p = conn._pipeline
        assert p is not None
        assert p.status == pq.PipelineStatus.ON
        with pytest.raises(ProgrammingError):
            with conn.pipeline():
                pass
    assert p.status == pq.PipelineStatus.OFF
    assert not conn._pipeline


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_server_cursor(conn):
    with conn.cursor(name="pipeline") as cur, conn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 1")
