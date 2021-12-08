import pytest

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
