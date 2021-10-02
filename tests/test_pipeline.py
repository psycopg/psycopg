import pytest

from psycopg import pq

pytestmark = pytest.mark.libpq(">=14")


def test_pipeline_status(conn):
    assert not conn._pipeline_mode
    with conn.pipeline() as p:
        assert p.status() == pq.PipelineStatus.ON
        assert conn._pipeline_mode
        p.sync()
        r = conn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = conn.pgconn.get_result()
        assert r is None
    assert p.status() == pq.PipelineStatus.OFF
    assert not conn._pipeline_mode
