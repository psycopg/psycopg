import pytest

from psycopg import pq

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.libpq(">=14"),
]


async def test_pipeline_status(aconn):
    assert not aconn._pipeline_mode
    async with aconn.pipeline() as p:
        assert await p.status() == pq.PipelineStatus.ON
        assert aconn._pipeline_mode
        await p.sync()
        r = aconn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = aconn.pgconn.get_result()
        assert r is None
    assert await p.status() == pq.PipelineStatus.OFF
    assert not aconn._pipeline_mode
