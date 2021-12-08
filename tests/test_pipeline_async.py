import pytest

from psycopg import pq
from psycopg.errors import ProgrammingError

pytestmark = [
    pytest.mark.libpq(">= 14"),
    pytest.mark.asyncio,
]


async def test_pipeline_status(aconn):
    async with aconn.pipeline():
        p = aconn._pipeline
        assert p is not None
        assert p.status == pq.PipelineStatus.ON
        with pytest.raises(ProgrammingError):
            async with aconn.pipeline():
                pass
    assert p.status == pq.PipelineStatus.OFF
    assert not aconn._pipeline
