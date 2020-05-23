import time
import pytest
import asyncio
from asyncio.queues import Queue

import psycopg3

pytestmark = pytest.mark.asyncio


@pytest.mark.slow
@pytest.mark.skip  # TODO: sometimes this test hangs?
async def test_commit_concurrency(aconn):
    # Check the condition reported in psycopg2#103
    # Because of bad status check, we commit even when a commit is already on
    # its way. We can detect this condition by the warnings.
    notices = Queue()
    aconn.add_notice_handler(
        lambda diag: notices.put_nowait(diag.message_primary)
    )
    stop = False

    async def committer():
        nonlocal stop
        while not stop:
            await aconn.commit()

    async def runner():
        nonlocal stop
        cur = aconn.cursor()
        for i in range(1000):
            await cur.execute("select %s;", (i,))
            await aconn.commit()

        # Stop the committer thread
        stop = True

    await asyncio.wait([committer(), runner()])
    assert notices.empty(), "%d notices raised" % notices.qsize()


@pytest.mark.slow
async def test_concurrent_execution(dsn):
    async def worker():
        cnn = await psycopg3.AsyncConnection.connect(dsn)
        cur = cnn.cursor()
        await cur.execute("select pg_sleep(0.5)")
        await cur.close()
        await cnn.close()

    workers = [worker(), worker()]
    t0 = time.time()
    await asyncio.wait(workers)
    assert time.time() - t0 < 0.8, "something broken in concurrency"
