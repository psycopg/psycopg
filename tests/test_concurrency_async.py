import time
import pytest
import asyncio
from asyncio.queues import Queue

import psycopg
from psycopg.compat import create_task

pytestmark = pytest.mark.asyncio


@pytest.mark.slow
@pytest.mark.skip
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
            await asyncio.sleep(0)  # Allow the other worker to work

    async def runner():
        nonlocal stop
        cur = aconn.cursor()
        for i in range(1000):
            await cur.execute("select %s;", (i,))
            await aconn.commit()

        # Stop the committer thread
        stop = True

    await asyncio.gather(committer(), runner())
    assert notices.empty(), "%d notices raised" % notices.qsize()


@pytest.mark.slow
async def test_concurrent_execution(dsn):
    async def worker():
        cnn = await psycopg.AsyncConnection.connect(dsn)
        cur = cnn.cursor()
        await cur.execute("select pg_sleep(0.5)")
        await cur.close()
        await cnn.close()

    workers = [worker(), worker()]
    t0 = time.time()
    await asyncio.gather(*workers)
    assert time.time() - t0 < 0.8, "something broken in concurrency"


@pytest.mark.slow
@pytest.mark.timing
async def test_notifies(aconn, dsn):
    nconn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    npid = nconn.pgconn.backend_pid

    async def notifier():
        cur = nconn.cursor()
        await asyncio.sleep(0.25)
        await cur.execute("notify foo, '1'")
        await asyncio.sleep(0.25)
        await cur.execute("notify foo, '2'")
        await nconn.close()

    async def receiver():
        await aconn.set_autocommit(True)
        cur = aconn.cursor()
        await cur.execute("listen foo")
        gen = aconn.notifies()
        async for n in gen:
            ns.append((n, time.time()))
            if len(ns) >= 2:
                await gen.aclose()

    ns = []
    t0 = time.time()
    workers = [notifier(), receiver()]
    await asyncio.gather(*workers)
    assert len(ns) == 2

    n, t1 = ns[0]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "1"
    assert t1 - t0 == pytest.approx(0.25, abs=0.05)

    n, t1 = ns[1]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "2"
    assert t1 - t0 == pytest.approx(0.5, abs=0.05)


@pytest.mark.slow
async def test_cancel(aconn):

    errors = []

    async def canceller():
        try:
            await asyncio.sleep(0.5)
            aconn.cancel()
        except Exception as exc:
            errors.append(exc)

    async def worker():
        cur = aconn.cursor()
        with pytest.raises(psycopg.DatabaseError):
            await cur.execute("select pg_sleep(2)")

    workers = [worker(), canceller()]

    t0 = time.time()
    await asyncio.gather(*workers)

    t1 = time.time()
    assert not errors
    assert 0.0 < t1 - t0 < 1.0

    # still working
    await aconn.rollback()
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)


@pytest.mark.slow
async def test_identify_closure(dsn, retries):
    async def closer():
        await asyncio.sleep(0.3)
        await conn2.execute(
            "select pg_terminate_backend(%s)", [aconn.pgconn.backend_pid]
        )

    async for retry in retries:
        with retry:
            aconn = await psycopg.AsyncConnection.connect(dsn)
            conn2 = await psycopg.AsyncConnection.connect(dsn)

            t0 = time.time()
            ev = asyncio.Event()
            loop = asyncio.get_event_loop()
            loop.add_reader(aconn.fileno(), ev.set)
            create_task(closer())

            await asyncio.wait_for(ev.wait(), 1.0)
            with pytest.raises(psycopg.OperationalError):
                await aconn.execute("select 1")
            t1 = time.time()
            assert 0.3 < t1 - t0 < 0.6
