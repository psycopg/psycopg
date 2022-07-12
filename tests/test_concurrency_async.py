import sys
import time
import signal
import asyncio
import subprocess as sp
from asyncio.queues import Queue
from typing import List, Tuple

import pytest

import psycopg
from psycopg import errors as e
from psycopg._compat import create_task

pytestmark = pytest.mark.asyncio


@pytest.mark.slow
async def test_commit_concurrency(aconn):
    # Check the condition reported in psycopg2#103
    # Because of bad status check, we commit even when a commit is already on
    # its way. We can detect this condition by the warnings.
    notices = Queue()  # type: ignore[var-annotated]
    aconn.add_notice_handler(lambda diag: notices.put_nowait(diag.message_primary))
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
async def test_concurrent_execution(aconn_cls, dsn):
    async def worker():
        cnn = await aconn_cls.connect(dsn)
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
@pytest.mark.crdb_skip("notify")
async def test_notifies(aconn_cls, aconn, dsn):
    nconn = await aconn_cls.connect(dsn, autocommit=True)
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

    ns: List[Tuple[psycopg.Notify, float]] = []
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


async def canceller(aconn, errors):
    try:
        await asyncio.sleep(0.5)
        aconn.cancel()
    except Exception as exc:
        errors.append(exc)


@pytest.mark.slow
@pytest.mark.crdb_skip("cancel")
async def test_cancel(aconn):
    async def worker():
        cur = aconn.cursor()
        with pytest.raises(e.QueryCanceled):
            await cur.execute("select pg_sleep(2)")

    errors: List[Exception] = []
    workers = [worker(), canceller(aconn, errors)]

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
@pytest.mark.crdb_skip("cancel")
async def test_cancel_stream(aconn):
    async def worker():
        cur = aconn.cursor()
        with pytest.raises(e.QueryCanceled):
            async for row in cur.stream("select pg_sleep(2)"):
                pass

    errors: List[Exception] = []
    workers = [worker(), canceller(aconn, errors)]

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
@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_identify_closure(aconn_cls, dsn):
    async def closer():
        await asyncio.sleep(0.2)
        await conn2.execute(
            "select pg_terminate_backend(%s)", [aconn.pgconn.backend_pid]
        )

    aconn = await aconn_cls.connect(dsn)
    conn2 = await aconn_cls.connect(dsn)
    try:
        t = create_task(closer())
        t0 = time.time()
        try:
            with pytest.raises(psycopg.OperationalError):
                await aconn.execute("select pg_sleep(1.0)")
            t1 = time.time()
            assert 0.2 < t1 - t0 < 0.4
        finally:
            await asyncio.gather(t)
    finally:
        await aconn.close()
        await conn2.close()


@pytest.mark.slow
@pytest.mark.subprocess
@pytest.mark.skipif(
    sys.platform == "win32", reason="don't know how to Ctrl-C on Windows"
)
@pytest.mark.crdb_skip("cancel")
async def test_ctrl_c(dsn):
    script = f"""\
import signal
import asyncio
import psycopg

async def main():
    ctrl_c = False
    loop = asyncio.get_event_loop()
    async with await psycopg.AsyncConnection.connect({dsn!r}) as conn:
        loop.add_signal_handler(signal.SIGINT, conn.cancel)
        cur = conn.cursor()
        try:
            await cur.execute("select pg_sleep(2)")
        except psycopg.errors.QueryCanceled:
            ctrl_c = True

        assert ctrl_c, "ctrl-c not received"
        assert (
            conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        ), f"transaction status: {{conn.info.transaction_status!r}}"

        await conn.rollback()
        assert (
            conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        ), f"transaction status: {{conn.info.transaction_status!r}}"

        await cur.execute("select 1")
        assert (await cur.fetchone()) == (1,)

asyncio.run(main())
"""
    if sys.platform == "win32":
        creationflags = sp.CREATE_NEW_PROCESS_GROUP
        sig = signal.CTRL_C_EVENT
    else:
        creationflags = 0
        sig = signal.SIGINT

    proc = sp.Popen([sys.executable, "-s", "-c", script], creationflags=creationflags)
    with pytest.raises(sp.TimeoutExpired):
        outs, errs = proc.communicate(timeout=1)

    proc.send_signal(sig)
    proc.communicate()
    assert proc.returncode == 0
