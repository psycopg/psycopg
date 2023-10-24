import sys
import time
import signal
import asyncio
import threading
import subprocess as sp
from asyncio import create_task
from asyncio.queues import Queue
from typing import List

import pytest

import psycopg
from psycopg import errors as e


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
@pytest.mark.timing
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
@pytest.mark.timing
@pytest.mark.crdb_skip("cancel")
def test_ctrl_c_handler(dsn):
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


@pytest.mark.slow
@pytest.mark.subprocess
@pytest.mark.skipif(
    sys.platform == "win32", reason="don't know how to Ctrl-C on Windows"
)
@pytest.mark.crdb("skip")
def test_ctrl_c(conn, dsn):
    # https://github.com/psycopg/psycopg/issues/543
    conn.autocommit = True

    APPNAME = "test_ctrl_c"
    script = f"""\
import asyncio
import psycopg

async def main():
    async with await psycopg.AsyncConnection.connect(
        {dsn!r}, application_name={APPNAME!r}
    ) as conn:
        await conn.execute("select pg_sleep(5)")

asyncio.run(main())
"""
    if sys.platform == "win32":
        creationflags = sp.CREATE_NEW_PROCESS_GROUP
        sig = signal.CTRL_C_EVENT
    else:
        creationflags = 0
        sig = signal.SIGINT

    proc = None

    def run_process():
        nonlocal proc
        proc = sp.Popen(
            [sys.executable, "-s", "-c", script],
            creationflags=creationflags,
            stderr=sp.PIPE,
        )
        proc.communicate()

    t = threading.Thread(target=run_process)
    t.start()

    for i in range(20):
        cur = conn.execute(
            "select pid from pg_stat_activity where application_name = %s", (APPNAME,)
        )
        rec = cur.fetchone()
        if rec:
            pid = rec[0]
            break
        time.sleep(0.1)
    else:
        assert False, "process didn't start?"

    t0 = time.time()
    assert proc
    proc.send_signal(sig)
    proc.wait()

    for i in range(20):
        cur = conn.execute("select 1 from pg_stat_activity where pid = %s", (pid,))
        if not cur.fetchone():
            break
        time.sleep(0.1)
    else:
        assert False, "process didn't stop?"

    t1 = time.time()
    assert t1 - t0 < 1.0


@pytest.mark.slow
@pytest.mark.subprocess
@pytest.mark.parametrize("itimername, signame", [("ITIMER_REAL", "SIGALRM")])
def test_eintr(dsn, itimername, signame):
    try:
        itimer = int(getattr(signal, itimername))
        sig = int(getattr(signal, signame))
    except AttributeError:
        pytest.skip(f"unknown interrupt timer: {itimername}")

    script = f"""\
import signal
import asyncio
import psycopg

def signal_handler(signum, frame):
    assert signum == {sig!r}

# Install a handler for the signal
signal.signal({sig!r}, signal_handler)

# Restart system calls interrupted by the signal
signal.siginterrupt({sig!r}, False)


async def main():
    async with await psycopg.AsyncConnection.connect({dsn!r}) as conn:
        # Fire an interrupt signal every 0.25 seconds
        signal.setitimer({itimer!r}, 0.25, 0.25)

        cur = conn.cursor()
        await cur.execute("select 'ok' from pg_sleep(0.5)")
        print((await cur.fetchone())[0])

asyncio.run(main())
"""
    cp = sp.run(
        [sys.executable, "-s"], input=script, text=True, stdout=sp.PIPE, stderr=sp.PIPE
    )
    assert "InterruptedError" not in cp.stderr
    assert (
        cp.returncode == 0
    ), f"script terminated with {signal.Signals(abs(cp.returncode)).name}"
    assert cp.stdout.rstrip() == "ok"


@pytest.mark.slow
@pytest.mark.crdb("skip")
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Fails with: An operation was attempted on something that is not a socket",
)
async def test_concurrent_close(dsn, aconn):
    # Test issue #608: concurrent closing shouldn't hang the server
    # (although, at the moment, it doesn't cancel a running query).
    pid = aconn.info.backend_pid
    await aconn.set_autocommit(True)

    async def worker():
        try:
            await aconn.execute("select pg_sleep(3)")
        except psycopg.OperationalError:
            pass  # expected

    t0 = time.time()
    task = create_task(worker())
    await asyncio.sleep(0.5)

    async def test():
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn1:
            cur = await conn1.execute(
                "select query from pg_stat_activity where pid = %s", [pid]
            )
            assert await cur.fetchone()
            await aconn.close()
            await asyncio.gather(task)
            await asyncio.sleep(0.5)
            t = time.time()
            # TODO: this statement can pass only if we send cancel on close
            # but because async cancelling is not available in the libpq,
            # we'd rather not do it.
            # cur = await conn1.execute(
            #     "select query from pg_stat_activity where pid = %s", [pid]
            # )
            # assert not await cur.fetchone()
            assert t - t0 < 2

    await asyncio.wait_for(test(), 5.0)


@pytest.mark.parametrize("what", ["commit", "rollback", "error"])
async def test_transaction_concurrency(aconn, what):
    await aconn.set_autocommit(True)

    evs = [asyncio.Event() for i in range(3)]

    async def worker(unlock, wait_on):
        with pytest.raises(e.ProgrammingError) as ex:
            async with aconn.transaction():
                unlock.set()
                await wait_on.wait()
                await aconn.execute("select 1")

                if what == "error":
                    1 / 0
                elif what == "rollback":
                    raise psycopg.Rollback()
                else:
                    assert what == "commit"

        if what == "error":
            assert "transaction rollback" in str(ex.value)
            assert isinstance(ex.value.__context__, ZeroDivisionError)
        elif what == "rollback":
            assert "transaction rollback" in str(ex.value)
            assert isinstance(ex.value.__context__, psycopg.Rollback)
        else:
            assert "transaction commit" in str(ex.value)

    # Start a first transaction in a task
    t1 = create_task(worker(unlock=evs[0], wait_on=evs[1]))
    await evs[0].wait()

    # Start a nested transaction in a task
    t2 = create_task(worker(unlock=evs[1], wait_on=evs[2]))

    # Terminate the first transaction before the second does
    await asyncio.gather(t1)
    evs[2].set()
    await asyncio.gather(t2)
