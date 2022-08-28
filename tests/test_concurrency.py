"""
Tests dealing with concurrency issues.
"""

import os
import sys
import time
import queue
import signal
import threading
import multiprocessing
import subprocess as sp
from typing import List

import pytest

import psycopg
from psycopg import errors as e


@pytest.mark.slow
def test_concurrent_execution(conn_cls, dsn):
    def worker():
        cnn = conn_cls.connect(dsn)
        cur = cnn.cursor()
        cur.execute("select pg_sleep(0.5)")
        cur.close()
        cnn.close()

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t0 = time.time()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert time.time() - t0 < 0.8, "something broken in concurrency"


@pytest.mark.slow
def test_commit_concurrency(conn):
    # Check the condition reported in psycopg2#103
    # Because of bad status check, we commit even when a commit is already on
    # its way. We can detect this condition by the warnings.
    notices = queue.Queue()  # type: ignore[var-annotated]
    conn.add_notice_handler(lambda diag: notices.put(diag.message_primary))
    stop = False

    def committer():
        nonlocal stop
        while not stop:
            conn.commit()

    cur = conn.cursor()
    t1 = threading.Thread(target=committer)
    t1.start()
    for i in range(1000):
        cur.execute("select %s;", (i,))
        conn.commit()

    # Stop the committer thread
    stop = True
    t1.join()

    assert notices.empty(), "%d notices raised" % notices.qsize()


@pytest.mark.slow
@pytest.mark.subprocess
def test_multiprocess_close(dsn, tmpdir):
    # Check the problem reported in psycopg2#829
    # Subprocess gcs the copy of the fd after fork so it closes connection.
    module = f"""\
import time
import psycopg

def thread():
    conn = psycopg.connect({dsn!r})
    curs = conn.cursor()
    for i in range(10):
        curs.execute("select 1")
        time.sleep(0.1)

def process():
    time.sleep(0.2)
"""

    script = """\
import time
import threading
import multiprocessing
import mptest

t = threading.Thread(target=mptest.thread, name='mythread')
t.start()
time.sleep(0.2)
multiprocessing.Process(target=mptest.process, name='myprocess').start()
t.join()
"""

    with (tmpdir / "mptest.py").open("w") as f:
        f.write(module)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(tmpdir + os.pathsep + env.get("PYTHONPATH", ""))
    out = sp.check_output(
        [sys.executable, "-c", script], stderr=sp.STDOUT, env=env
    ).decode("utf8", "replace")
    assert out == "", out.strip().splitlines()[-1]


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("notify")
def test_notifies(conn_cls, conn, dsn):
    nconn = conn_cls.connect(dsn, autocommit=True)
    npid = nconn.pgconn.backend_pid

    def notifier():
        time.sleep(0.25)
        nconn.cursor().execute("notify foo, '1'")
        time.sleep(0.25)
        nconn.cursor().execute("notify foo, '2'")
        nconn.close()

    conn.autocommit = True
    conn.cursor().execute("listen foo")

    t0 = time.time()
    t = threading.Thread(target=notifier)
    t.start()

    ns = []
    gen = conn.notifies()
    for n in gen:
        ns.append((n, time.time()))
        if len(ns) >= 2:
            gen.close()

    assert len(ns) == 2

    n, t1 = ns[0]
    assert isinstance(n, psycopg.Notify)
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "1"
    assert t1 - t0 == pytest.approx(0.25, abs=0.05)

    n, t1 = ns[1]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "2"
    assert t1 - t0 == pytest.approx(0.5, abs=0.05)

    t.join()


def canceller(conn, errors):
    try:
        time.sleep(0.5)
        conn.cancel()
    except Exception as exc:
        errors.append(exc)


@pytest.mark.slow
@pytest.mark.crdb_skip("cancel")
def test_cancel(conn):
    errors: List[Exception] = []

    cur = conn.cursor()
    t = threading.Thread(target=canceller, args=(conn, errors))
    t0 = time.time()
    t.start()

    with pytest.raises(e.QueryCanceled):
        cur.execute("select pg_sleep(2)")

    t1 = time.time()
    assert not errors
    assert 0.0 < t1 - t0 < 1.0

    # still working
    conn.rollback()
    assert cur.execute("select 1").fetchone()[0] == 1

    t.join()


@pytest.mark.slow
@pytest.mark.crdb_skip("cancel")
def test_cancel_stream(conn):
    errors: List[Exception] = []

    cur = conn.cursor()
    t = threading.Thread(target=canceller, args=(conn, errors))
    t0 = time.time()
    t.start()

    with pytest.raises(e.QueryCanceled):
        for row in cur.stream("select pg_sleep(2)"):
            pass

    t1 = time.time()
    assert not errors
    assert 0.0 < t1 - t0 < 1.0

    # still working
    conn.rollback()
    assert cur.execute("select 1").fetchone()[0] == 1

    t.join()


@pytest.mark.crdb_skip("pg_terminate_backend")
@pytest.mark.slow
def test_identify_closure(conn_cls, dsn):
    def closer():
        time.sleep(0.2)
        conn2.execute("select pg_terminate_backend(%s)", [conn.pgconn.backend_pid])

    conn = conn_cls.connect(dsn)
    conn2 = conn_cls.connect(dsn)
    try:
        t = threading.Thread(target=closer)
        t.start()
        t0 = time.time()
        try:
            with pytest.raises(psycopg.OperationalError):
                conn.execute("select pg_sleep(1.0)")
            t1 = time.time()
            assert 0.2 < t1 - t0 < 0.4
        finally:
            t.join()
    finally:
        conn.close()
        conn2.close()


@pytest.mark.slow
@pytest.mark.subprocess
@pytest.mark.skipif(
    sys.platform == "win32", reason="don't know how to Ctrl-C on Windows"
)
@pytest.mark.crdb_skip("cancel")
def test_ctrl_c(dsn):
    if sys.platform == "win32":
        sig = int(signal.CTRL_C_EVENT)
        # Or pytest will receive the Ctrl-C too
        creationflags = sp.CREATE_NEW_PROCESS_GROUP
    else:
        sig = int(signal.SIGINT)
        creationflags = 0

    script = f"""\
import os
import time
import psycopg
from threading import Thread

def tired_of_life():
    time.sleep(1)
    os.kill(os.getpid(), {sig!r})

t = Thread(target=tired_of_life, daemon=True)
t.start()

with psycopg.connect({dsn!r}) as conn:
    cur = conn.cursor()
    ctrl_c = False
    try:
        cur.execute("select pg_sleep(2)")
    except KeyboardInterrupt:
        ctrl_c = True

    assert ctrl_c, "ctrl-c not received"
    assert (
        conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
    ), f"transaction status: {{conn.info.transaction_status!r}}"

    conn.rollback()
    assert (
        conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    ), f"transaction status: {{conn.info.transaction_status!r}}"

    cur.execute("select 1")
    assert cur.fetchone() == (1,)
"""
    t0 = time.time()
    proc = sp.Popen([sys.executable, "-s", "-c", script], creationflags=creationflags)
    proc.communicate()
    t = time.time() - t0
    assert proc.returncode == 0
    assert 1 < t < 2


@pytest.mark.slow
@pytest.mark.subprocess
@pytest.mark.skipif(
    multiprocessing.get_all_start_methods()[0] != "fork",
    reason="problematic behavior only exhibited via fork",
)
def test_segfault_on_fork_close(dsn):
    # https://github.com/psycopg/psycopg/issues/300
    script = f"""\
import gc
import psycopg
from multiprocessing import Pool

def test(arg):
    conn1 = psycopg.connect({dsn!r})
    conn1.close()
    conn1 = None
    gc.collect()
    return 1

if __name__ == '__main__':
    conn = psycopg.connect({dsn!r})
    with Pool(2) as p:
        pool_result = p.map_async(test, [1, 2])
        pool_result.wait(timeout=5)
        if pool_result.ready():
            print(pool_result.get(timeout=1))
"""
    env = dict(os.environ)
    env["PYTHONFAULTHANDLER"] = "1"
    out = sp.check_output([sys.executable, "-s", "-c", script], env=env)
    assert out.decode().rstrip() == "[1, 1]"
