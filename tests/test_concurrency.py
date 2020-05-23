"""
Tests dealing with concurrency issues.
"""

import time
import queue
import pytest
import threading

import psycopg3


@pytest.mark.slow
def test_concurrent_execution(dsn):
    def worker():
        cnn = psycopg3.connect(dsn)
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
    notices = queue.Queue()
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

    assert notices.empty(), "%d notices raised" % notices.qsize()
