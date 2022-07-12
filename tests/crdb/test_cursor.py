import json
import threading
from uuid import uuid4
from queue import Queue
from typing import Any

import pytest
from psycopg import pq, errors as e
from psycopg.rows import namedtuple_row

pytestmark = pytest.mark.crdb


@pytest.fixture
def testfeed(svcconn):
    name = f"test_feed_{str(uuid4()).replace('-', '')}"
    svcconn.execute("set cluster setting kv.rangefeed.enabled to true")
    svcconn.execute(f"create table {name} (id serial primary key, data text)")
    yield name
    svcconn.execute(f"drop table {name}")


@pytest.mark.slow
@pytest.mark.parametrize("fmt_out", pq.Format)
def test_changefeed(conn_cls, dsn, conn, testfeed, fmt_out):
    conn.autocommit = True
    q: "Queue[Any]" = Queue()

    def worker():
        try:
            with conn_cls.connect(dsn, autocommit=True) as conn:
                cur = conn.cursor(binary=fmt_out, row_factory=namedtuple_row)
                try:
                    for row in cur.stream(f"experimental changefeed for {testfeed}"):
                        q.put(row)
                except e.QueryCanceled:
                    assert conn.info.transaction_status == conn.TransactionStatus.IDLE
                    q.put(None)
        except Exception as ex:
            q.put(ex)

    t = threading.Thread(target=worker)
    t.start()

    cur = conn.cursor()
    cur.execute(f"insert into {testfeed} (data) values ('hello') returning id")
    (key,) = cur.fetchone()
    row = q.get(timeout=1)
    assert row.table == testfeed
    assert json.loads(row.key) == [key]
    assert json.loads(row.value)["after"] == {"id": key, "data": "hello"}

    cur.execute(f"delete from {testfeed} where id = %s", [key])
    row = q.get(timeout=1)
    assert row.table == testfeed
    assert json.loads(row.key) == [key]
    assert json.loads(row.value)["after"] is None

    cur.execute("select query_id from [show statements] where query !~ 'show'")
    (qid,) = cur.fetchone()
    cur.execute("cancel query %s", [qid])
    assert cur.statusmessage == "CANCEL QUERIES 1"

    assert q.get(timeout=1) is None
    t.join()
