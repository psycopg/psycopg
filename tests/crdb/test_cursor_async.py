from __future__ import annotations

import json
from uuid import uuid4

import pytest
from psycopg import pq, errors as e
from psycopg.rows import namedtuple_row
from ..acompat import AQueue, spawn, gather

pytestmark = [pytest.mark.crdb]
if True:  # ASYNC
    pytestmark.append(pytest.mark.anyio)


@pytest.fixture
def testfeed(svcconn):
    name = f"test_feed_{str(uuid4()).replace('-', '')}"
    svcconn.execute("set cluster setting kv.rangefeed.enabled to true")
    svcconn.execute(f"create table {name} (id serial primary key, data text)")
    yield name
    svcconn.execute(f"drop table {name}")


@pytest.mark.slow
@pytest.mark.parametrize("fmt_out", pq.Format)
async def test_changefeed(aconn_cls, dsn, aconn, testfeed, fmt_out):
    await aconn.set_autocommit(True)
    q = AQueue()

    async def worker():
        try:
            async with await aconn_cls.connect(dsn, autocommit=True) as conn:
                cur = conn.cursor(binary=fmt_out, row_factory=namedtuple_row)
                try:
                    async for row in cur.stream(
                        f"experimental changefeed for {testfeed}"
                    ):
                        q.put_nowait(row)
                except e.QueryCanceled:
                    assert conn.info.transaction_status == conn.TransactionStatus.IDLE
                    q.put_nowait(None)
        except Exception as ex:
            q.put_nowait(ex)

    t = spawn(worker)

    cur = aconn.cursor()
    await cur.execute(f"insert into {testfeed} (data) values ('hello') returning id")
    (key,) = await cur.fetchone()
    row = await q.get()
    assert row.table == testfeed
    assert json.loads(row.key) == [key]
    assert json.loads(row.value)["after"] == {"id": key, "data": "hello"}

    await cur.execute(f"delete from {testfeed} where id = %s", [key])
    row = await q.get()
    assert row.table == testfeed
    assert json.loads(row.key) == [key]
    assert json.loads(row.value)["after"] is None

    await cur.execute("select query_id from [show statements] where query !~ 'show'")
    (qid,) = await cur.fetchone()
    await cur.execute("cancel query %s", [qid])
    assert cur.statusmessage == "CANCEL QUERIES 1"

    # We often find the record with {"after": null} at least another time
    # in the queue. Let's tolerate an extra one.
    for i in range(2):
        row = await q.get()
        if row is None:
            break
        assert json.loads(row.value)["after"] is None, json
    else:
        pytest.fail("keep on receiving messages")

    await gather(t)
