import json
import asyncio
from typing import Any
from asyncio.queues import Queue

import pytest
from psycopg import pq, errors as e
from psycopg.rows import namedtuple_row
from psycopg._compat import create_task

from .test_cursor import testfeed

testfeed  # fixture

pytestmark = [pytest.mark.crdb, pytest.mark.asyncio]


@pytest.mark.slow
@pytest.mark.parametrize("fmt_out", pq.Format)
async def test_changefeed(aconn_cls, dsn, aconn, testfeed, fmt_out):
    await aconn.set_autocommit(True)
    q: "Queue[Any]" = Queue()

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

    t = create_task(worker())

    cur = aconn.cursor()
    await cur.execute(f"insert into {testfeed} (data) values ('hello') returning id")
    (key,) = await cur.fetchone()
    row = await asyncio.wait_for(q.get(), 1.0)
    assert row.table == testfeed
    assert json.loads(row.key) == [key]
    assert json.loads(row.value)["after"] == {"id": key, "data": "hello"}

    await cur.execute(f"delete from {testfeed} where id = %s", [key])
    row = await asyncio.wait_for(q.get(), 1.0)
    assert row.table == testfeed
    assert json.loads(row.key) == [key]
    assert json.loads(row.value)["after"] is None

    await cur.execute("select query_id from [show statements] where query !~ 'show'")
    (qid,) = await cur.fetchone()
    await cur.execute("cancel query %s", [qid])
    assert cur.statusmessage == "CANCEL QUERIES 1"

    assert await asyncio.wait_for(q.get(), 1.0) is None
    await asyncio.gather(t)
