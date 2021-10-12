import select
import time

import pytest

import psycopg
from psycopg import waiting
from psycopg import generators
from psycopg.pq import ConnStatus, ExecStatus


hasepoll = hasattr(select, "epoll")
skip_no_epoll = pytest.mark.skipif(not hasepoll, reason="epoll not available")

timeouts = [
    {},
    {"timeout": None},
    {"timeout": 0},
    {"timeout": 0.2},
    {"timeout": 10},
]


def rwgen(pgconn):
    """Generator waiting on RW and returning received ready event."""
    r = yield waiting.Wait.RW
    return r


@pytest.mark.parametrize("timeout", timeouts)
def test_wait_conn(dsn, timeout):
    gen = generators.connect(dsn)
    conn = waiting.wait_conn(gen, **timeout)
    assert conn.status == ConnStatus.OK


def test_wait_conn_bad(dsn):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg.OperationalError):
        waiting.wait_conn(gen)


@pytest.mark.parametrize("timeout", timeouts)
def test_wait(pgconn, timeout):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = waiting.wait(gen, pgconn.socket, **timeout)
    assert res.status == ExecStatus.TUPLES_OK


waits_and_ids = [
    (waiting.wait, "wait"),
    (waiting.wait_selector, "wait_selector"),
]
if hasepoll:
    waits_and_ids.append((waiting.wait_epoll, "wait_epoll"))

waits, wids = list(zip(*waits_and_ids))


@pytest.mark.libpq(">= 14")
@pytest.mark.slow
@pytest.mark.parametrize("wait", waits, ids=wids)
def test_pipeline_wait(pgconn, wait):
    # Issue a "large" query in a pipeline-mode connection, sleep a bit, and
    # check that the connection is read- and write-ready.
    pgconn.enter_pipeline_mode()
    pgconn.send_query(f"select '{'x' * 10000}'".encode())
    wait(generators.send(pgconn), pgconn.socket)
    time.sleep(0.5)
    ready = wait(rwgen(pgconn), pgconn.socket)
    assert ready == waiting.Ready.RW
    pgconn.pipeline_sync()
    (rto,) = wait(generators.fetch_many(pgconn), pgconn.socket)
    assert rto.status == ExecStatus.TUPLES_OK
    (rs,) = wait(generators.fetch_many(pgconn), pgconn.socket)
    assert rs.status == ExecStatus.PIPELINE_SYNC
    pgconn.exit_pipeline_mode()


@pytest.mark.parametrize("timeout", timeouts)
def test_wait_selector(pgconn, timeout):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = waiting.wait_selector(gen, pgconn.socket, **timeout)
    assert res.status == ExecStatus.TUPLES_OK


def test_wait_selector_bad(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        waiting.wait_selector(gen, pgconn.socket)


@skip_no_epoll
@pytest.mark.parametrize("timeout", timeouts)
def test_wait_epoll(pgconn, timeout):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = waiting.wait_epoll(gen, pgconn.socket, **timeout)
    assert res.status == ExecStatus.TUPLES_OK


@skip_no_epoll
def test_wait_epoll_bad(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = waiting.wait_epoll(gen, pgconn.socket)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.parametrize("timeout", timeouts)
@pytest.mark.asyncio
async def test_wait_conn_async(dsn, timeout):
    gen = generators.connect(dsn)
    conn = await waiting.wait_conn_async(gen, **timeout)
    assert conn.status == ConnStatus.OK


@pytest.mark.asyncio
async def test_wait_conn_async_bad(dsn):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg.OperationalError):
        await waiting.wait_conn_async(gen)


@pytest.mark.asyncio
async def test_wait_async(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = await waiting.wait_async(gen, pgconn.socket)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.libpq(">= 14")
@pytest.mark.slow
@pytest.mark.asyncio
async def test_pipeline_wait_async(pgconn):
    # Issue a "large" query in a pipeline-mode connection, sleep a bit, and
    # check that the connection is read- and write-ready.
    pgconn.enter_pipeline_mode()
    pgconn.send_query(f"select '{'x' * 10000}'".encode())
    await waiting.wait_async(generators.send(pgconn), pgconn.socket)
    time.sleep(0.5)
    ready = await waiting.wait_async(rwgen(pgconn), pgconn.socket)
    assert ready == waiting.Ready.RW
    pgconn.pipeline_sync()
    (rto,) = await waiting.wait_async(
        generators.fetch_many(pgconn), pgconn.socket
    )
    assert rto.status == ExecStatus.TUPLES_OK
    (rs,) = await waiting.wait_async(
        generators.fetch_many(pgconn), pgconn.socket
    )
    assert rs.status == ExecStatus.PIPELINE_SYNC
    pgconn.exit_pipeline_mode()


@pytest.mark.asyncio
async def test_wait_async_bad(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    socket = pgconn.socket
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        await waiting.wait_async(gen, socket)
