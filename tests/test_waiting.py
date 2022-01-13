import select
import socket
import sys

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


skip_if_not_linux = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="non-Linux platform"
)


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


@pytest.mark.parametrize("waitfn", waits, ids=wids)
@pytest.mark.parametrize("wait, ready", zip(waiting.Wait, waiting.Ready))
@skip_if_not_linux
def test_wait_ready(waitfn, wait, ready):
    def gen():
        r = yield wait
        return r

    with socket.socket() as s:
        r = waitfn(gen(), s.fileno())
    assert r & ready


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


@pytest.mark.asyncio
@pytest.mark.parametrize("wait, ready", zip(waiting.Wait, waiting.Ready))
@skip_if_not_linux
async def test_wait_ready_async(wait, ready):
    def gen():
        r = yield wait
        return r

    with socket.socket() as s:
        r = await waiting.wait_async(gen(), s.fileno())
    assert r & ready


@pytest.mark.asyncio
async def test_wait_async_bad(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    socket = pgconn.socket
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        await waiting.wait_async(gen, socket)
