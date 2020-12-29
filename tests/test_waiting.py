import select

import pytest

import psycopg3
from psycopg3 import waiting
from psycopg3 import generators
from psycopg3.pq import ConnStatus, ExecStatus


skip_no_epoll = pytest.mark.skipif(
    not hasattr(select, "epoll"), reason="epoll not available"
)

timeouts = [
    {},
    {"timeout": None},
    {"timeout": 0},
    {"timeout": 0.1},
    {"timeout": 10},
]


@pytest.mark.parametrize("timeout", timeouts)
def test_wait_conn(dsn, timeout):
    gen = generators.connect(dsn)
    conn = waiting.wait_conn(gen, **timeout)
    assert conn.status == ConnStatus.OK


def test_wait_conn_bad(dsn):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg3.OperationalError):
        waiting.wait_conn(gen)


@pytest.mark.parametrize("timeout", timeouts)
def test_wait(pgconn, timeout):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = waiting.wait(gen, pgconn.socket, **timeout)
    assert res.status == ExecStatus.TUPLES_OK


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
    with pytest.raises(psycopg3.OperationalError):
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


@pytest.mark.asyncio
async def test_wait_conn_async(dsn):
    gen = generators.connect(dsn)
    conn = await waiting.wait_conn_async(gen)
    assert conn.status == ConnStatus.OK


@pytest.mark.asyncio
async def test_wait_conn_async_bad(dsn):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg3.OperationalError):
        await waiting.wait_conn_async(gen)


@pytest.mark.asyncio
async def test_wait_async(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = await waiting.wait_async(gen, pgconn.socket)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.asyncio
async def test_wait_async_bad(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    socket = pgconn.socket
    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        await waiting.wait_async(gen, socket)
