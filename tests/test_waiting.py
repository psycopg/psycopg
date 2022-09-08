import select  # noqa: used in pytest.mark.skipif
import socket
import sys

import pytest

import psycopg
from psycopg import waiting
from psycopg._anyio import waiting as waiting_anyio
from psycopg import generators
from psycopg.pq import ConnStatus, ExecStatus


skip_if_not_linux = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="non-Linux platform"
)

waitfns = [
    "wait",
    "wait_selector",
    pytest.param(
        "wait_select", marks=pytest.mark.skipif("not hasattr(select, 'select')")
    ),
    pytest.param(
        "wait_epoll", marks=pytest.mark.skipif("not hasattr(select, 'epoll')")
    ),
    pytest.param("wait_c", marks=pytest.mark.skipif("not psycopg._cmodule._psycopg")),
]

timeouts = [pytest.param({}, id="blank")]
timeouts += [pytest.param({"timeout": x}, id=str(x)) for x in [None, 0, 0.2, 10]]


@pytest.mark.parametrize("timeout", timeouts)
def test_wait_conn(dsn, timeout):
    gen = generators.connect(dsn)
    conn = waiting.wait_conn(gen, **timeout)
    assert conn.status == ConnStatus.OK


def test_wait_conn_bad(dsn):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg.OperationalError):
        waiting.wait_conn(gen)


@pytest.mark.parametrize("waitfn", waitfns)
@pytest.mark.parametrize("wait, ready", zip(waiting.Wait, waiting.Ready))
@skip_if_not_linux
def test_wait_ready(waitfn, wait, ready):
    waitfn = getattr(waiting, waitfn)

    def gen():
        r = yield wait
        return r

    with socket.socket() as s:
        r = waitfn(gen(), s.fileno())
    assert r & ready


@pytest.mark.parametrize("waitfn", waitfns)
@pytest.mark.parametrize("timeout", timeouts)
def test_wait(pgconn, waitfn, timeout):
    waitfn = getattr(waiting, waitfn)

    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = waitfn(gen, pgconn.socket, **timeout)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.parametrize("waitfn", waitfns)
def test_wait_bad(pgconn, waitfn):
    waitfn = getattr(waiting, waitfn)

    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        waitfn(gen, pgconn.socket)


@pytest.mark.slow
@pytest.mark.skipif(
    "sys.platform == 'win32'", reason="win32 works ok, but FDs are mysterious"
)
@pytest.mark.parametrize("waitfn", waitfns)
def test_wait_large_fd(dsn, waitfn):
    waitfn = getattr(waiting, waitfn)

    files = []
    try:
        try:
            for i in range(1100):
                files.append(open(__file__))
        except OSError:
            pytest.skip("can't open the number of files needed for the test")

        pgconn = psycopg.pq.PGconn.connect(dsn.encode())
        try:
            assert pgconn.socket > 1024
            pgconn.send_query(b"select 1")
            gen = generators.execute(pgconn)
            if waitfn is waiting.wait_select:
                with pytest.raises(ValueError):
                    waitfn(gen, pgconn.socket)
            else:
                (res,) = waitfn(gen, pgconn.socket)
                assert res.status == ExecStatus.TUPLES_OK
        finally:
            pgconn.finish()
    finally:
        for f in files:
            f.close()


@pytest.fixture
def wait_async(use_anyio):
    return waiting_anyio.wait if use_anyio else waiting.wait_asyncio


@pytest.fixture
def wait_conn_async(use_anyio):
    return waiting_anyio.wait_conn if use_anyio else waiting.wait_conn_asyncio


@pytest.mark.parametrize("timeout", timeouts)
async def test_wait_conn_async(dsn, timeout, wait_conn_async):
    gen = generators.connect(dsn)
    conn = await wait_conn_async(gen, **timeout)
    assert conn.status == ConnStatus.OK


async def test_wait_conn_async_bad(dsn, wait_conn_async):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg.OperationalError):
        await wait_conn_async(gen)


@pytest.mark.anyio
@pytest.mark.parametrize("wait, ready", zip(waiting.Wait, waiting.Ready))
@skip_if_not_linux
async def test_wait_ready_async(wait, ready, wait_async):
    def gen():
        r = yield wait
        return r

    with socket.socket() as s:
        r = await wait_async(gen(), s.fileno())
    assert r & ready


@pytest.mark.anyio
async def test_wait_async(pgconn, wait_async):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = await wait_async(gen, pgconn.socket)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.anyio
async def test_wait_async_bad(pgconn, wait_async):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    socket = pgconn.socket
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        await wait_async(gen, socket)
