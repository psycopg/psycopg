import sys
import time
import select  # noqa: used in pytest.mark.skipif
import socket

import pytest

import psycopg
from psycopg import waiting
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
    pytest.param("wait_poll", marks=pytest.mark.skipif("not hasattr(select, 'poll')")),
    pytest.param("wait_c", marks=pytest.mark.skipif("not psycopg._cmodule._psycopg")),
]

events = ["R", "W", "RW"]
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
@pytest.mark.parametrize("event", events)
@skip_if_not_linux
def test_wait_ready(waitfn, event):
    wait = getattr(waiting.Wait, event)
    ready = getattr(waiting.Ready, event)
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
@pytest.mark.timing
@pytest.mark.parametrize("waitfn", waitfns)
def test_wait_timeout(pgconn, waitfn):
    waitfn = getattr(waiting, waitfn)

    pgconn.send_query(b"select pg_sleep(0.5)")
    gen = generators.execute(pgconn)

    ts = [time.time()]

    def gen_wrapper():
        try:
            for x in gen:
                res = yield x
                ts.append(time.time())
                gen.send(res)
        except StopIteration as ex:
            return ex.value

    (res,) = waitfn(gen_wrapper(), pgconn.socket, timeout=0.1)
    assert res.status == ExecStatus.TUPLES_OK
    ds = [t1 - t0 for t0, t1 in zip(ts[:-1], ts[1:])]
    assert len(ds) >= 5
    for d in ds[:5]:
        assert d == pytest.approx(0.1, 0.05)


@pytest.mark.slow
@pytest.mark.skipif(
    "sys.platform == 'win32'", reason="win32 works ok, but FDs are mysterious"
)
@pytest.mark.parametrize("fname", waitfns)
def test_wait_large_fd(dsn, fname):
    waitfn = getattr(waiting, fname)

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
            if fname == "wait_select":
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


@pytest.mark.parametrize("timeout", timeouts)
@pytest.mark.anyio
async def test_wait_conn_async(dsn, timeout):
    gen = generators.connect(dsn)
    conn = await waiting.wait_conn_async(gen, **timeout)
    assert conn.status == ConnStatus.OK


@pytest.mark.anyio
async def test_wait_conn_async_bad(dsn):
    gen = generators.connect("dbname=nosuchdb")
    with pytest.raises(psycopg.OperationalError):
        await waiting.wait_conn_async(gen)


@pytest.mark.anyio
@pytest.mark.parametrize("event", events)
@skip_if_not_linux
async def test_wait_ready_async(event):
    wait = getattr(waiting.Wait, event)
    ready = getattr(waiting.Ready, event)

    def gen():
        r = yield wait
        return r

    with socket.socket() as s:
        r = await waiting.wait_async(gen(), s.fileno())
    assert r & ready


@pytest.mark.anyio
async def test_wait_async(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = await waiting.wait_async(gen, pgconn.socket)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.anyio
async def test_wait_async_bad(pgconn):
    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    socket = pgconn.socket
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        await waiting.wait_async(gen, socket)
