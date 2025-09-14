import sys
import time
import select  # noqa: used in pytest.mark.skipif
import socket

import pytest

import psycopg
from psycopg import generators, waiting
from psycopg.pq import ConnStatus, ExecStatus
from psycopg.conninfo import make_conninfo

from .acompat import AEvent, asleep, gather, spawn

skip_if_not_linux = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="non-Linux platform"
)

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]
    waitfns = [
        "wait_async",
    ]
else:
    waitfns = [
        "wait",
        "wait_selector",
        pytest.param(
            "wait_select", marks=pytest.mark.skipif("not hasattr(select, 'select')")
        ),
        pytest.param(
            "wait_epoll", marks=pytest.mark.skipif("not hasattr(select, 'epoll')")
        ),
        pytest.param(
            "wait_poll", marks=pytest.mark.skipif("not hasattr(select, 'poll')")
        ),
        pytest.param(
            "wait_c", marks=pytest.mark.skipif("not psycopg._cmodule._psycopg")
        ),
    ]


events = ["R", "W", "RW"]
intervals = [0, 0.2, 2]


def tgen(wait):
    """A generator waiting for a specific event and returning what waited on."""
    r = yield wait
    return r


@pytest.mark.parametrize("interval", intervals)
async def test_wait_conn(dsn, interval):
    gen = generators.connect(dsn)
    conn = await waiting.wait_conn_async(gen, interval)
    assert conn.status == ConnStatus.OK


@pytest.mark.crdb("skip", reason="can connect to any db name")
async def test_wait_conn_bad(dsn):
    gen = generators.connect(make_conninfo(dsn, dbname="nosuchdb"))
    with pytest.raises(psycopg.OperationalError):
        await waiting.wait_conn_async(gen)


@pytest.mark.slow
@pytest.mark.skipif("sys.platform != 'linux'")
@pytest.mark.parametrize("interval", [i for i in intervals if i > 0])
@pytest.mark.parametrize("ready", ["R", "NONE"])
@pytest.mark.parametrize("event", ["R", "RW"])
@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_r(waitfn, event, ready, interval, request):
    # Test that wait functions handle waiting and returning state correctly
    # This test doesn't work on macOS for some internal race condition betwwn
    # listen/connect/accept.
    waitfn = getattr(waiting, waitfn)
    wait = getattr(waiting.Wait, event)
    ready = getattr(waiting.Ready, ready)
    delay = interval / 2

    port = None
    ev = AEvent()

    async def writer():
        # Wake up the socket, or let it time out, according to the expected `ready`.
        await ev.wait()
        assert port
        await asleep(delay)
        if ready == waiting.Ready.R:
            with socket.create_connection(("127.0.0.1", port)):
                pass

    tasks = [spawn(writer)]

    try:
        with socket.socket() as s:
            # Make a listening socket
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            s.listen(10)
            s.setblocking(False)
            # Let the writer start
            ev.set()
            # Wait for socket ready to read or timing out
            t0 = time.time()
            r = await waitfn(tgen(wait), s.fileno(), interval)
            dt = time.time() - t0
            # Check timing and received waiting state
            assert r == ready
            if check_timing(request):
                exptime = {waiting.Ready.R: delay, waiting.Ready.NONE: interval}[ready]
                assert exptime <= dt < (exptime * 1.2)
    finally:
        await gather(*tasks)


@pytest.mark.slow
@pytest.mark.skipif("sys.platform == 'linux'")
@pytest.mark.parametrize("interval", [2])
@pytest.mark.parametrize("ready", ["R", "NONE"])
@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_r_no_linux(waitfn, ready, interval, request):
    # A version of test_wait_r that works on macOS too, but doesn't allow to
    # test for the RW wait (because it seems that the sockets returned by
    # socketpair() is immediately w-ready, including the r one.
    waitfn = getattr(waiting, waitfn)
    wait = waiting.Wait.R
    ready = getattr(waiting.Ready, ready)
    delay = interval / 2

    ev = AEvent()

    async def writer():
        # Wake up the socket, or let it time out, according to the expected `ready`.
        await ev.wait()
        await asleep(delay)
        if ready == waiting.Ready.R:
            for att in range(10):
                try:
                    ws.sendall(b"hi")
                    ws.close()
                except Exception as ex:
                    the_ex = ex
                    await asleep(0.1)
                else:
                    break
            else:
                pytest.fail(
                    f"failed after many attempts. Socket: {ws}, Last error: {the_ex}"
                )

    tasks = [spawn(writer)]
    try:
        rs, ws = socket.socketpair()
        rs.setblocking(False)
        ws.setblocking(False)
        # Let the writer start
        ev.set()
        # Wait for socket ready to read or timing out
        t0 = time.time()
        r = await waitfn(tgen(wait), rs.fileno(), interval)
        dt = time.time() - t0
        # Check timing and received waiting state
        assert r == ready
        if check_timing(request):
            exptime = {waiting.Ready.R: delay, waiting.Ready.NONE: interval}[ready]
            assert exptime <= dt < (exptime * 1.2)
    finally:
        await gather(*tasks)
        rs.close()
        ws.close()


@pytest.mark.parametrize("ready", ["R", "NONE"])
@pytest.mark.parametrize("event", ["R", "RW"])
@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_r_nowait(waitfn, event, ready, request):
    # Test that wait functions handle a poll when called with no timeout
    waitfn = getattr(waiting, waitfn)
    wait = getattr(waiting.Wait, event)
    ready = getattr(waiting.Ready, ready)

    port = None
    ev1 = AEvent()
    ev2 = AEvent()
    ev3 = AEvent()

    async def writer():
        await ev1.wait()
        assert port
        if ready == waiting.Ready.R:
            with socket.create_connection(("127.0.0.1", port)):
                ev2.set()
        else:
            ev2.set()

    async def unblocker():
        # If test doesn't pass, wake up the socket again to avoid hanging forever
        if not await ev3.wait_timeout(0.5):
            assert port
            with socket.create_connection(("127.0.0.1", port)):
                pass

    t1 = spawn(writer)
    t2 = spawn(unblocker)
    try:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            s.listen(10)
            s.setblocking(False)
            ev1.set()
            await ev2.wait()
            t0 = time.time()
            r = await waitfn(tgen(wait), s.fileno())
            dt = time.time() - t0
            ev3.set()  # unblock the unblocker
            if check_timing(request):
                assert dt < 0.1
            assert r == ready
    finally:
        # await gather(t1)
        await gather(t1, t2)


@pytest.mark.slow
@pytest.mark.parametrize("event", ["W", "RW"])
@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_w(waitfn, event, request):
    # Test that wait functions handle waiting and returning state correctly
    waitfn = getattr(waiting, waitfn)
    wait = getattr(waiting.Wait, event)

    rs, ws = socket.socketpair()  # the w socket is already ready for writing
    rs.setblocking(False)
    ws.setblocking(False)
    with rs, ws:
        t0 = time.time()
        r = await waitfn(tgen(wait), ws.fileno(), 0.5)
        dt = time.time() - t0
        # Check timing and received waiting state
        assert r == waiting.Ready.W
        if check_timing(request):
            assert dt < 0.1


@pytest.mark.parametrize("waitfn", waitfns)
@pytest.mark.parametrize("interval", intervals)
async def test_wait(pgconn, waitfn, interval):
    waitfn = getattr(waiting, waitfn)

    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    (res,) = await waitfn(gen, pgconn.socket, interval)
    assert res.status == ExecStatus.TUPLES_OK


@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_bad(pgconn, waitfn):
    waitfn = getattr(waiting, waitfn)

    pgconn.send_query(b"select 1")
    gen = generators.execute(pgconn)
    pgconn.finish()
    with pytest.raises(psycopg.OperationalError):
        await waitfn(gen, pgconn.socket)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_timeout(pgconn, waitfn):
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

    (res,) = await waitfn(gen_wrapper(), pgconn.socket, interval=0.1)
    assert res.status == ExecStatus.TUPLES_OK
    ds = [t1 - t0 for t0, t1 in zip(ts[:-1], ts[1:])]
    assert len(ds) >= 5
    for d in ds[:5]:
        assert d == pytest.approx(0.1, 0.05)


@pytest.mark.slow
@pytest.mark.skipif(
    "sys.platform == 'win32'", reason="win32 works ok, but FDs are mysterious"
)
@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_large_fd(dsn, waitfn):
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
            if waitfn.__name__ == "wait_select":
                with pytest.raises(ValueError):
                    await waitfn(gen, pgconn.socket)
            else:
                (res,) = await waitfn(gen, pgconn.socket)
                assert res.status == ExecStatus.TUPLES_OK
        finally:
            pgconn.finish()
    finally:
        for f in files:
            f.close()


@pytest.mark.parametrize("waitfn", waitfns)
async def test_wait_timeout_none_unsupported(waitfn):
    waitfn = getattr(waiting, waitfn)

    with pytest.raises(ValueError):
        await waitfn(tgen(waiting.Wait.R), 1, None)


def check_timing(request):
    """Return true if the test run requires to check timing

    Return false if the user has specified something like `pytest -m "not timing"

    Allow to run the tests to verify if the responses are correct but ignoring
    the timing, which on macOS and Windows in CI is very slow.
    """
    tokens = request.config.option.markexpr.split()
    if "timing" not in tokens:
        return True
    if (idx := tokens.index("timing")) > 0 and tokens[idx - 1] == "not":
        return False
    return True
