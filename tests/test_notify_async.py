from __future__ import annotations

from time import time

import pytest
from psycopg import Notify

from .acompat import AEvent, alist, asleep, gather, spawn

pytestmark = pytest.mark.crdb_skip("notify")


async def test_notify_handlers(aconn):
    nots1 = []
    nots2 = []

    def cb1(n):
        nots1.append(n)

    aconn.add_notify_handler(cb1)
    aconn.add_notify_handler(lambda n: nots2.append(n))

    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")
    await aconn.execute("notify foo, 'n1'")

    assert len(nots1) == 1
    n = nots1[0]
    assert n.channel == "foo"
    assert n.payload == "n1"
    assert n.pid == aconn.pgconn.backend_pid

    assert len(nots2) == 1
    assert nots2[0] == nots1[0]

    aconn.remove_notify_handler(cb1)
    await aconn.execute("notify foo, 'n2'")

    assert len(nots1) == 1
    assert len(nots2) == 2
    n = nots2[1]
    assert isinstance(n, Notify)
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == aconn.pgconn.backend_pid
    assert hash(n)

    with pytest.raises(ValueError):
        aconn.remove_notify_handler(cb1)


@pytest.mark.slow
@pytest.mark.timing
async def test_notify(aconn_cls, aconn, dsn):
    npid = None

    async def notifier():
        async with await aconn_cls.connect(dsn, autocommit=True) as nconn:
            nonlocal npid
            npid = nconn.pgconn.backend_pid

            await asleep(0.25)
            await nconn.execute("notify foo, '1'")
            await asleep(0.25)
            await nconn.execute("notify foo, '2'")

    async def receiver():
        await aconn.set_autocommit(True)
        cur = aconn.cursor()
        await cur.execute("listen foo")
        gen = aconn.notifies()
        async for n in gen:
            ns.append((n, time()))
            if len(ns) >= 2:
                await gen.aclose()

    ns: list[tuple[Notify, float]] = []
    t0 = time()
    workers = [spawn(notifier), spawn(receiver)]
    await gather(*workers)
    assert len(ns) == 2

    n, t1 = ns[0]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "1"
    assert t1 - t0 == pytest.approx(0.25, abs=0.05)

    n, t1 = ns[1]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "2"
    assert t1 - t0 == pytest.approx(0.5, abs=0.05)


@pytest.mark.slow
@pytest.mark.timing
async def test_no_notify_timeout(aconn):
    await aconn.set_autocommit(True)
    t0 = time()
    async for n in aconn.notifies(timeout=0.5):
        assert False
    dt = time() - t0
    assert 0.5 <= dt < 0.75


@pytest.mark.slow
@pytest.mark.timing
async def test_notify_timeout(aconn_cls, aconn, dsn):
    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")

    async def notifier():
        async with await aconn_cls.connect(dsn, autocommit=True) as nconn:
            await asleep(0.25)
            await nconn.execute("notify foo, '1'")

    worker = spawn(notifier)
    try:
        times = [time()]
        async for n in aconn.notifies(timeout=0.5):
            times.append(time())
        times.append(time())
    finally:
        await gather(worker)

    assert len(times) == 3
    assert times[1] - times[0] == pytest.approx(0.25, 0.1)
    assert times[2] - times[1] == pytest.approx(0.25, 0.1)


@pytest.mark.slow
@pytest.mark.timing
async def test_notify_timeout_0(aconn_cls, aconn, dsn):
    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")

    ns = await alist(aconn.notifies(timeout=0))
    assert not ns

    async with await aconn_cls.connect(dsn, autocommit=True) as nconn:
        await nconn.execute("notify foo, '1'")
        await asleep(0.1)

    ns = await alist(aconn.notifies(timeout=0))
    assert len(ns) == 1


@pytest.mark.slow
@pytest.mark.timing
async def test_stop_after(aconn_cls, aconn, dsn):
    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")

    async def notifier():
        async with await aconn_cls.connect(dsn, autocommit=True) as nconn:
            await nconn.execute("notify foo, '1'")
            await asleep(0.1)
            await nconn.execute("notify foo, '2'")
            await asleep(0.1)
            await nconn.execute("notify foo, '3'")

    worker = spawn(notifier)
    try:
        ns = await alist(aconn.notifies(timeout=1.0, stop_after=2))
        assert len(ns) == 2
        assert ns[0].payload == "1"
        assert ns[1].payload == "2"
    finally:
        await gather(worker)

    ns = await alist(aconn.notifies(timeout=0.0))
    assert len(ns) == 1
    assert ns[0].payload == "3"


@pytest.mark.timing
async def test_stop_after_batch(aconn_cls, aconn, dsn):
    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")

    async def notifier():
        async with await aconn_cls.connect(dsn, autocommit=True) as nconn:
            async with nconn.transaction():
                await nconn.execute("notify foo, '1'")
                await nconn.execute("notify foo, '2'")

    worker = spawn(notifier)
    try:
        ns = await alist(aconn.notifies(timeout=1.0, stop_after=1))
        assert len(ns) == 2
        assert ns[0].payload == "1"
        assert ns[1].payload == "2"
    finally:
        await gather(worker)


@pytest.mark.slow
@pytest.mark.timing
async def test_notifies_blocking(aconn):
    async def listener():
        async for _ in aconn.notifies(timeout=1):
            pass

    worker = spawn(listener)
    try:
        # Make sure the listener is listening
        if not aconn.lock.locked():
            await asleep(0.01)

        t0 = time()
        await aconn.execute("select 1")
        dt = time() - t0
    finally:
        await gather(worker)

    assert dt > 0.5


@pytest.mark.slow
async def test_generator_and_handler(aconn, aconn_cls, dsn):
    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")

    n1 = None
    n2 = None

    def set_n2(n):
        nonlocal n2
        n2 = n

    aconn.add_notify_handler(set_n2)

    async def listener():
        nonlocal n1
        async for n1 in aconn.notifies(timeout=1, stop_after=1):
            pass

    worker = spawn(listener)
    try:
        # Make sure the listener is listening
        if not aconn.lock.locked():
            await asleep(0.01)

        async with await aconn_cls.connect(dsn, autocommit=True) as nconn:
            await nconn.execute("notify foo, '1'")

    finally:
        await gather(worker)

    assert n1
    assert n2


@pytest.mark.parametrize("query_between", [True, False])
async def test_first_notify_not_lost(aconn, aconn_cls, dsn, query_between):
    await aconn.set_autocommit(True)
    await aconn.execute("listen foo")

    async with await aconn_cls.connect(dsn, autocommit=True) as conn2:
        await conn2.execute("notify foo, 'hi'")

    if query_between:
        await aconn.execute("select 1")

    n = None
    async for n in aconn.notifies(timeout=1, stop_after=1):
        pass
    assert n


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.parametrize("sleep_on", ["server", "client"])
async def test_notify_query_notify(aconn_cls, dsn, sleep_on):
    e = AEvent()
    by_gen: list[int] = []
    by_cb: list[int] = []
    workers = []

    async def notifier():
        async with await aconn_cls.connect(dsn, autocommit=True) as aconn:
            await asleep(0.1)
            for i in range(3):
                await aconn.execute("select pg_notify('counter', %s)", (str(i),))
                await asleep(0.2)

    async def listener():
        async with await aconn_cls.connect(dsn, autocommit=True) as aconn:
            aconn.add_notify_handler(lambda n: by_cb.append(int(n.payload)))

            await aconn.execute("listen counter")
            e.set()
            async for n in aconn.notifies(timeout=0.2):
                by_gen.append(int(n.payload))

            if sleep_on == "server":
                await aconn.execute("select pg_sleep(0.2)")
            else:
                assert sleep_on == "client"
                await asleep(0.2)

            async for n in aconn.notifies(timeout=0.2):
                by_gen.append(int(n.payload))

    workers.append(spawn(listener))
    await e.wait()
    workers.append(spawn(notifier))
    await gather(*workers)

    assert list(range(3)) == by_cb == by_gen, f"{by_gen=}, {by_cb=}"
