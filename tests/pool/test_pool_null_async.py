import logging
from typing import Any, Dict, List

import pytest
from packaging.version import parse as ver  # noqa: F401  # used in skipif

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import class_row, Row, TupleRow

from ..utils import assert_type, set_autocommit
from ..acompat import AEvent, asleep, spawn, gather, skip_sync
from .test_pool_common_async import delay_connection, ensure_waiting

try:
    import psycopg_pool as pool
except ImportError:
    # Tests should have been skipped if the package is not available
    pass

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


async def test_default_sizes(dsn):
    async with pool.AsyncNullConnectionPool(dsn) as p:
        assert p.min_size == p.max_size == 0


async def test_min_size_max_size(dsn):
    async with pool.AsyncNullConnectionPool(dsn, min_size=0, max_size=2) as p:
        assert p.min_size == 0
        assert p.max_size == 2


@pytest.mark.parametrize("min_size, max_size", [(1, None), (-1, None), (0, -2)])
async def test_bad_size(dsn, min_size, max_size):
    with pytest.raises(ValueError):
        pool.AsyncNullConnectionPool(min_size=min_size, max_size=max_size)


class MyRow(Dict[str, Any]):
    ...


async def test_generic_connection_type(dsn):
    async def configure(conn: psycopg.AsyncConnection[Any]) -> None:
        await set_autocommit(conn, True)

    class MyConnection(psycopg.AsyncConnection[Row]):
        pass

    async with pool.AsyncNullConnectionPool(
        dsn,
        connection_class=MyConnection[MyRow],
        kwargs={"row_factory": class_row(MyRow)},
        configure=configure,
    ) as p1:
        async with p1.connection() as conn1:
            cur1 = await conn1.execute("select 1 as x")
            (row1,) = await cur1.fetchall()
    assert_type(p1, pool.AsyncNullConnectionPool[MyConnection[MyRow]])
    assert_type(conn1, MyConnection[MyRow])
    assert_type(row1, MyRow)
    assert conn1.autocommit
    assert row1 == {"x": 1}

    async with pool.AsyncNullConnectionPool(
        dsn, connection_class=MyConnection[TupleRow]
    ) as p2:
        async with p2.connection() as conn2:
            cur2 = await conn2.execute("select 2 as y")
            (row2,) = await cur2.fetchall()
    assert_type(p2, pool.AsyncNullConnectionPool[MyConnection[TupleRow]])
    assert_type(conn2, MyConnection[TupleRow])
    assert_type(row2, TupleRow)
    assert row2 == (2,)


async def test_non_generic_connection_type(dsn):
    async def configure(conn: psycopg.AsyncConnection[Any]) -> None:
        await set_autocommit(conn, True)

    class MyConnection(psycopg.AsyncConnection[MyRow]):
        def __init__(self, *args: Any, **kwargs: Any):
            kwargs["row_factory"] = class_row(MyRow)
            super().__init__(*args, **kwargs)

    async with pool.AsyncNullConnectionPool(
        dsn, connection_class=MyConnection, configure=configure
    ) as p1:
        async with p1.connection() as conn1:
            (row1,) = await (await conn1.execute("select 1 as x")).fetchall()
    assert_type(p1, pool.AsyncNullConnectionPool[MyConnection])
    assert_type(conn1, MyConnection)
    assert_type(row1, MyRow)
    assert conn1.autocommit
    assert row1 == {"x": 1}


@pytest.mark.crdb_skip("backend pid")
async def test_its_no_pool_at_all(dsn):
    async with pool.AsyncNullConnectionPool(dsn, max_size=2) as p:
        async with p.connection() as conn:
            pid1 = conn.info.backend_pid

            async with p.connection() as conn2:
                pid2 = conn2.info.backend_pid

        async with p.connection() as conn:
            assert conn.info.backend_pid not in (pid1, pid2)


@pytest.mark.slow
@pytest.mark.timing
async def test_wait_ready(dsn, monkeypatch):
    delay_connection(monkeypatch, 0.2)
    with pytest.raises(pool.PoolTimeout):
        async with pool.AsyncNullConnectionPool(dsn, num_workers=1) as p:
            await p.wait(0.1)

    async with pool.AsyncNullConnectionPool(dsn, num_workers=1) as p:
        await p.wait(0.4)


async def test_configure(dsn):
    inits = 0

    async def configure(conn):
        nonlocal inits
        inits += 1
        async with conn.transaction():
            await conn.execute("set default_transaction_read_only to on")

    async with pool.AsyncNullConnectionPool(dsn, configure=configure) as p:
        async with p.connection() as conn:
            assert inits == 1
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone()) == ("on",)

        async with p.connection() as conn:
            assert inits == 2
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone()) == ("on",)
            await conn.close()

        async with p.connection() as conn:
            assert inits == 3
            res = await conn.execute("show default_transaction_read_only")
            assert (await res.fetchone()) == ("on",)


@pytest.mark.crdb_skip("backend pid")
async def test_reset(dsn):
    resets = 0

    async def setup(conn):
        async with conn.transaction():
            await conn.execute("set timezone to '+1:00'")

    async def reset(conn):
        nonlocal resets
        resets += 1
        async with conn.transaction():
            await conn.execute("set timezone to utc")

    pids = []

    async def worker():
        async with p.connection() as conn:
            assert resets == 1
            cur = await conn.execute("show timezone")
            assert (await cur.fetchone()) == ("UTC",)
            pids.append(conn.info.backend_pid)

    async with pool.AsyncNullConnectionPool(dsn, max_size=1, reset=reset) as p:
        async with p.connection() as conn:
            # Queue the worker so it will take the same connection a second time
            # instead of making a new one.
            t = spawn(worker)
            await ensure_waiting(p)

            assert resets == 0
            await conn.execute("set timezone to '+2:00'")
            pids.append(conn.info.backend_pid)

        await gather(t)
        await p.wait()

    assert resets == 1
    assert pids[0] == pids[1]


@pytest.mark.crdb_skip("backend pid")
async def test_reset_badstate(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def reset(conn):
        await conn.execute("reset all")

    pids = []

    async def worker():
        async with p.connection() as conn:
            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

    async with pool.AsyncNullConnectionPool(dsn, max_size=1, reset=reset) as p:
        async with p.connection() as conn:
            t = spawn(worker)
            await ensure_waiting(p)

            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

        await gather(t)

    assert pids[0] != pids[1]
    assert caplog.records
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_reset_broken(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")

    async def reset(conn):
        async with conn.transaction():
            await conn.execute("WAT")

    pids = []

    async def worker():
        async with p.connection() as conn:
            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

    async with pool.AsyncNullConnectionPool(dsn, max_size=1, reset=reset) as p:
        async with p.connection() as conn:
            t = spawn(worker)
            await ensure_waiting(p)

            await conn.execute("select 1")
            pids.append(conn.info.backend_pid)

        await gather(t)

    assert pids[0] != pids[1]
    assert caplog.records
    assert "WAT" in caplog.records[0].message


@pytest.mark.slow
@pytest.mark.skipif("ver(psycopg.__version__) < ver('3.0.8')")
async def test_no_queue_timeout(deaf_port):
    async with pool.AsyncNullConnectionPool(
        kwargs={"host": "localhost", "port": deaf_port}
    ) as p:
        with pytest.raises(pool.PoolTimeout):
            async with p.connection(timeout=1):
                pass


@pytest.mark.crdb_skip("backend pid")
async def test_intrans_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE
            cur = await conn.execute(
                "select 1 from pg_class where relname = 'test_intrans_rollback'"
            )
            assert not await cur.fetchone()

    async with pool.AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()

        # Queue the worker so it will take the connection a second time instead
        # of making a new one.
        t = spawn(worker)
        await ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        await conn.execute("create table test_intrans_rollback ()")
        assert conn.info.transaction_status == TransactionStatus.INTRANS
        await p.putconn(conn)
        await gather(t)

    assert pids[0] == pids[1]
    assert len(caplog.records) == 1
    assert "INTRANS" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
async def test_inerror_rollback(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    async with pool.AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()

        # Queue the worker so it will take the connection a second time instead
        # of making a new one.
        t = spawn(worker)
        await ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        with pytest.raises(psycopg.ProgrammingError):
            await conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)
        await gather(t)

    assert pids[0] == pids[1]
    assert len(caplog.records) == 1
    assert "INERROR" in caplog.records[0].message


@pytest.mark.crdb_skip("backend pid")
@pytest.mark.crdb_skip("copy")
async def test_active_close(dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    async with pool.AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()

        t = spawn(worker)
        await ensure_waiting(p)

        pids.append(conn.info.backend_pid)
        conn.pgconn.exec_(b"copy (select * from generate_series(1, 10)) to stdout")
        assert conn.info.transaction_status == TransactionStatus.ACTIVE
        await p.putconn(conn)
        await gather(t)

    assert pids[0] != pids[1]
    assert len(caplog.records) == 2
    assert "ACTIVE" in caplog.records[0].message
    assert "BAD" in caplog.records[1].message


@pytest.mark.crdb_skip("backend pid")
async def test_fail_rollback_close(dsn, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="psycopg.pool")
    pids = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            assert conn.info.transaction_status == TransactionStatus.IDLE

    async with pool.AsyncNullConnectionPool(dsn, max_size=1) as p:
        conn = await p.getconn()
        t = spawn(worker)
        await ensure_waiting(p)

        async def bad_rollback():
            conn.pgconn.finish()
            await orig_rollback()

        # Make the rollback fail
        orig_rollback = conn.rollback
        monkeypatch.setattr(conn, "rollback", bad_rollback)

        pids.append(conn.info.backend_pid)
        with pytest.raises(psycopg.ProgrammingError):
            await conn.execute("wat")
        assert conn.info.transaction_status == TransactionStatus.INERROR
        await p.putconn(conn)
        await gather(t)

    assert pids[0] != pids[1]
    assert len(caplog.records) == 3
    assert "INERROR" in caplog.records[0].message
    assert "OperationalError" in caplog.records[1].message
    assert "BAD" in caplog.records[2].message


async def test_closed_putconn(dsn):
    async with pool.AsyncNullConnectionPool(dsn) as p:
        async with p.connection() as conn:
            pass
        assert conn.closed


@pytest.mark.parametrize("min_size, max_size", [(1, None), (-1, None), (0, -2)])
async def test_bad_resize(dsn, min_size, max_size):
    async with pool.AsyncNullConnectionPool() as p:
        with pytest.raises(ValueError):
            await p.resize(min_size=min_size, max_size=max_size)


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("backend pid")
async def test_max_lifetime(dsn):
    pids: List[int] = []

    async def worker():
        async with p.connection() as conn:
            pids.append(conn.info.backend_pid)
            await asleep(0.1)

    async with pool.AsyncNullConnectionPool(dsn, max_size=1, max_lifetime=0.2) as p:
        ts = [spawn(worker) for i in range(5)]
        await gather(*ts)

    assert pids[0] == pids[1] != pids[4], pids


async def test_check(dsn):
    # no.op
    async with pool.AsyncNullConnectionPool(dsn) as p:
        await p.check()


@pytest.mark.slow
async def test_stats_connect(dsn, proxy, monkeypatch):
    proxy.start()
    delay_connection(monkeypatch, 0.2)
    async with pool.AsyncNullConnectionPool(proxy.client_dsn, max_size=3) as p:
        await p.wait()
        stats = p.get_stats()
        assert stats["connections_num"] == 1
        assert stats.get("connections_errors", 0) == 0
        assert stats.get("connections_lost", 0) == 0
        assert 200 <= stats["connections_ms"] < 300


@skip_sync
async def test_cancellation_in_queue(dsn):
    # https://github.com/psycopg/psycopg/issues/509

    nconns = 3

    async with pool.AsyncNullConnectionPool(
        dsn, min_size=0, max_size=nconns, timeout=1
    ) as p:
        await p.wait()

        got_conns = []
        ev = AEvent()

        async def worker(i):
            try:
                logging.info("worker %s started", i)
                nonlocal got_conns

                async with p.connection() as conn:
                    logging.info("worker %s got conn", i)
                    cur = await conn.execute("select 1")
                    assert (await cur.fetchone()) == (1,)

                    got_conns.append(conn)
                    if len(got_conns) >= nconns:
                        ev.set()

                    await asleep(5)

            except BaseException as ex:
                logging.info("worker %s stopped: %r", i, ex)
                raise

        # Start tasks taking up all the connections and getting in the queue
        tasks = [spawn(worker, (i,)) for i in range(nconns * 3)]

        # wait until the pool has served all the connections and clients are queued.
        await ev.wait_timeout(3.0)
        for i in range(10):
            if p.get_stats().get("requests_queued", 0):
                break
            else:
                await asleep(0.1)
        else:
            pytest.fail("no client got in the queue")

        [task.cancel() for task in reversed(tasks)]
        await gather(*tasks, return_exceptions=True, timeout=1.0)

        stats = p.get_stats()
        assert stats.get("requests_waiting", 0) == 0

        async with p.connection() as conn:
            cur = await conn.execute("select 1")
            assert await cur.fetchone() == (1,)
