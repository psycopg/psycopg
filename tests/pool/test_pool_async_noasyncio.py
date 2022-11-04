# These tests relate to AsyncConnectionPool, but are not marked asyncio
# because they rely on the pool initialization outside the asyncio loop.

import asyncio

import pytest

try:
    import psycopg_pool as pool
except ImportError:
    # Tests should have been skipped if the package is not available
    pass


@pytest.mark.slow
def test_reconnect_after_max_lifetime(dsn):
    # See issue #219, pool created before the loop.
    p = pool.AsyncConnectionPool(dsn, min_size=1, max_lifetime=0.2, open=False)

    async def test():
        try:
            await p.open()
            ns = []
            for i in range(5):
                async with p.connection() as conn:
                    cur = await conn.execute("select 1")
                    ns.append(await cur.fetchone())
                await asyncio.sleep(0.2)
            assert len(ns) == 5
        finally:
            await p.close()

    asyncio.run(asyncio.wait_for(test(), timeout=2.0))


@pytest.mark.slow
def test_working_created_before_loop(dsn):
    p = pool.AsyncNullConnectionPool(dsn, open=False)

    async def test():
        try:
            await p.open()
            ns = []
            for i in range(5):
                async with p.connection() as conn:
                    cur = await conn.execute("select 1")
                    ns.append(await cur.fetchone())
                await asyncio.sleep(0.2)
            assert len(ns) == 5
        finally:
            await p.close()

    asyncio.run(asyncio.wait_for(test(), timeout=2.0))


def test_cant_create_open_outside_loop(dsn):
    with pytest.raises(RuntimeError):
        pool.AsyncConnectionPool(dsn, open=True)
