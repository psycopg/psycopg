import asyncio

import pytest


@pytest.fixture
def loop():
    """Return the async loop to test coroutines."""
    return asyncio.get_event_loop()


@pytest.fixture
def aconn(loop, dsn, pq):
    """Return an `AsyncConnection` connected to the ``--test-dsn`` database."""
    from psycopg3 import AsyncConnection

    conn = loop.run_until_complete(AsyncConnection.connect(dsn))
    yield conn
    loop.run_until_complete(conn.close())
