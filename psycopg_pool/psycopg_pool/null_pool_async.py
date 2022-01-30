"""
psycopg asynchronous null connection pool
"""

# Copyright (C) 2022 The Psycopg Team

import asyncio
import logging
from typing import Any, Optional

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .errors import PoolTimeout, TooManyRequests
from ._compat import ConnectionTimeout
from .null_pool import _BaseNullConnectionPool
from .pool_async import AsyncConnectionPool, AddConnection

logger = logging.getLogger("psycopg.pool")


class AsyncNullConnectionPool(_BaseNullConnectionPool, AsyncConnectionPool):
    async def wait(self, timeout: float = 30.0) -> None:
        self._check_open_getconn()

        async with self._lock:
            assert not self._pool_full_event
            self._pool_full_event = asyncio.Event()

        logger.info("waiting for pool %r initialization", self.name)
        self.run_task(AddConnection(self))
        try:
            await asyncio.wait_for(self._pool_full_event.wait(), timeout)
        except asyncio.TimeoutError:
            await self.close()  # stop all the tasks
            raise PoolTimeout(
                f"pool initialization incomplete after {timeout} sec"
            ) from None

        async with self._lock:
            assert self._pool_full_event
            self._pool_full_event = None

        logger.info("pool %r is ready to use", self.name)

    async def _get_ready_connection(
        self, timeout: Optional[float]
    ) -> Optional[AsyncConnection[Any]]:
        conn: Optional[AsyncConnection[Any]] = None
        if self.max_size == 0 or self._nconns < self.max_size:
            # Create a new connection for the client
            try:
                conn = await self._connect(timeout=timeout)
            except ConnectionTimeout as ex:
                raise PoolTimeout(str(ex)) from None
            self._nconns += 1
        elif self.max_waiting and len(self._waiting) >= self.max_waiting:
            self._stats[self._REQUESTS_ERRORS] += 1
            raise TooManyRequests(
                f"the pool {self.name!r} has already"
                f" {len(self._waiting)} requests waiting"
            )
        return conn

    async def _maybe_close_connection(self, conn: AsyncConnection[Any]) -> bool:
        # Close the connection if no client is waiting for it, or if the pool
        # is closed. For extra refcare remove the pool reference from it.
        # Maintain the stats.
        async with self._lock:
            if not self._closed and self._waiting:
                return False

            conn._pool = None
            if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
                self._stats[self._RETURNS_BAD] += 1
            await conn.close()
            self._nconns -= 1
            return True

    async def resize(self, min_size: int, max_size: Optional[int] = None) -> None:
        min_size, max_size = self._check_size(min_size, max_size)

        logger.info(
            "resizing %r to min_size=%s max_size=%s",
            self.name,
            min_size,
            max_size,
        )
        async with self._lock:
            self._min_size = min_size
            self._max_size = max_size

    async def check(self) -> None:
        pass

    async def _add_to_pool(self, conn: AsyncConnection[Any]) -> None:
        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        # Also disable the warning for open connection in conn.__del__
        conn._pool = None

        # Critical section: if there is a client waiting give it the connection
        # otherwise put it back into the pool.
        async with self._lock:
            while self._waiting:
                # If there is a client waiting (which is still waiting and
                # hasn't timed out), give it the connection and notify it.
                pos = self._waiting.popleft()
                if await pos.set(conn):
                    break
            else:
                # No client waiting for a connection: close the connection
                await conn.close()

                # If we have been asked to wait for pool init, notify the
                # waiter if the pool is ready.
                if self._pool_full_event:
                    self._pool_full_event.set()
                else:
                    # The connection created by wait shouldn't decrease the
                    # count of the number of connection used.
                    self._nconns -= 1
