"""
psycopg3 synchronous connection pool
"""

# Copyright (C) 2021 The Psycopg Team

import sys
import asyncio
import logging
from time import monotonic
from typing import Any, Awaitable, Callable, Deque, AsyncIterator, Optional
from collections import deque

from ..pq import TransactionStatus
from ..connection import AsyncConnection

from . import tasks
from .base import ConnectionAttempt, BasePool
from .errors import PoolClosed, PoolTimeout

if sys.version_info >= (3, 7):
    from contextlib import asynccontextmanager

    get_running_loop = asyncio.get_running_loop

else:
    from ..utils.context import asynccontextmanager

    get_running_loop = asyncio.get_event_loop

logger = logging.getLogger(__name__)


class AsyncConnectionPool(BasePool[AsyncConnection]):
    def __init__(
        self,
        conninfo: str = "",
        configure: Optional[
            Callable[[AsyncConnection], Awaitable[None]]
        ] = None,
        **kwargs: Any,
    ):
        self._configure = configure

        self._lock = asyncio.Lock()
        self._waiting: Deque["AsyncClient"] = deque()

        # to notify that the pool is full
        self._pool_full_event: Optional[asyncio.Event] = None

        self.loop = get_running_loop()

        super().__init__(conninfo, **kwargs)

    async def wait_ready(self, timeout: float = 30.0) -> None:
        """
        Wait for the pool to be full after init.

        Raise `PoolTimeout` if not ready within *timeout* sec.
        """
        async with self._lock:
            assert not self._pool_full_event
            if len(self._pool) >= self._nconns:
                return
            self._pool_full_event = asyncio.Event()

        try:
            await asyncio.wait_for(self._pool_full_event.wait(), timeout)
        except asyncio.TimeoutError:
            await self.close()  # stop all the threads
            raise PoolTimeout(
                f"pool initialization incomplete after {timeout} sec"
            )

        async with self._lock:
            self._pool_full_event = None

    @asynccontextmanager
    async def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[AsyncConnection]:
        """Context manager to obtain a connection from the pool.

        Returned the connection immediately if available, otherwise wait up to
        *timeout* or `self.timeout` and throw `PoolTimeout` if a connection is
        not available in time.

        Upon context exit, return the connection to the pool. Apply the normal
        connection context behaviour (commit/rollback the transaction in case
        of success/error). If the connection is no more in working state
        replace it with a new one.
        """
        conn = await self.getconn(timeout=timeout)
        try:
            async with conn:
                yield conn
        finally:
            await self.putconn(conn)

    async def getconn(
        self, timeout: Optional[float] = None
    ) -> AsyncConnection:
        """Obtain a contection from the pool.

        You should preferrably use `connection()`. Use this function only if
        it is not possible to use the connection as context manager.

        After using this function you *must* call a corresponding `putconn()`:
        failing to do so will deplete the pool. A depleted pool is a sad pool:
        you don't want a depleted pool.
        """
        logger.info("connection requested to %r", self.name)
        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        async with self._lock:
            if self._closed:
                raise PoolClosed(f"the pool {self.name!r} is closed")

            pos: Optional[AsyncClient] = None
            if self._pool:
                # Take a connection ready out of the pool
                conn = self._pool.popleft()
                if len(self._pool) < self._nconns_min:
                    self._nconns_min = len(self._pool)
            else:
                # No connection available: put the client in the waiting queue
                pos = AsyncClient()
                self._waiting.append(pos)

                # If there is space for the pool to grow, let's do it
                if self._nconns < self._maxconn:
                    self._nconns += 1
                    logger.info(
                        "growing pool %r to %s", self.name, self._nconns
                    )
                    self.run_task(tasks.AddConnection(self))

        # If we are in the waiting queue, wait to be assigned a connection
        # (outside the critical section, so only the waiting client is locked)
        if pos:
            if timeout is None:
                timeout = self.timeout
            conn = await pos.wait(timeout=timeout)

        # Tell the connection it belongs to a pool to avoid closing on __exit__
        # Note that this property shouldn't be set while the connection is in
        # the pool, to avoid to create a reference loop.
        conn._pool = self
        logger.info("connection given by %r", self.name)
        return conn

    async def putconn(self, conn: AsyncConnection) -> None:
        """Return a connection to the loving hands of its pool.

        Use this function only paired with a `getconn()`. You don't need to use
        it if you use the much more comfortable `connection()` context manager.
        """
        # Quick check to discard the wrong connection
        pool = getattr(conn, "_pool", None)
        if pool is not self:
            if pool:
                msg = f"it comes from pool {pool.name!r}"
            else:
                msg = "it doesn't come from any pool"
            raise ValueError(
                f"can't return connection to pool {self.name!r}, {msg}: {conn}"
            )

        logger.info("returning connection to %r", self.name)

        # If the pool is closed just close the connection instead of returning
        # it to the pool. For extra refcare remove the pool reference from it.
        if self._closed:
            conn._pool = None
            await conn.close()
            return

        # Use a worker to perform eventual maintenance work in a separate thread
        self.run_task(tasks.ReturnConnection(self, conn))

    async def close(self, timeout: float = 1.0) -> None:
        """Close the pool and make it unavailable to new clients.

        All the waiting and future client will fail to acquire a connection
        with a `PoolClosed` exception. Currently used connections will not be
        closed until returned to the pool.

        Wait *timeout* for threads to terminate their job, if positive.
        """
        if self._closed:
            return

        async with self._lock:
            self._closed = True
            logger.debug("pool %r closed", self.name)

            # Take waiting client and pool connections out of the state
            waiting = list(self._waiting)
            self._waiting.clear()
            pool = list(self._pool)
            self._pool.clear()

        # Now that the flag _closed is set, getconn will fail immediately,
        # putconn will just close the returned connection.

        # Stop the scheduler
        self._sched.enter(0, None)

        # Stop the worker threads
        for i in range(len(self._workers)):
            self.run_task(tasks.StopWorker(self))

        # Signal to eventual clients in the queue that business is closed.
        for pos in waiting:
            await pos.fail(PoolClosed(f"the pool {self.name!r} is closed"))

        # Close the connections still in the pool
        for conn in pool:
            await conn.close()

        # Wait for the worker threads to terminate
        if timeout > 0:
            loop = get_running_loop()
            for t in [self._sched_runner] + self._workers:
                if not t.is_alive():
                    continue
                await loop.run_in_executor(None, lambda: t.join(timeout))
                if t.is_alive():
                    logger.warning(
                        "couldn't stop thread %s in pool %r within %s seconds",
                        t,
                        self.name,
                        timeout,
                    )

    async def resize(
        self, minconn: int, maxconn: Optional[int] = None
    ) -> None:
        if maxconn is None:
            maxconn = minconn
        if maxconn < minconn:
            raise ValueError("maxconn must be greater or equal than minconn")

        ngrow = max(0, minconn - self._minconn)

        logger.info(
            "resizing %r to minconn=%s maxconn=%s", self.name, minconn, maxconn
        )
        async with self._lock:
            self._minconn = minconn
            self._maxconn = maxconn
            self._nconns += ngrow

        for i in range(ngrow):
            self.run_task(tasks.AddConnection(self))

    async def configure(self, conn: AsyncConnection) -> None:
        """Configure a connection after creation."""
        if self._configure:
            await self._configure(conn)

    def reconnect_failed(self) -> None:
        """
        Called when reconnection failed for longer than `reconnect_timeout`.
        """
        self._reconnect_failed(self)

    async def _connect(self) -> AsyncConnection:
        """Return a new connection configured for the pool."""
        conn = await AsyncConnection.connect(self.conninfo, **self.kwargs)
        await self.configure(conn)
        conn._pool = self
        return conn

    async def _add_connection(
        self, attempt: Optional[ConnectionAttempt]
    ) -> None:
        """Try to connect and add the connection to the pool.

        If failed, reschedule a new attempt in the future for a few times, then
        give up, decrease the pool connections number and call
        `self.reconnect_failed()`.

        """
        now = monotonic()
        if not attempt:
            attempt = ConnectionAttempt(
                reconnect_timeout=self.reconnect_timeout
            )

        try:
            conn = await self._connect()
        except Exception as e:
            logger.warning(f"error connecting in {self.name!r}: {e}")
            if attempt.time_to_give_up(now):
                logger.warning(
                    "reconnection attempt in pool %r failed after %s sec",
                    self.name,
                    self.reconnect_timeout,
                )
                async with self._lock:
                    self._nconns -= 1
                self.reconnect_failed()
            else:
                attempt.update_delay(now)
                self.schedule_task(
                    tasks.AddConnection(self, attempt), attempt.delay
                )
        else:
            await self._add_to_pool(conn)

    async def _return_connection(self, conn: AsyncConnection) -> None:
        """
        Return a connection to the pool after usage.
        """
        await self._reset_connection(conn)
        if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
            # Connection no more in working state: create a new one.
            logger.warning("discarding closed connection: %s", conn)
            self.run_task(tasks.AddConnection(self))
        else:
            await self._add_to_pool(conn)

    async def _add_to_pool(self, conn: AsyncConnection) -> None:
        """
        Add a connection to the pool.

        The connection can be a fresh one or one already used in the pool.

        If a client is already waiting for a connection pass it on, otherwise
        put it back into the pool
        """
        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        # Also disable the warning for open connection in conn.__del__
        conn._pool = None

        pos: Optional[AsyncClient] = None

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
                # No client waiting for a connection: put it back into the pool
                self._pool.append(conn)

                # If we have been asked to wait for pool init, notify the
                # waiter if the pool is full.
                if self._pool_full_event and len(self._pool) >= self._nconns:
                    self._pool_full_event.set()

    async def _reset_connection(self, conn: AsyncConnection) -> None:
        """
        Bring a connection to IDLE state or close it.
        """
        status = conn.pgconn.transaction_status
        if status == TransactionStatus.IDLE:
            return

        if status in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
            # Connection returned with an active transaction
            logger.warning("rolling back returned connection: %s", conn)
            try:
                await conn.rollback()
            except Exception as e:
                logger.warning(
                    "rollback failed: %s: %s. Discarding connection %s",
                    e.__class__.__name__,
                    e,
                    conn,
                )
                await conn.close()

        elif status == TransactionStatus.ACTIVE:
            # Connection returned during an operation. Bad... just close it.
            logger.warning("closing returned connection: %s", conn)
            await conn.close()

    async def _shrink_pool(self) -> None:
        to_close: Optional[AsyncConnection] = None

        async with self._lock:
            # Reset the min number of connections used
            nconns_min = self._nconns_min
            self._nconns_min = len(self._pool)

            # If the pool can shrink and connections were unused, drop one
            if self._nconns > self._minconn and nconns_min > 0:
                to_close = self._pool.popleft()
                self._nconns -= 1

        if to_close:
            logger.info(
                "shrinking pool %r to %s because %s unused connections"
                " in the last %s sec",
                self.name,
                self._nconns,
                nconns_min,
                self.max_idle,
            )
            await to_close.close()


class AsyncClient:
    """A position in a queue for a client waiting for a connection."""

    __slots__ = ("conn", "error", "_cond")

    def __init__(self) -> None:
        self.conn: Optional[AsyncConnection] = None
        self.error: Optional[Exception] = None

        # The AsyncClient behaves in a way similar to an Event, but we need
        # to notify reliably the flagger that the waiter has "accepted" the
        # message and it hasn't timed out yet, otherwise the pool may give a
        # connection to a client that has already timed out getconn(), which
        # will be lost.
        self._cond = asyncio.Condition()

    async def wait(self, timeout: float) -> AsyncConnection:
        """Wait for a connection to be set and return it.

        Raise an exception if the wait times out or if fail() is called.
        """
        async with self._cond:
            if not (self.conn or self.error):
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout)
                except asyncio.TimeoutError:
                    self.error = PoolTimeout(
                        f"couldn't get a connection after {timeout} sec"
                    )

        if self.conn:
            return self.conn
        else:
            assert self.error
            raise self.error

    async def set(self, conn: AsyncConnection) -> bool:
        """Signal the client waiting that a connection is ready.

        Return True if the client has "accepted" the connection, False
        otherwise (typically because wait() has timed out).
        """
        async with self._cond:
            if self.conn or self.error:
                return False

            self.conn = conn
            self._cond.notify_all()
            return True

    async def fail(self, error: Exception) -> bool:
        """Signal the client that, alas, they won't have a connection today.

        Return True if the client has "accepted" the error, False otherwise
        (typically because wait() has timed out).
        """
        async with self._cond:
            if self.conn or self.error:
                return False

            self.error = error
            self._cond.notify_all()
            return True


tasks.AsyncConnectionPool = AsyncConnectionPool  # type: ignore
