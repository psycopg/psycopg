"""
Psycopg null connection pools
"""

# Copyright (C) 2022 The Psycopg Team

import logging
import threading
from time import monotonic
from typing import Any, Optional, Tuple

from psycopg import Connection
from psycopg.pq import TransactionStatus

from .pool import ConnectionPool, WaitingClient
from .pool import AddConnection, ReturnConnection
from .errors import PoolTimeout, TooManyRequests
from ._compat import ConnectionTimeout

logger = logging.getLogger("psycopg.pool")


class _BaseNullConnectionPool:
    def __init__(
        self, conninfo: str = "", min_size: int = 0, *args: Any, **kwargs: Any
    ):
        super().__init__(  # type: ignore[call-arg]
            conninfo, *args, min_size=min_size, **kwargs
        )

    def _check_size(
        self, min_size: int, max_size: Optional[int]
    ) -> Tuple[int, int]:
        if max_size is None:
            max_size = min_size

        if min_size != 0:
            raise ValueError("null pools must have min_size = 0")
        if max_size < min_size:
            raise ValueError("max_size must be greater or equal than min_size")

        return min_size, max_size

    def _start_initial_tasks(self) -> None:
        # Null pools don't have background tasks to fill connections
        # or to grow/shrink.
        return


class NullConnectionPool(_BaseNullConnectionPool, ConnectionPool):
    def wait(self, timeout: float = 30.0) -> None:
        """
        Create a connection for test.

        Calling this function will verify that the connectivity with the
        database works as expected. However the connection will not be stored
        in the pool.

        Raise `PoolTimeout` if not ready within *timeout* sec.
        """
        self._check_open_getconn()

        with self._lock:
            assert not self._pool_full_event
            self._pool_full_event = threading.Event()

        logger.info("waiting for pool %r initialization", self.name)
        self.run_task(AddConnection(self))
        if not self._pool_full_event.wait(timeout):
            self.close()  # stop all the threads
            raise PoolTimeout(
                f"pool initialization incomplete after {timeout} sec"
            )

        with self._lock:
            assert self._pool_full_event
            self._pool_full_event = None

        logger.info("pool %r is ready to use", self.name)

    def getconn(self, timeout: Optional[float] = None) -> Connection[Any]:
        logger.info("connection requested from %r", self.name)
        self._stats[self._REQUESTS_NUM] += 1

        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        with self._lock:
            self._check_open_getconn()

            pos: Optional[WaitingClient] = None
            if self.max_size == 0 or self._nconns < self.max_size:
                # Create a new connection for the client
                try:
                    conn = self._connect(timeout=timeout)
                except ConnectionTimeout as ex:
                    raise PoolTimeout(str(ex)) from None
                self._nconns += 1
            else:
                if self.max_waiting and len(self._waiting) >= self.max_waiting:
                    self._stats[self._REQUESTS_ERRORS] += 1
                    raise TooManyRequests(
                        f"the pool {self.name!r} has aleady"
                        f" {len(self._waiting)} requests waiting"
                    )

                # No connection available: put the client in the waiting queue
                t0 = monotonic()
                pos = WaitingClient()
                self._waiting.append(pos)
                self._stats[self._REQUESTS_QUEUED] += 1

        # If we are in the waiting queue, wait to be assigned a connection
        # (outside the critical section, so only the waiting client is locked)
        if pos:
            if timeout is None:
                timeout = self.timeout
            try:
                conn = pos.wait(timeout=timeout)
            except Exception:
                self._stats[self._REQUESTS_ERRORS] += 1
                raise
            finally:
                t1 = monotonic()
                self._stats[self._REQUESTS_WAIT_MS] += int(1000.0 * (t1 - t0))

        # Tell the connection it belongs to a pool to avoid closing on __exit__
        conn._pool = self
        logger.info("connection given by %r", self.name)
        return conn

    def putconn(self, conn: Connection[Any]) -> None:
        # Quick check to discard the wrong connection
        self._check_pool_putconn(conn)

        logger.info("returning connection to %r", self.name)

        # Close the connection if no client is waiting for it, or if the pool
        # is closed. For extra refcare remove the pool reference from it.
        # Maintain the stats.
        with self._lock:
            if self._closed or not self._waiting:
                conn._pool = None
                if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
                    self._stats[self._RETURNS_BAD] += 1
                conn.close()
                self._nconns -= 1
                return

        # Use a worker to perform eventual maintenance work in a separate thread
        if self._reset:
            self.run_task(ReturnConnection(self, conn))
        else:
            self._return_connection(conn)

    def resize(self, min_size: int, max_size: Optional[int] = None) -> None:
        min_size, max_size = self._check_size(min_size, max_size)

        logger.info(
            "resizing %r to min_size=%s max_size=%s",
            self.name,
            min_size,
            max_size,
        )
        with self._lock:
            self._min_size = min_size
            self._max_size = max_size

    def check(self) -> None:
        """No-op, as the pool doesn't have connections in its state."""
        pass

    def _add_to_pool(self, conn: Connection[Any]) -> None:
        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        # Also disable the warning for open connection in conn.__del__
        conn._pool = None

        # Critical section: if there is a client waiting give it the connection
        # otherwise put it back into the pool.
        with self._lock:
            while self._waiting:
                # If there is a client waiting (which is still waiting and
                # hasn't timed out), give it the connection and notify it.
                pos = self._waiting.popleft()
                if pos.set(conn):
                    break
            else:
                # No client waiting for a connection: close the connection
                conn.close()

                # If we have been asked to wait for pool init, notify the
                # waiter if the pool is ready.
                if self._pool_full_event:
                    self._pool_full_event.set()
                else:
                    # The connection created by wait shoudn't decrease the
                    # count of the number of connection used.
                    self._nconns -= 1
