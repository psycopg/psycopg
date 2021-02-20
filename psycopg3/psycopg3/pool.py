"""
psycopg3 connection pool
"""

# Copyright (C) 2021 The Psycopg Team

import random
import logging
import threading
from abc import ABC, abstractmethod
from queue import Queue, Empty
from typing import Any, Callable, Deque, Dict, Iterator, List, Optional
from contextlib import contextmanager
from collections import deque

from . import errors as e
from .pq import TransactionStatus
from .connection import Connection

WORKER_TIMEOUT = 60.0

logger = logging.getLogger(__name__)


class PoolTimeout(e.OperationalError):
    pass


class PoolClosed(e.OperationalError):
    pass


class ConnectionPool:

    _num_pool = 0

    def __init__(
        self,
        conninfo: str = "",
        kwargs: Optional[Dict[str, Any]] = None,
        configure: Optional[Callable[[Connection], None]] = None,
        minconn: int = 4,
        maxconn: Optional[int] = None,
        name: Optional[str] = None,
        timeout_sec: float = 30.0,
        num_workers: int = 1,
    ):
        if maxconn is None:
            maxconn = minconn
        if maxconn < minconn:
            raise ValueError(
                f"can't create {self.__class__.__name__}"
                f" with maxconn={maxconn} < minconn={minconn}"
            )
        if not name:
            self.__class__._num_pool += 1
            name = f"pool-{self._num_pool}"

        if num_workers < 1:
            # TODO: allow num_workers to be 0 - sync pool?
            raise ValueError("num_workers must be at least 1")

        self.conninfo = conninfo
        self.kwargs: Dict[str, Any] = kwargs or {}
        self._configure: Callable[[Connection], None]
        self._configure = configure or (lambda conn: None)
        self.name = name
        self.minconn = minconn
        self.maxconn = maxconn
        self.timeout_sec = timeout_sec
        self.num_workers = num_workers

        self._nconns = 0  # currently in the pool, out, being prepared
        self._pool: List[Connection] = []
        self._waiting: Deque["WaitingClient"] = deque()
        self._lock = threading.Lock()
        self._closed = False

        self._wqueue: "Queue[MaintenanceTask]" = Queue()
        self._workers: List[threading.Thread] = []
        for i in range(num_workers):
            t = threading.Thread(target=self.worker, args=(self._wqueue,))
            t.daemon = True
            t.start()
            self._workers.append(t)

        # Run a task to create the connections immediately
        self.add_task(TopUpConnections(self))

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f" {self.name!r} at 0x{id(self):x}>"
        )

    @contextmanager
    def connection(
        self, timeout_sec: Optional[float] = None
    ) -> Iterator[Connection]:
        """Context manager to obtain a connection from the pool.

        Returned the connection immediately if available, otherwise wait up to
        *timeout_sec* or `self.timeout_sec` and throw `PoolTimeout` if a
        connection is available in time.

        Upon context exit, return the connection to the pool. Apply the normal
        connection context behaviour (commit/rollback the transaction in case
        of success/error). If the connection is no more in working state
        replace it with a new one.
        """
        conn = self.getconn(timeout_sec=timeout_sec)
        try:
            with conn:
                yield conn
        finally:
            self.putconn(conn)

    def getconn(self, timeout_sec: Optional[float] = None) -> Connection:
        """Obtain a contection from the pool.

        You should preferrably use `connection()`. Use this function only if
        it is not possible to use the connection as context manager.

        After using this function you *must* call a corresponding `putconn()`:
        failing to do so will deplete the pool. A depleted pool is a sad pool:
        you don't want a depleted pool.
        """
        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        with self._lock:
            if self._closed:
                raise PoolClosed(f"the pool {self.name!r} is closed")

            pos: Optional[WaitingClient] = None
            if self._pool:
                # Take a connection ready out of the pool
                conn = self._pool.pop(-1)[0]
            else:
                # No connection available: put the client in the waiting queue
                pos = WaitingClient()
                self._waiting.append(pos)

                # If there is space for the pool to grow, let's do it
                if self._nconns < self.maxconn:
                    logger.debug("growing pool %s", self.name)
                    self._nconns += 1
                    self.add_task(AddConnection(self))

        # If we are in the waiting queue, wait to be assigned a connection
        # (outside the critical section, so only the waiting client is locked)
        if pos:
            if timeout_sec is None:
                timeout_sec = self.timeout_sec
            conn = pos.wait(timeout=timeout_sec)

        # Tell the connection it belongs to a pool to avoid closing on __exit__
        # Note that this property shouldn't be set while the connection is in
        # the pool, to avoid to create a reference loop.
        conn._pool = self
        return conn

    def putconn(self, conn: Connection) -> None:
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

        # If the pool is closed just close the connection instead of returning
        # it to the poo. For extra refcare remove the pool reference from it.
        if self._closed:
            conn._pool = None
            conn.close()
            return

        # Use a worker to perform eventual maintenance work in a separate thread
        self.add_task(ReturnConnection(self, conn))

    def _return_connection(self, conn: Connection) -> None:
        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        # Also disable the warning for open connection in conn.__del__
        conn._pool = None

        self._reset_transaction_status(conn)
        if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
            # Connection no more in working state: create a new one.
            logger.warning("discarding closed connection: %s", conn)
            self.add_task(AddConnection(self))
            return

        # Critical section: if there is a client waiting give it the connection
        # otherwise put it back into the pool.
        with self._lock:
            pos: Optional[WaitingClient] = None
            if self._waiting:
                # Extract the first client from the queue
                pos = self._waiting.popleft()
            else:
                # No client waiting for a connection: put it back into the pool
                self._pool.append(conn)

        # If we found a client in queue, give it the connection and notify it
        if pos:
            pos.set(conn)

    def _reset_transaction_status(self, conn: Connection) -> None:
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
                conn.rollback()
            except Exception as e:
                logger.warning(
                    "rollback failed: %s: %s. Discarding connection %s",
                    e.__class__.__name__,
                    e,
                    conn,
                )
                conn.close()

        elif status == TransactionStatus.ACTIVE:
            # Connection returned during an operation. Bad... just close it.
            logger.warning("closing returned connection: %s", conn)
            conn.close()

    @property
    def closed(self) -> bool:
        """`!True` if the pool is closed."""
        return self._closed

    def close(self) -> None:
        """Close the pool and make it unavailable to new clients.

        All the waiting and future client will fail to acquire a connection
        with a `PoolClosed` exception. Currently used connections will not be
        closed until returned to the pool.
        """
        with self._lock:
            self._closed = True

        # Now that the flag _closed is set, getconn will fail immediately,
        # putconn will just close the returned connection.

        # Signal to eventual clients in the queue that business is closed.
        while self._waiting:
            pos = self._waiting.popleft()
            pos.fail(PoolClosed(f"the pool {self.name!r} is closed"))

        # Close the connections still in the pool
        while self._pool:
            conn = self._pool.pop(-1)
            conn.close()

        # Stop the worker threads
        for i in range(len(self._workers)):
            self.add_task(StopWorker(self))

    def add_task(self, task: "MaintenanceTask") -> None:
        """Add a task to the queue of tasts to perform."""
        self._wqueue.put(task)

    @classmethod
    def worker(cls, q: "Queue[MaintenanceTask]") -> None:
        """Runner to execute pending maintenance task.

        The function is designed to run as a separate thread.

        Block on the queue *q*, run a task received. Finish running if a
        StopWorker is received.
        """
        # Don't make all the workers time out at the same moment
        timeout = WORKER_TIMEOUT * (0.9 + 0.1 * random.random())
        while True:
            # Use a timeout to make the wait unterruptable
            try:
                task = q.get(timeout=timeout)
            except Empty:
                continue

            # Run the task. Make sure don't die in the attempt.
            try:
                task.run()
            except Exception as e:
                logger.warning(
                    "task run %s failed: %s: %s", task, e.__class__.__name__, e
                )

            # delete reference loops which may keep the pool alive
            del task.pool
            if isinstance(task, StopWorker):
                return
            del task

    def _connect(self) -> Connection:
        """Return a new connection configured for the pool."""
        conn = Connection.connect(self.conninfo, **self.kwargs)
        self.configure(conn)
        conn._pool = self
        return conn

    def configure(self, conn: Connection) -> None:
        """Configure a connection after creation."""
        self._configure(conn)


class WaitingClient:
    """An position in a queue for a client waiting for a connection."""

    __slots__ = ("event", "conn", "error")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.conn: Connection
        self.error: Optional[Exception] = None

    def wait(self, timeout: float) -> Connection:
        """Wait for the event to be set and return the connection."""
        if not self.event.wait(timeout):
            raise PoolTimeout(f"couldn't get a connection after {timeout} sec")
        if self.error:
            raise self.error
        return self.conn

    def set(self, conn: Connection) -> None:
        """Signal the client waiting that a connection is ready."""
        self.conn = conn
        self.event.set()

    def fail(self, error: Exception) -> None:
        """Signal the client that, alas, they won't have a connection today."""
        self.error = error
        self.event.set()


class MaintenanceTask(ABC):
    """A task run asynchronously to maintain the pool state."""

    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        logger.debug("task created: %s", self)

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} {self.pool.name!r} at 0x{id(self):x}>"
        )

    def run(self) -> None:
        logger.debug("task running: %s", self)
        self._run()

    @abstractmethod
    def _run(self) -> None:
        ...


class StopWorker(MaintenanceTask):
    """Signal the maintenance thread to terminate."""

    def _run(self) -> None:
        pass


class TopUpConnections(MaintenanceTask):
    """Increase the number of connections in the pool to the desired number."""

    def _run(self) -> None:
        with self.pool._lock:
            # Check if there are new connections to create. If there are
            # update the number of connections managed immediately and in
            # the same critical section to avoid finding more than owed
            nconns = self.pool._nconns
            if nconns < self.pool.minconn:
                newconns = self.pool.minconn - nconns
                self.pool._nconns += newconns
            else:
                return

        # enqueue connection creations command so that might be picked in
        # parallel if possible
        for i in range(newconns):
            self.pool.add_task(AddConnection(self.pool))


class AddConnection(MaintenanceTask):
    """Add a new connection into to the pool."""

    def _run(self) -> None:
        conn = self.pool._connect()
        conn._pool = self.pool  # make it accepted by putconn
        self.pool.putconn(conn)


class ReturnConnection(MaintenanceTask):
    """Clean up and return a connection to the pool."""

    def __init__(self, pool: ConnectionPool, conn: Connection):
        super().__init__(pool)
        self.conn = conn

    def _run(self) -> None:
        self.pool._return_connection(self.conn)
