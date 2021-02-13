"""
psycopg3 connection pool
"""

# Copyright (C) 2021 The Psycopg Team

import random
import logging
import threading
from queue import Queue, Empty
from typing import Any, Callable, Deque, Dict, Iterator, List, Optional
from contextlib import contextmanager
from collections import deque

from . import errors as e
from .connection import Connection

WORKER_TIMEOUT = 60.0

logger = logging.getLogger(__name__)


class PoolTimeout(e.OperationalError):
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
        nworkers: int = 1,
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

        self.conninfo = conninfo
        self.kwargs: Dict[str, Any] = kwargs or {}
        self._configure: Callable[[Connection], None]
        self._configure = configure or (lambda conn: None)
        self.name = name
        self.minconn = minconn
        self.maxconn = maxconn
        self.timeout_sec = timeout_sec
        self.nworkers = nworkers

        self._nconns = 0  # currently in the pool, out, being prepared
        self._pool: List[Connection] = []
        self._waiting: Deque["WaitingClient"] = deque()
        self._lock = threading.Lock()

        self._wqueue: "Queue[MaintenanceTask]" = Queue()
        self._workers: List[threading.Thread] = []
        for i in range(nworkers):
            t = threading.Thread(target=self.worker, args=(self._wqueue,))
            t.daemon = True
            t.start()
            self._workers.append(t)

        # Run a task to create the connections immediately
        self.add_task(TopUpConnections(self))

    @contextmanager
    def connection(
        self, timeout_sec: Optional[float] = None
    ) -> Iterator[Connection]:
        conn = self.getconn(timeout_sec=timeout_sec)
        try:
            with conn:
                yield conn
        finally:
            self.putconn(conn)

    def getconn(self, timeout_sec: Optional[float] = None) -> Connection:
        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        with self._lock:
            pos: Optional[WaitingClient] = None
            if self._pool:
                # Take a connection ready out of the pool
                conn = self._pool.pop(-1)
            else:
                # No connection available: put the client in the waiting queue
                pos = WaitingClient()
                self._waiting.append(pos)

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
        # TODO: this should happen in a maintenance thread
        # TODO: add check for broken connections

        if conn._pool is not self:
            if conn._pool:
                raise ValueError(f"the connection belongs to {conn._pool}")
            else:
                raise ValueError("the connection doesn't belong to a pool")

        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        conn._pool = None

        # Critical section: if there is a client waiting give it the connection
        # otherwise put it back into the pool.
        with self._lock:
            if self._waiting:
                # Give the connection to the client and notify it
                pos = self._waiting.popleft()
                pos.set(conn)
            else:
                # No client waiting for a connection: put it back into the queue
                self._pool.append(conn)

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
                task()
            except Exception as e:
                logger.warning(
                    "task run %s failed: %s: %s", task, e.__class__.__name__, e
                )
            if isinstance(task, StopWorker):
                return

    def _connect(self) -> Connection:
        """Return a connection configured for the pool."""
        conn = Connection.connect(self.conninfo, **self.kwargs)
        self.configure(conn)
        conn._pool = self
        return conn

    def configure(self, conn: Connection) -> None:
        """Configure a connection after creation."""
        self._configure(conn)


class WaitingClient:
    """An position in a queue for a client waiting for a connection."""

    __slots__ = ("event", "conn")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.conn: Connection

    def wait(self, timeout: float) -> Connection:
        """Wait for the event to be set and return the connection."""
        if not self.event.wait(timeout):
            raise PoolTimeout(f"couldn't get a connection after {timeout} sec")
        return self.conn

    def set(self, conn: Connection) -> None:
        self.conn = conn
        self.event.set()


class MaintenanceTask:
    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        logger.debug("task created: %s", self)

    def __call__(self) -> None:
        logger.debug("task running: %s", self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.pool.name})"


class StopWorker(MaintenanceTask):
    pass


class TopUpConnections(MaintenanceTask):
    def __call__(self) -> None:
        super().__call__()

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
    def __call__(self) -> None:
        super().__call__()

        conn = self.pool._connect()
        conn._pool = self.pool  # make it acceptable
        self.pool.putconn(conn)
