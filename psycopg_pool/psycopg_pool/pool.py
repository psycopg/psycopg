"""
psycopg synchronous connection pool
"""

# Copyright (C) 2021 The Psycopg Team

import logging
import threading
from abc import ABC, abstractmethod
from time import monotonic
from queue import Queue, Empty
from types import TracebackType
from typing import Any, Callable, Dict, Iterator, List
from typing import Optional, Sequence, Type
from weakref import ref
from contextlib import contextmanager

from psycopg import errors as e
from psycopg import Connection
from psycopg.pq import TransactionStatus

from .base import ConnectionAttempt, BasePool
from .sched import Scheduler
from .errors import PoolClosed, PoolTimeout, TooManyRequests
from ._compat import Deque

logger = logging.getLogger("psycopg.pool")


class ConnectionPool(BasePool[Connection[Any]]):
    def __init__(
        self,
        conninfo: str = "",
        *,
        open: bool = True,
        connection_class: Type[Connection[Any]] = Connection,
        configure: Optional[Callable[[Connection[Any]], None]] = None,
        reset: Optional[Callable[[Connection[Any]], None]] = None,
        **kwargs: Any,
    ):
        self.connection_class = connection_class
        self._configure = configure
        self._reset = reset

        self._lock = threading.RLock()
        self._waiting = Deque["WaitingClient"]()

        # to notify that the pool is full
        self._pool_full_event: Optional[threading.Event] = None

        self._sched = Scheduler()
        self._sched_runner: Optional[threading.Thread] = None
        self._tasks: "Queue[MaintenanceTask]" = Queue()
        self._workers: List[threading.Thread] = []

        super().__init__(conninfo, **kwargs)

        if open:
            self.open()

    def __del__(self) -> None:
        # If the '_closed' property is not set we probably failed in __init__.
        # Don't try anything complicated as probably it won't work.
        if getattr(self, "_closed", True):
            return

        self._stop_workers()

    def wait(self, timeout: float = 30.0) -> None:
        """
        Wait for the pool to be full (with `min_size` connections) after creation.

        Close the pool, and raise `PoolTimeout`, if not ready within *timeout*
        sec.

        Calling this method is not mandatory: you can try and use the pool
        immediately after its creation. The first client will be served as soon
        as a connection is ready. You can use this method if you prefer your
        program to terminate in case the environment is not configured
        properly, rather than trying to stay up the hardest it can.
        """
        self._check_open_getconn()

        with self._lock:
            assert not self._pool_full_event
            if len(self._pool) >= self._min_size:
                return
            self._pool_full_event = threading.Event()

        logger.info("waiting for pool %r initialization", self.name)
        if not self._pool_full_event.wait(timeout):
            self.close()  # stop all the threads
            raise PoolTimeout(f"pool initialization incomplete after {timeout} sec")

        with self._lock:
            assert self._pool_full_event
            self._pool_full_event = None

        logger.info("pool %r is ready to use", self.name)

    @contextmanager
    def connection(self, timeout: Optional[float] = None) -> Iterator[Connection[Any]]:
        """Context manager to obtain a connection from the pool.

        Return the connection immediately if available, otherwise wait up to
        *timeout* or `self.timeout` seconds and throw `PoolTimeout` if a
        connection is not available in time.

        Upon context exit, return the connection to the pool. Apply the normal
        :ref:`connection context behaviour <with-connection>` (commit/rollback
        the transaction in case of success/error). If the connection is no more
        in working state, replace it with a new one.
        """
        conn = self.getconn(timeout=timeout)
        t0 = monotonic()
        try:
            with conn:
                yield conn
        finally:
            t1 = monotonic()
            self._stats[self._USAGE_MS] += int(1000.0 * (t1 - t0))
            self.putconn(conn)

    def getconn(self, timeout: Optional[float] = None) -> Connection[Any]:
        """Obtain a connection from the pool.

        You should preferably use `connection()`. Use this function only if
        it is not possible to use the connection as context manager.

        After using this function you *must* call a corresponding `putconn()`:
        failing to do so will deplete the pool. A depleted pool is a sad pool:
        you don't want a depleted pool.
        """
        logger.info("connection requested from %r", self.name)
        self._stats[self._REQUESTS_NUM] += 1

        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        with self._lock:
            self._check_open_getconn()
            conn = self._get_ready_connection(timeout)
            if not conn:
                # No connection available: put the client in the waiting queue
                t0 = monotonic()
                pos = WaitingClient()
                self._waiting.append(pos)
                self._stats[self._REQUESTS_QUEUED] += 1

                # If there is space for the pool to grow, let's do it
                self._maybe_grow_pool()

        # If we are in the waiting queue, wait to be assigned a connection
        # (outside the critical section, so only the waiting client is locked)
        if not conn:
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
        # Note that this property shouldn't be set while the connection is in
        # the pool, to avoid to create a reference loop.
        conn._pool = self
        logger.info("connection given by %r", self.name)
        return conn

    def _get_ready_connection(
        self, timeout: Optional[float]
    ) -> Optional[Connection[Any]]:
        """Return a connection, if the client deserves one."""
        conn: Optional[Connection[Any]] = None
        if self._pool:
            # Take a connection ready out of the pool
            conn = self._pool.popleft()
            if len(self._pool) < self._nconns_min:
                self._nconns_min = len(self._pool)
        elif self.max_waiting and len(self._waiting) >= self.max_waiting:
            self._stats[self._REQUESTS_ERRORS] += 1
            raise TooManyRequests(
                f"the pool {self.name!r} has already"
                f" {len(self._waiting)} requests waiting"
            )
        return conn

    def _maybe_grow_pool(self) -> None:
        # Allow only one thread at time to grow the pool (or returning
        # connections might be starved).
        if self._nconns >= self._max_size or self._growing:
            return
        self._nconns += 1
        logger.info("growing pool %r to %s", self.name, self._nconns)
        self._growing = True
        self.run_task(AddConnection(self, growing=True))

    def putconn(self, conn: Connection[Any]) -> None:
        """Return a connection to the loving hands of its pool.

        Use this function only paired with a `getconn()`. You don't need to use
        it if you use the much more comfortable `connection()` context manager.
        """
        # Quick check to discard the wrong connection
        self._check_pool_putconn(conn)

        logger.info("returning connection to %r", self.name)

        if self._maybe_close_connection(conn):
            return

        # Use a worker to perform eventual maintenance work in a separate thread
        if self._reset:
            self.run_task(ReturnConnection(self, conn))
        else:
            self._return_connection(conn)

    def _maybe_close_connection(self, conn: Connection[Any]) -> bool:
        """Close a returned connection if necessary.

        Return `!True if the connection was closed.
        """
        # If the pool is closed just close the connection instead of returning
        # it to the pool. For extra refcare remove the pool reference from it.
        if not self._closed:
            return False

        conn._pool = None
        conn.close()
        return True

    def open(self, wait: bool = False, timeout: float = 30.0) -> None:
        """Open the pool by starting connecting and and accepting clients.

        If *wait* is `!False`, return immediately and let the background worker
        fill the pool if `min_size` > 0. Otherwise wait up to *timeout* seconds
        for the requested number of connections to be ready (see `wait()` for
        details).

        It is safe to call `!open()` again on a pool already open (because the
        method was already called, or because the pool context was entered, or
        because the pool was initialized with *open* = `!True`) but you cannot
        currently re-open a closed pool.
        """
        with self._lock:
            self._open()

        if wait:
            self.wait(timeout=timeout)

    def _open(self) -> None:
        if not self._closed:
            return

        self._check_open()

        self._closed = False
        self._opened = True

        self._start_workers()
        self._start_initial_tasks()

    def _start_workers(self) -> None:
        self._sched_runner = threading.Thread(
            target=self._sched.run,
            name=f"{self.name}-scheduler",
            daemon=True,
        )
        assert not self._workers
        for i in range(self.num_workers):
            t = threading.Thread(
                target=self.worker,
                args=(self._tasks,),
                name=f"{self.name}-worker-{i}",
                daemon=True,
            )
            self._workers.append(t)

        # The object state is complete. Start the worker threads
        self._sched_runner.start()
        for t in self._workers:
            t.start()

    def _start_initial_tasks(self) -> None:
        # populate the pool with initial min_size connections in background
        for i in range(self._nconns):
            self.run_task(AddConnection(self))

        # Schedule a task to shrink the pool if connections over min_size have
        # remained unused.
        self.schedule_task(ShrinkPool(self), self.max_idle)

    def close(self, timeout: float = 5.0) -> None:
        """Close the pool and make it unavailable to new clients.

        All the waiting and future clients will fail to acquire a connection
        with a `PoolClosed` exception. Currently used connections will not be
        closed until returned to the pool.

        Wait *timeout* seconds for threads to terminate their job, if positive.
        If the timeout expires the pool is closed anyway, although it may raise
        some warnings on exit.
        """
        if self._closed:
            return

        with self._lock:
            self._closed = True
            logger.debug("pool %r closed", self.name)

            # Take waiting client and pool connections out of the state
            waiting = list(self._waiting)
            self._waiting.clear()
            connections = list(self._pool)
            self._pool.clear()

        # Now that the flag _closed is set, getconn will fail immediately,
        # putconn will just close the returned connection.
        self._stop_workers(waiting, connections, timeout)

    def _stop_workers(
        self,
        waiting_clients: Sequence["WaitingClient"] = (),
        connections: Sequence[Connection[Any]] = (),
        timeout: float = 0.0,
    ) -> None:

        # Stop the scheduler
        self._sched.enter(0, None)

        # Stop the worker threads
        workers, self._workers = self._workers[:], []
        for i in range(len(workers)):
            self.run_task(StopWorker(self))

        # Signal to eventual clients in the queue that business is closed.
        for pos in waiting_clients:
            pos.fail(PoolClosed(f"the pool {self.name!r} is closed"))

        # Close the connections still in the pool
        for conn in connections:
            conn.close()

        # Wait for the worker threads to terminate
        assert self._sched_runner is not None
        sched_runner, self._sched_runner = self._sched_runner, None
        if timeout > 0:
            for t in [sched_runner] + workers:
                if not t.is_alive():
                    continue
                t.join(timeout)
                if t.is_alive():
                    logger.warning(
                        "couldn't stop thread %s in pool %r within %s seconds",
                        t,
                        self.name,
                        timeout,
                    )

    def __enter__(self) -> "ConnectionPool":
        self.open()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def resize(self, min_size: int, max_size: Optional[int] = None) -> None:
        """Change the size of the pool during runtime."""
        min_size, max_size = self._check_size(min_size, max_size)

        ngrow = max(0, min_size - self._min_size)

        logger.info(
            "resizing %r to min_size=%s max_size=%s",
            self.name,
            min_size,
            max_size,
        )
        with self._lock:
            self._min_size = min_size
            self._max_size = max_size
            self._nconns += ngrow

        for i in range(ngrow):
            self.run_task(AddConnection(self))

    def check(self) -> None:
        """Verify the state of the connections currently in the pool.

        Test each connection: if it works return it to the pool, otherwise
        dispose of it and create a new one.
        """
        with self._lock:
            conns = list(self._pool)
            self._pool.clear()

        while conns:
            conn = conns.pop()
            try:
                conn.execute("SELECT 1")
                if conn.pgconn.transaction_status == TransactionStatus.INTRANS:
                    conn.rollback()
            except Exception:
                self._stats[self._CONNECTIONS_LOST] += 1
                logger.warning("discarding broken connection: %s", conn)
                self.run_task(AddConnection(self))
            else:
                self._add_to_pool(conn)

    def reconnect_failed(self) -> None:
        """
        Called when reconnection failed for longer than `reconnect_timeout`.
        """
        self._reconnect_failed(self)

    def run_task(self, task: "MaintenanceTask") -> None:
        """Run a maintenance task in a worker thread."""
        self._tasks.put_nowait(task)

    def schedule_task(self, task: "MaintenanceTask", delay: float) -> None:
        """Run a maintenance task in a worker thread in the future."""
        self._sched.enter(delay, task.tick)

    _WORKER_TIMEOUT = 60.0

    @classmethod
    def worker(cls, q: "Queue[MaintenanceTask]") -> None:
        """Runner to execute pending maintenance task.

        The function is designed to run as a separate thread.

        Block on the queue *q*, run a task received. Finish running if a
        StopWorker is received.
        """
        # Don't make all the workers time out at the same moment
        timeout = cls._jitter(cls._WORKER_TIMEOUT, -0.1, 0.1)
        while True:
            # Use a timeout to make the wait interruptible
            try:
                task = q.get(timeout=timeout)
            except Empty:
                continue

            if isinstance(task, StopWorker):
                logger.debug(
                    "terminating working thread %s",
                    threading.current_thread().name,
                )
                return

            # Run the task. Make sure don't die in the attempt.
            try:
                task.run()
            except Exception as ex:
                logger.warning(
                    "task run %s failed: %s: %s",
                    task,
                    ex.__class__.__name__,
                    ex,
                )

    def _connect(self, timeout: Optional[float] = None) -> Connection[Any]:
        """Return a new connection configured for the pool."""
        self._stats[self._CONNECTIONS_NUM] += 1
        kwargs = self.kwargs
        if timeout:
            kwargs = kwargs.copy()
            kwargs["connect_timeout"] = max(round(timeout), 1)
        t0 = monotonic()
        try:
            conn: Connection[Any]
            conn = self.connection_class.connect(self.conninfo, **kwargs)
        except Exception:
            self._stats[self._CONNECTIONS_ERRORS] += 1
            raise
        else:
            t1 = monotonic()
            self._stats[self._CONNECTIONS_MS] += int(1000.0 * (t1 - t0))

        conn._pool = self

        if self._configure:
            self._configure(conn)
            status = conn.pgconn.transaction_status
            if status != TransactionStatus.IDLE:
                sname = TransactionStatus(status).name
                raise e.ProgrammingError(
                    f"connection left in status {sname} by configure function"
                    f" {self._configure}: discarded"
                )

        # Set an expiry date, with some randomness to avoid mass reconnection
        self._set_connection_expiry_date(conn)
        return conn

    def _add_connection(
        self, attempt: Optional[ConnectionAttempt], growing: bool = False
    ) -> None:
        """Try to connect and add the connection to the pool.

        If failed, reschedule a new attempt in the future for a few times, then
        give up, decrease the pool connections number and call
        `self.reconnect_failed()`.

        """
        now = monotonic()
        if not attempt:
            attempt = ConnectionAttempt(reconnect_timeout=self.reconnect_timeout)

        try:
            conn = self._connect()
        except Exception as ex:
            logger.warning(f"error connecting in {self.name!r}: {ex}")
            if attempt.time_to_give_up(now):
                logger.warning(
                    "reconnection attempt in pool %r failed after %s sec",
                    self.name,
                    self.reconnect_timeout,
                )
                with self._lock:
                    self._nconns -= 1
                    # If we have given up with a growing attempt, allow a new one.
                    if growing and self._growing:
                        self._growing = False
                self.reconnect_failed()
            else:
                attempt.update_delay(now)
                self.schedule_task(
                    AddConnection(self, attempt, growing=growing),
                    attempt.delay,
                )
            return

        logger.info("adding new connection to the pool")
        self._add_to_pool(conn)
        if growing:
            with self._lock:
                # Keep on growing if the pool is not full yet, or if there are
                # clients waiting and the pool can extend.
                if self._nconns < self._min_size or (
                    self._nconns < self._max_size and self._waiting
                ):
                    self._nconns += 1
                    logger.info("growing pool %r to %s", self.name, self._nconns)
                    self.run_task(AddConnection(self, growing=True))
                else:
                    self._growing = False

    def _return_connection(self, conn: Connection[Any]) -> None:
        """
        Return a connection to the pool after usage.
        """
        self._reset_connection(conn)
        if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
            self._stats[self._RETURNS_BAD] += 1
            # Connection no more in working state: create a new one.
            self.run_task(AddConnection(self))
            logger.warning("discarding closed connection: %s", conn)
            return

        # Check if the connection is past its best before date
        if conn._expire_at <= monotonic():
            self.run_task(AddConnection(self))
            logger.info("discarding expired connection")
            conn.close()
            return

        self._add_to_pool(conn)

    def _add_to_pool(self, conn: Connection[Any]) -> None:
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
                # No client waiting for a connection: put it back into the pool
                self._pool.append(conn)

                # If we have been asked to wait for pool init, notify the
                # waiter if the pool is full.
                if self._pool_full_event and len(self._pool) >= self._min_size:
                    self._pool_full_event.set()

    def _reset_connection(self, conn: Connection[Any]) -> None:
        """
        Bring a connection to IDLE state or close it.
        """
        status = conn.pgconn.transaction_status
        if status == TransactionStatus.IDLE:
            pass

        elif status in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
            # Connection returned with an active transaction
            logger.warning("rolling back returned connection: %s", conn)
            try:
                conn.rollback()
            except Exception as ex:
                logger.warning(
                    "rollback failed: %s: %s. Discarding connection %s",
                    ex.__class__.__name__,
                    ex,
                    conn,
                )
                conn.close()

        elif status == TransactionStatus.ACTIVE:
            # Connection returned during an operation. Bad... just close it.
            logger.warning("closing returned connection: %s", conn)
            conn.close()

        if not conn.closed and self._reset:
            try:
                self._reset(conn)
                status = conn.pgconn.transaction_status
                if status != TransactionStatus.IDLE:
                    sname = TransactionStatus(status).name
                    raise e.ProgrammingError(
                        f"connection left in status {sname} by reset function"
                        f" {self._reset}: discarded"
                    )
            except Exception as ex:
                logger.warning(f"error resetting connection: {ex}")
                conn.close()

    def _shrink_pool(self) -> None:
        to_close: Optional[Connection[Any]] = None

        with self._lock:
            # Reset the min number of connections used
            nconns_min = self._nconns_min
            self._nconns_min = len(self._pool)

            # If the pool can shrink and connections were unused, drop one
            if self._nconns > self._min_size and nconns_min > 0:
                to_close = self._pool.popleft()
                self._nconns -= 1
                self._nconns_min -= 1

        if to_close:
            logger.info(
                "shrinking pool %r to %s because %s unused connections"
                " in the last %s sec",
                self.name,
                self._nconns,
                nconns_min,
                self.max_idle,
            )
            to_close.close()

    def _get_measures(self) -> Dict[str, int]:
        rv = super()._get_measures()
        rv[self._REQUESTS_WAITING] = len(self._waiting)
        return rv


class WaitingClient:
    """A position in a queue for a client waiting for a connection."""

    __slots__ = ("conn", "error", "_cond")

    def __init__(self) -> None:
        self.conn: Optional[Connection[Any]] = None
        self.error: Optional[Exception] = None

        # The WaitingClient behaves in a way similar to an Event, but we need
        # to notify reliably the flagger that the waiter has "accepted" the
        # message and it hasn't timed out yet, otherwise the pool may give a
        # connection to a client that has already timed out getconn(), which
        # will be lost.
        self._cond = threading.Condition()

    def wait(self, timeout: float) -> Connection[Any]:
        """Wait for a connection to be set and return it.

        Raise an exception if the wait times out or if fail() is called.
        """
        with self._cond:
            if not (self.conn or self.error):
                if not self._cond.wait(timeout):
                    self.error = PoolTimeout(
                        f"couldn't get a connection after {timeout} sec"
                    )

        if self.conn:
            return self.conn
        else:
            assert self.error
            raise self.error

    def set(self, conn: Connection[Any]) -> bool:
        """Signal the client waiting that a connection is ready.

        Return True if the client has "accepted" the connection, False
        otherwise (typically because wait() has timed out).
        """
        with self._cond:
            if self.conn or self.error:
                return False

            self.conn = conn
            self._cond.notify_all()
            return True

    def fail(self, error: Exception) -> bool:
        """Signal the client that, alas, they won't have a connection today.

        Return True if the client has "accepted" the error, False otherwise
        (typically because wait() has timed out).
        """
        with self._cond:
            if self.conn or self.error:
                return False

            self.error = error
            self._cond.notify_all()
            return True


class MaintenanceTask(ABC):
    """A task to run asynchronously to maintain the pool state."""

    def __init__(self, pool: "ConnectionPool"):
        self.pool = ref(pool)

    def __repr__(self) -> str:
        pool = self.pool()
        name = repr(pool.name) if pool else "<pool is gone>"
        return f"<{self.__class__.__name__} {name} at 0x{id(self):x}>"

    def run(self) -> None:
        """Run the task.

        This usually happens in a worker thread. Call the concrete _run()
        implementation, if the pool is still alive.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            logger.debug("task run discarded: %s", self)
            return

        logger.debug("task running in %s: %s", threading.current_thread().name, self)
        self._run(pool)

    def tick(self) -> None:
        """Run the scheduled task

        This function is called by the scheduler thread. Use a worker to
        run the task for real in order to free the scheduler immediately.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            logger.debug("task tick discarded: %s", self)
            return

        pool.run_task(self)

    @abstractmethod
    def _run(self, pool: "ConnectionPool") -> None:
        ...


class StopWorker(MaintenanceTask):
    """Signal the maintenance thread to terminate."""

    def _run(self, pool: "ConnectionPool") -> None:
        pass


class AddConnection(MaintenanceTask):
    def __init__(
        self,
        pool: "ConnectionPool",
        attempt: Optional["ConnectionAttempt"] = None,
        growing: bool = False,
    ):
        super().__init__(pool)
        self.attempt = attempt
        self.growing = growing

    def _run(self, pool: "ConnectionPool") -> None:
        pool._add_connection(self.attempt, growing=self.growing)


class ReturnConnection(MaintenanceTask):
    """Clean up and return a connection to the pool."""

    def __init__(self, pool: "ConnectionPool", conn: "Connection[Any]"):
        super().__init__(pool)
        self.conn = conn

    def _run(self, pool: "ConnectionPool") -> None:
        pool._return_connection(self.conn)


class ShrinkPool(MaintenanceTask):
    """If the pool can shrink, remove one connection.

    Re-schedule periodically and also reset the minimum number of connections
    in the pool.
    """

    def _run(self, pool: "ConnectionPool") -> None:
        # Reschedule the task now so that in case of any error we don't lose
        # the periodic run.
        pool.schedule_task(self, pool.max_idle)
        pool._shrink_pool()
