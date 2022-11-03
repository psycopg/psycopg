"""
psycopg asynchronous connection pool
"""

# Copyright (C) 2021 The Psycopg Team

import asyncio
import logging
from abc import ABC, abstractmethod
from time import monotonic
from types import TracebackType
from typing import Any, AsyncIterator, Awaitable, Callable
from typing import Dict, List, Optional, Sequence, Type
from weakref import ref
from contextlib import asynccontextmanager

from psycopg import errors as e
from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .base import ConnectionAttempt, BasePool
from .sched import AsyncScheduler
from .errors import PoolClosed, PoolTimeout, TooManyRequests
from ._compat import Task, create_task, Deque

logger = logging.getLogger("psycopg.pool")


class AsyncConnectionPool(BasePool[AsyncConnection[Any]]):
    def __init__(
        self,
        conninfo: str = "",
        *,
        open: bool = True,
        connection_class: Type[AsyncConnection[Any]] = AsyncConnection,
        configure: Optional[Callable[[AsyncConnection[Any]], Awaitable[None]]] = None,
        reset: Optional[Callable[[AsyncConnection[Any]], Awaitable[None]]] = None,
        **kwargs: Any,
    ):
        self.connection_class = connection_class
        self._configure = configure
        self._reset = reset

        # asyncio objects, created on open to attach them to the right loop.
        self._lock: asyncio.Lock
        self._sched: AsyncScheduler
        self._tasks: "asyncio.Queue[MaintenanceTask]"

        self._waiting = Deque["AsyncClient"]()

        # to notify that the pool is full
        self._pool_full_event: Optional[asyncio.Event] = None

        self._sched_runner: Optional[Task[None]] = None
        self._workers: List[Task[None]] = []

        super().__init__(conninfo, **kwargs)

        if open:
            self._open()

    async def wait(self, timeout: float = 30.0) -> None:
        self._check_open_getconn()

        async with self._lock:
            assert not self._pool_full_event
            if len(self._pool) >= self._min_size:
                return
            self._pool_full_event = asyncio.Event()

        logger.info("waiting for pool %r initialization", self.name)
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

    @asynccontextmanager
    async def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[AsyncConnection[Any]]:
        conn = await self.getconn(timeout=timeout)
        t0 = monotonic()
        try:
            async with conn:
                yield conn
        finally:
            t1 = monotonic()
            self._stats[self._USAGE_MS] += int(1000.0 * (t1 - t0))
            await self.putconn(conn)

    async def getconn(self, timeout: Optional[float] = None) -> AsyncConnection[Any]:
        logger.info("connection requested from %r", self.name)
        self._stats[self._REQUESTS_NUM] += 1

        self._check_open_getconn()

        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        async with self._lock:
            conn = await self._get_ready_connection(timeout)
            if not conn:
                # No connection available: put the client in the waiting queue
                t0 = monotonic()
                pos = AsyncClient()
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
                conn = await pos.wait(timeout=timeout)
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

    async def _get_ready_connection(
        self, timeout: Optional[float]
    ) -> Optional[AsyncConnection[Any]]:
        conn: Optional[AsyncConnection[Any]] = None
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
        # Allow only one task at time to grow the pool (or returning
        # connections might be starved).
        if self._nconns < self._max_size and not self._growing:
            self._nconns += 1
            logger.info("growing pool %r to %s", self.name, self._nconns)
            self._growing = True
            self.run_task(AddConnection(self, growing=True))

    async def putconn(self, conn: AsyncConnection[Any]) -> None:
        self._check_pool_putconn(conn)

        logger.info("returning connection to %r", self.name)
        if await self._maybe_close_connection(conn):
            return

        # Use a worker to perform eventual maintenance work in a separate task
        if self._reset:
            self.run_task(ReturnConnection(self, conn))
        else:
            await self._return_connection(conn)

    async def _maybe_close_connection(self, conn: AsyncConnection[Any]) -> bool:
        # If the pool is closed just close the connection instead of returning
        # it to the pool. For extra refcare remove the pool reference from it.
        if not self._closed:
            return False

        conn._pool = None
        await conn.close()
        return True

    async def open(self, wait: bool = False, timeout: float = 30.0) -> None:
        # Make sure the lock is created after there is an event loop
        try:
            self._lock
        except AttributeError:
            self._lock = asyncio.Lock()

        async with self._lock:
            self._open()

        if wait:
            await self.wait(timeout=timeout)

    def _open(self) -> None:
        if not self._closed:
            return

        # Throw a RuntimeError if the pool is open outside a running loop.
        asyncio.get_running_loop()

        self._check_open()

        # Create these objects now to attach them to the right loop.
        # See #219
        self._tasks = asyncio.Queue()
        self._sched = AsyncScheduler()
        # This has been most likely, but not necessarily, created in `open()`.
        try:
            self._lock
        except AttributeError:
            self._lock = asyncio.Lock()

        self._closed = False
        self._opened = True

        self._start_workers()
        self._start_initial_tasks()

    def _start_workers(self) -> None:
        self._sched_runner = create_task(
            self._sched.run(), name=f"{self.name}-scheduler"
        )
        for i in range(self.num_workers):
            t = create_task(
                self.worker(self._tasks),
                name=f"{self.name}-worker-{i}",
            )
            self._workers.append(t)

    def _start_initial_tasks(self) -> None:
        # populate the pool with initial min_size connections in background
        for i in range(self._nconns):
            self.run_task(AddConnection(self))

        # Schedule a task to shrink the pool if connections over min_size have
        # remained unused.
        self.run_task(Schedule(self, ShrinkPool(self), self.max_idle))

    async def close(self, timeout: float = 5.0) -> None:
        if self._closed:
            return

        async with self._lock:
            self._closed = True
            logger.debug("pool %r closed", self.name)

            # Take waiting client and pool connections out of the state
            waiting = list(self._waiting)
            self._waiting.clear()
            connections = list(self._pool)
            self._pool.clear()

        # Now that the flag _closed is set, getconn will fail immediately,
        # putconn will just close the returned connection.
        await self._stop_workers(waiting, connections, timeout)

    async def _stop_workers(
        self,
        waiting_clients: Sequence["AsyncClient"] = (),
        connections: Sequence[AsyncConnection[Any]] = (),
        timeout: float = 0.0,
    ) -> None:
        # Stop the scheduler
        await self._sched.enter(0, None)

        # Stop the worker tasks
        workers, self._workers = self._workers[:], []
        for w in workers:
            self.run_task(StopWorker(self))

        # Signal to eventual clients in the queue that business is closed.
        for pos in waiting_clients:
            await pos.fail(PoolClosed(f"the pool {self.name!r} is closed"))

        # Close the connections still in the pool
        for conn in connections:
            await conn.close()

        # Wait for the worker tasks to terminate
        assert self._sched_runner is not None
        sched_runner, self._sched_runner = self._sched_runner, None
        wait = asyncio.gather(sched_runner, *workers)
        try:
            if timeout > 0:
                await asyncio.wait_for(asyncio.shield(wait), timeout=timeout)
            else:
                await wait
        except asyncio.TimeoutError:
            logger.warning(
                "couldn't stop pool %r tasks within %s seconds",
                self.name,
                timeout,
            )

    async def __aenter__(self) -> "AsyncConnectionPool":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def resize(self, min_size: int, max_size: Optional[int] = None) -> None:
        min_size, max_size = self._check_size(min_size, max_size)

        ngrow = max(0, min_size - self._min_size)

        logger.info(
            "resizing %r to min_size=%s max_size=%s",
            self.name,
            min_size,
            max_size,
        )
        async with self._lock:
            self._min_size = min_size
            self._max_size = max_size
            self._nconns += ngrow

        for i in range(ngrow):
            self.run_task(AddConnection(self))

    async def check(self) -> None:
        async with self._lock:
            conns = list(self._pool)
            self._pool.clear()

        while conns:
            conn = conns.pop()
            try:
                await conn.execute("SELECT 1")
                if conn.pgconn.transaction_status == TransactionStatus.INTRANS:
                    await conn.rollback()
            except Exception:
                self._stats[self._CONNECTIONS_LOST] += 1
                logger.warning("discarding broken connection: %s", conn)
                self.run_task(AddConnection(self))
            else:
                await self._add_to_pool(conn)

    def reconnect_failed(self) -> None:
        """
        Called when reconnection failed for longer than `reconnect_timeout`.
        """
        self._reconnect_failed(self)

    def run_task(self, task: "MaintenanceTask") -> None:
        """Run a maintenance task in a worker."""
        self._tasks.put_nowait(task)

    async def schedule_task(self, task: "MaintenanceTask", delay: float) -> None:
        """Run a maintenance task in a worker in the future."""
        await self._sched.enter(delay, task.tick)

    @classmethod
    async def worker(cls, q: "asyncio.Queue[MaintenanceTask]") -> None:
        """Runner to execute pending maintenance task.

        The function is designed to run as a task.

        Block on the queue *q*, run a task received. Finish running if a
        StopWorker is received.
        """
        while True:
            task = await q.get()

            if isinstance(task, StopWorker):
                logger.debug("terminating working task")
                return

            # Run the task. Make sure don't die in the attempt.
            try:
                await task.run()
            except Exception as ex:
                logger.warning(
                    "task run %s failed: %s: %s",
                    task,
                    ex.__class__.__name__,
                    ex,
                )

    async def _connect(self, timeout: Optional[float] = None) -> AsyncConnection[Any]:
        self._stats[self._CONNECTIONS_NUM] += 1
        kwargs = self.kwargs
        if timeout:
            kwargs = kwargs.copy()
            kwargs["connect_timeout"] = max(round(timeout), 1)
        t0 = monotonic()
        try:
            conn: AsyncConnection[Any]
            conn = await self.connection_class.connect(self.conninfo, **kwargs)
        except Exception:
            self._stats[self._CONNECTIONS_ERRORS] += 1
            raise
        else:
            t1 = monotonic()
            self._stats[self._CONNECTIONS_MS] += int(1000.0 * (t1 - t0))

        conn._pool = self

        if self._configure:
            await self._configure(conn)
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

    async def _add_connection(
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
            conn = await self._connect()
        except Exception as ex:
            logger.warning(f"error connecting in {self.name!r}: {ex}")
            if attempt.time_to_give_up(now):
                logger.warning(
                    "reconnection attempt in pool %r failed after %s sec",
                    self.name,
                    self.reconnect_timeout,
                )
                async with self._lock:
                    self._nconns -= 1
                    # If we have given up with a growing attempt, allow a new one.
                    if growing and self._growing:
                        self._growing = False
                self.reconnect_failed()
            else:
                attempt.update_delay(now)
                await self.schedule_task(
                    AddConnection(self, attempt, growing=growing),
                    attempt.delay,
                )
            return

        logger.info("adding new connection to the pool")
        await self._add_to_pool(conn)
        if growing:
            async with self._lock:
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

    async def _return_connection(self, conn: AsyncConnection[Any]) -> None:
        """
        Return a connection to the pool after usage.
        """
        await self._reset_connection(conn)
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
            await conn.close()
            return

        await self._add_to_pool(conn)

    async def _add_to_pool(self, conn: AsyncConnection[Any]) -> None:
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
                if self._pool_full_event and len(self._pool) >= self._min_size:
                    self._pool_full_event.set()

    async def _reset_connection(self, conn: AsyncConnection[Any]) -> None:
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
                await conn.rollback()
            except Exception as ex:
                logger.warning(
                    "rollback failed: %s: %s. Discarding connection %s",
                    ex.__class__.__name__,
                    ex,
                    conn,
                )
                await conn.close()

        elif status == TransactionStatus.ACTIVE:
            # Connection returned during an operation. Bad... just close it.
            logger.warning("closing returned connection: %s", conn)
            await conn.close()

        if not conn.closed and self._reset:
            try:
                await self._reset(conn)
                status = conn.pgconn.transaction_status
                if status != TransactionStatus.IDLE:
                    sname = TransactionStatus(status).name
                    raise e.ProgrammingError(
                        f"connection left in status {sname} by reset function"
                        f" {self._reset}: discarded"
                    )
            except Exception as ex:
                logger.warning(f"error resetting connection: {ex}")
                await conn.close()

    async def _shrink_pool(self) -> None:
        to_close: Optional[AsyncConnection[Any]] = None

        async with self._lock:
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
            await to_close.close()

    def _get_measures(self) -> Dict[str, int]:
        rv = super()._get_measures()
        rv[self._REQUESTS_WAITING] = len(self._waiting)
        return rv


class AsyncClient:
    """A position in a queue for a client waiting for a connection."""

    __slots__ = ("conn", "error", "_cond")

    def __init__(self) -> None:
        self.conn: Optional[AsyncConnection[Any]] = None
        self.error: Optional[Exception] = None

        # The AsyncClient behaves in a way similar to an Event, but we need
        # to notify reliably the flagger that the waiter has "accepted" the
        # message and it hasn't timed out yet, otherwise the pool may give a
        # connection to a client that has already timed out getconn(), which
        # will be lost.
        self._cond = asyncio.Condition()

    async def wait(self, timeout: float) -> AsyncConnection[Any]:
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

    async def set(self, conn: AsyncConnection[Any]) -> bool:
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


class MaintenanceTask(ABC):
    """A task to run asynchronously to maintain the pool state."""

    def __init__(self, pool: "AsyncConnectionPool"):
        self.pool = ref(pool)

    def __repr__(self) -> str:
        pool = self.pool()
        name = repr(pool.name) if pool else "<pool is gone>"
        return f"<{self.__class__.__name__} {name} at 0x{id(self):x}>"

    async def run(self) -> None:
        """Run the task.

        This usually happens in a worker. Call the concrete _run()
        implementation, if the pool is still alive.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            logger.debug("task run discarded: %s", self)
            return

        await self._run(pool)

    async def tick(self) -> None:
        """Run the scheduled task

        This function is called by the scheduler task. Use a worker to
        run the task for real in order to free the scheduler immediately.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            logger.debug("task tick discarded: %s", self)
            return

        pool.run_task(self)

    @abstractmethod
    async def _run(self, pool: "AsyncConnectionPool") -> None:
        ...


class StopWorker(MaintenanceTask):
    """Signal the maintenance worker to terminate."""

    async def _run(self, pool: "AsyncConnectionPool") -> None:
        pass


class AddConnection(MaintenanceTask):
    def __init__(
        self,
        pool: "AsyncConnectionPool",
        attempt: Optional["ConnectionAttempt"] = None,
        growing: bool = False,
    ):
        super().__init__(pool)
        self.attempt = attempt
        self.growing = growing

    async def _run(self, pool: "AsyncConnectionPool") -> None:
        await pool._add_connection(self.attempt, growing=self.growing)


class ReturnConnection(MaintenanceTask):
    """Clean up and return a connection to the pool."""

    def __init__(self, pool: "AsyncConnectionPool", conn: "AsyncConnection[Any]"):
        super().__init__(pool)
        self.conn = conn

    async def _run(self, pool: "AsyncConnectionPool") -> None:
        await pool._return_connection(self.conn)


class ShrinkPool(MaintenanceTask):
    """If the pool can shrink, remove one connection.

    Re-schedule periodically and also reset the minimum number of connections
    in the pool.
    """

    async def _run(self, pool: "AsyncConnectionPool") -> None:
        # Reschedule the task now so that in case of any error we don't lose
        # the periodic run.
        await pool.schedule_task(self, pool.max_idle)
        await pool._shrink_pool()


class Schedule(MaintenanceTask):
    """Schedule a task in the pool scheduler.

    This task is a trampoline to allow to use a sync call (pool.run_task)
    to execute an async one (pool.schedule_task).
    """

    def __init__(
        self,
        pool: "AsyncConnectionPool",
        task: MaintenanceTask,
        delay: float,
    ):
        super().__init__(pool)
        self.task = task
        self.delay = delay

    async def _run(self, pool: "AsyncConnectionPool") -> None:
        await pool.schedule_task(self.task, self.delay)
