"""
Maintenance tasks for the connection pools.
"""

# Copyright (C) 2021 The Psycopg Team

import asyncio
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, cast, Generic, Optional, Type, TYPE_CHECKING
from weakref import ref

from ..proto import ConnectionType
from .. import Connection, AsyncConnection

if TYPE_CHECKING:
    from .pool import ConnectionPool
    from .async_pool import AsyncConnectionPool
    from .base import BasePool, ConnectionAttempt
else:
    # Injected at pool.py and async_pool.py import
    ConnectionPool: "Type[BasePool[Connection]]"
    AsyncConnectionPool: "Type[BasePool[AsyncConnection]]"

logger = logging.getLogger(__name__)


class MaintenanceTask(ABC, Generic[ConnectionType]):
    """A task to run asynchronously to maintain the pool state."""

    TIMEOUT = 10.0

    def __init__(self, pool: "BasePool[Any]"):
        if isinstance(pool, AsyncConnectionPool):
            self.event = threading.Event()

        self.pool = ref(pool)
        logger.debug(
            "task created in %s: %s", threading.current_thread().name, self
        )

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
            return

        logger.debug(
            "task running in %s: %s", threading.current_thread().name, self
        )
        if isinstance(pool, ConnectionPool):
            self._run(pool)
        elif isinstance(pool, AsyncConnectionPool):
            self.event.clear()
            asyncio.run_coroutine_threadsafe(self._run_async(pool), pool.loop)
            if not self.event.wait(self.TIMEOUT):
                logger.warning(
                    "event %s didn't terminate after %s sec", self.TIMEOUT
                )
        else:
            logger.error("%s run got %s instead of a pool", self, pool)

    def tick(self) -> None:
        """Run the scheduled task

        This function is called by the scheduler thread. Use a worker to
        run the task for real in order to free the scheduler immediately.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            return

        pool.run_task(self)

    @abstractmethod
    def _run(self, pool: "ConnectionPool") -> None:
        ...

    @abstractmethod
    async def _run_async(self, pool: "AsyncConnectionPool") -> None:
        self.event.set()


class StopWorker(MaintenanceTask[ConnectionType]):
    """Signal the maintenance thread to terminate."""

    def _run(self, pool: "ConnectionPool") -> None:
        pass

    async def _run_async(self, pool: "AsyncConnectionPool") -> None:
        await super()._run_async(pool)


class AddConnection(MaintenanceTask[ConnectionType]):
    def __init__(
        self,
        pool: "BasePool[Any]",
        attempt: Optional["ConnectionAttempt"] = None,
    ):
        super().__init__(pool)
        self.attempt = attempt

    def _run(self, pool: "ConnectionPool") -> None:
        pool._add_connection(self.attempt)

    async def _run_async(self, pool: "AsyncConnectionPool") -> None:
        logger.debug("run async 1")
        await pool._add_connection(self.attempt)
        logger.debug("run async 2")
        await super()._run_async(pool)
        logger.debug("run async 3")


class ReturnConnection(MaintenanceTask[ConnectionType]):
    """Clean up and return a connection to the pool."""

    def __init__(self, pool: "BasePool[Any]", conn: "ConnectionType"):
        super().__init__(pool)
        self.conn = conn

    def _run(self, pool: "ConnectionPool") -> None:
        pool._return_connection(cast(Connection, self.conn))

    async def _run_async(self, pool: "AsyncConnectionPool") -> None:
        await pool._return_connection(cast(AsyncConnection, self.conn))
        await super()._run_async(pool)


class ShrinkPool(MaintenanceTask[ConnectionType]):
    """If the pool can shrink, remove one connection.

    Re-schedule periodically and also reset the minimum number of connections
    in the pool.
    """

    def _run(self, pool: "ConnectionPool") -> None:
        # Reschedule the task now so that in case of any error we don't lose
        # the periodic run.
        pool.schedule_task(self, pool.max_idle)
        pool._shrink_pool()

    async def _run_async(self, pool: "AsyncConnectionPool") -> None:
        pool.schedule_task(self, pool.max_idle)
        await pool._shrink_pool()
        await super()._run_async(pool)
