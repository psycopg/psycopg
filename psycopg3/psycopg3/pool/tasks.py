"""
Maintenance tasks for the connection pools.
"""

# Copyright (C) 2021 The Psycopg Team

import logging
from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING
from weakref import ref

if TYPE_CHECKING:
    from .base import ConnectionAttempt
    from .pool import ConnectionPool
    from ..connection import Connection

logger = logging.getLogger(__name__)


class MaintenanceTask(ABC):
    """A task to run asynchronously to maintain the pool state."""

    def __init__(self, pool: "ConnectionPool"):
        self.pool = ref(pool)
        logger.debug("task created: %s", self)

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

        logger.debug("task running: %s", self)
        self._run(pool)

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


class StopWorker(MaintenanceTask):
    """Signal the maintenance thread to terminate."""

    def _run(self, pool: "ConnectionPool") -> None:
        pass


class AddConnection(MaintenanceTask):
    def __init__(
        self,
        pool: "ConnectionPool",
        attempt: Optional["ConnectionAttempt"] = None,
    ):
        super().__init__(pool)
        self.attempt = attempt

    def _run(self, pool: "ConnectionPool") -> None:
        pool._add_connection(self.attempt)


class ReturnConnection(MaintenanceTask):
    """Clean up and return a connection to the pool."""

    def __init__(self, pool: "ConnectionPool", conn: "Connection"):
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

        pool._shrink_if_possible()
