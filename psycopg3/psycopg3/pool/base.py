"""
psycopg3 connection pool base class and functionalities.
"""

# Copyright (C) 2021 The Psycopg Team

import random
import logging
import threading
from queue import Queue, Empty
from typing import Any, Callable, Deque, Dict, Generic, List, Optional
from collections import deque

from ..proto import ConnectionType

from . import tasks
from .sched import Scheduler

logger = logging.getLogger(__name__)

WORKER_TIMEOUT = 60.0


class BasePool(Generic[ConnectionType]):

    # Used to generate pool names
    _num_pool = 0

    def __init__(
        self,
        conninfo: str = "",
        *,
        kwargs: Optional[Dict[str, Any]] = None,
        minconn: int = 4,
        maxconn: Optional[int] = None,
        name: Optional[str] = None,
        timeout: float = 30.0,
        max_idle: float = 10 * 60.0,
        reconnect_timeout: float = 5 * 60.0,
        reconnect_failed: Optional[
            Callable[["BasePool[ConnectionType]"], None]
        ] = None,
        num_workers: int = 3,
    ):
        if maxconn is None:
            maxconn = minconn
        if maxconn < minconn:
            raise ValueError(
                f"can't create {self.__class__.__name__}"
                f" with maxconn={maxconn} < minconn={minconn}"
            )
        if not name:
            num = BasePool._num_pool = BasePool._num_pool + 1
            name = f"pool-{num}"

        if num_workers < 1:
            raise ValueError("num_workers must be at least 1")

        self.conninfo = conninfo
        self.kwargs: Dict[str, Any] = kwargs or {}
        self._reconnect_failed: Callable[["BasePool[ConnectionType]"], None]
        self._reconnect_failed = reconnect_failed or (lambda pool: None)
        self.name = name
        self.minconn = minconn
        self.maxconn = maxconn
        self.timeout = timeout
        self.reconnect_timeout = reconnect_timeout
        self.max_idle = max_idle
        self.num_workers = num_workers

        self._nconns = minconn  # currently in the pool, out, being prepared
        self._pool: Deque[ConnectionType] = deque()
        self._sched = Scheduler()

        # Min number of connections in the pool in a max_idle unit of time.
        # It is reset periodically by the ShrinkPool scheduled task.
        # It is used to shrink back the pool if maxcon > minconn and extra
        # connections have been acquired, if we notice that in the last
        # max_idle interval they weren't all used.
        self._nconns_min = minconn

        self._tasks: "Queue[tasks.MaintenanceTask[ConnectionType]]" = Queue()
        self._workers: List[threading.Thread] = []
        for i in range(num_workers):
            t = threading.Thread(
                target=self.worker,
                args=(self._tasks,),
                name=f"{self.name}-worker-{i}",
                daemon=True,
            )
            self._workers.append(t)

        self._sched_runner = threading.Thread(
            target=self._sched.run, name=f"{self.name}-scheduler", daemon=True
        )

        # _close should be the last property to be set in the state
        # to avoid warning on __del__ in case __init__ fails.
        self._closed = False

        # The object state is complete. Start the worker threads
        self._sched_runner.start()
        for t in self._workers:
            t.start()

        # populate the pool with initial minconn connections in background
        for i in range(self._nconns):
            self.run_task(tasks.AddConnection(self))

        # Schedule a task to shrink the pool if connections over minconn have
        # remained unused. However if the pool can't grow don't bother.
        if maxconn > minconn:
            self.schedule_task(tasks.ShrinkPool(self), self.max_idle)

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f" {self.name!r} at 0x{id(self):x}>"
        )

    def __del__(self) -> None:
        # If the '_closed' property is not set we probably failed in __init__.
        # Don't try anything complicated as probably it won't work.
        if getattr(self, "_closed", True):
            return

        # Things we can try to do on a best-effort basis while the world
        # is crumbling (a-la Eternal Sunshine of the Spotless Mind)
        # At worse we put an item in a queue that is being deleted.

        # Stop the scheduler
        self._sched.enter(0, None)

        # Stop the worker threads
        for i in range(len(self._workers)):
            self.run_task(tasks.StopWorker(self))

    @property
    def closed(self) -> bool:
        """`!True` if the pool is closed."""
        return self._closed

    def run_task(self, task: tasks.MaintenanceTask[ConnectionType]) -> None:
        """Run a maintenance task in a worker thread."""
        self._tasks.put_nowait(task)

    def schedule_task(
        self, task: tasks.MaintenanceTask[Any], delay: float
    ) -> None:
        """Run a maintenance task in a worker thread in the future."""
        self._sched.enter(delay, task.tick)

    @classmethod
    def worker(cls, q: "Queue[tasks.MaintenanceTask[ConnectionType]]") -> None:
        """Runner to execute pending maintenance task.

        The function is designed to run as a separate thread.

        Block on the queue *q*, run a task received. Finish running if a
        StopWorker is received.
        """
        # Don't make all the workers time out at the same moment
        timeout = WORKER_TIMEOUT * (0.9 + 0.1 * random.random())
        while True:
            # Use a timeout to make the wait interruptable
            try:
                task = q.get(timeout=timeout)
            except Empty:
                continue

            if isinstance(task, tasks.StopWorker):
                logger.debug(
                    "terminating working thread %s",
                    threading.current_thread().name,
                )
                return

            # Run the task. Make sure don't die in the attempt.
            try:
                task.run()
            except Exception as e:
                logger.warning(
                    "task run %s failed: %s: %s", task, e.__class__.__name__, e
                )


class ConnectionAttempt:
    """Keep the state of a connection attempt."""

    INITIAL_DELAY = 1.0
    DELAY_JITTER = 0.1
    DELAY_BACKOFF = 2.0

    def __init__(self, *, reconnect_timeout: float):
        self.reconnect_timeout = reconnect_timeout
        self.delay = 0.0
        self.give_up_at = 0.0

    def update_delay(self, now: float) -> None:
        """Calculate how long to wait for a new connection attempt"""
        if self.delay == 0.0:
            self.give_up_at = now + self.reconnect_timeout
            # +/- 10% of the initial delay
            jitter = self.INITIAL_DELAY * (
                (2.0 * self.DELAY_JITTER * random.random()) - self.DELAY_JITTER
            )
            self.delay = self.INITIAL_DELAY + jitter
        else:
            self.delay *= self.DELAY_BACKOFF

        if self.delay + now > self.give_up_at:
            self.delay = max(0.0, self.give_up_at - now)

    def time_to_give_up(self, now: float) -> bool:
        """Return True if we are tired of trying to connect. Meh."""
        return self.give_up_at > 0.0 and now >= self.give_up_at
