"""
Utilities to ease the differences between async and sync code.

These object offer a similar interface between sync and async versions; the
script async_to_sync.py will replace the async names with the sync names
when generating the sync version.
"""

# Copyright (C) 2023 The Psycopg Team

from __future__ import annotations

import queue
import asyncio
import logging
import threading
from typing import Any, Callable, Coroutine, TypeVar, TYPE_CHECKING

from typing_extensions import TypeAlias

logger = logging.getLogger("psycopg.pool")
T = TypeVar("T")

# Re-exports
Event = threading.Event
Condition = threading.Condition
Lock = threading.RLock
ALock = asyncio.Lock

Worker: TypeAlias = threading.Thread
AWorker: TypeAlias = "asyncio.Task[None]"

# Hack required on Python 3.8 because subclassing Queue[T] fails at runtime.
# https://stackoverflow.com/questions/45414066/mypy-how-to-define-a-generic-subclass
if TYPE_CHECKING:
    _GQueue: TypeAlias = queue.Queue
    _AGQueue: TypeAlias = asyncio.Queue

else:

    class FakeGenericMeta(type):
        def __getitem__(self, item):
            return self

    class _GQueue(queue.Queue, metaclass=FakeGenericMeta):
        pass

    class _AGQueue(asyncio.Queue, metaclass=FakeGenericMeta):
        pass


def current_thread_name() -> str:
    return threading.current_thread().name


def current_task_name() -> str:
    t = asyncio.current_task()
    return t.get_name() if t else "<no task>"


class Queue(_GQueue[T]):
    """
    A Queue subclass with an interruptible get() method.
    """

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        # Always specify a timeout to make the wait interruptible.
        if timeout is None:
            timeout = 24.0 * 60.0 * 60.0
        return super().get(block, timeout)


class AEvent(asyncio.Event):
    """
    Subclass of asyncio.Event adding a wait with timeout like threading.Event.

    wait_timeout() is converted to wait() by async_to_sync.
    """

    async def wait_timeout(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


class ACondition(asyncio.Condition):
    """
    Subclass of asyncio.Condition adding a wait with timeout like threading.Condition.

    wait_timeout() is converted to wait() by async_to_sync.
    """

    async def wait_timeout(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


class AQueue(_AGQueue[T]):
    pass


def aspawn(
    f: Callable[..., Coroutine[Any, Any, None]],
    args: tuple[Any, ...] = (),
    name: str | None = None,
) -> asyncio.Task[None]:
    """
    Equivalent to asyncio.create_task.
    """
    return asyncio.create_task(f(*args), name=name)


def spawn(
    f: Callable[..., Any],
    args: tuple[Any, ...] = (),
    name: str | None = None,
) -> threading.Thread:
    """
    Equivalent to creating and running a daemon thread.
    """
    t = threading.Thread(target=f, args=args, name=name, daemon=True)
    t.start()
    return t


async def agather(*tasks: asyncio.Task[Any], timeout: float | None = None) -> None:
    """
    Equivalent to asyncio.gather or Thread.join()
    """
    wait = asyncio.gather(*tasks)
    try:
        if timeout is not None:
            await asyncio.wait_for(asyncio.shield(wait), timeout=timeout)
        else:
            await wait
    except asyncio.TimeoutError:
        pass
    else:
        return

    for t in tasks:
        if t.done():
            continue
        logger.warning("couldn't stop task %r within %s seconds", t.get_name(), timeout)


def gather(*tasks: threading.Thread, timeout: float | None = None) -> None:
    """
    Equivalent to asyncio.gather or Thread.join()
    """
    for t in tasks:
        if not t.is_alive():
            continue
        t.join(timeout)
        if not t.is_alive():
            continue
        logger.warning("couldn't stop thread %r within %s seconds", t.name, timeout)
