"""
Utilities to ease the differences between async and sync code.

These object offer a similar interface between sync and async versions; the
script async_to_sync.py will replace the async names with the sync names
when generating the sync version.
"""

# Copyright (C) 2023 The Psycopg Team

import queue
import asyncio
import logging
import threading
from typing import Any, Callable, Coroutine, TypeVar

logger = logging.getLogger("psycopg.pool")
T = TypeVar("T")

# Re-exports
Event = threading.Event
Condition = threading.Condition
Lock = threading.RLock
ALock = asyncio.Lock


class Queue(queue.Queue[T]):
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


class AQueue(asyncio.Queue[T]):
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
