"""
Utilities to ease the differences between async and sync code.

These object offer a similar interface between sync and async versions; the
script async_to_sync.py will replace the async names with the sync names
when generating the sync version.
"""

import sys
import time
import asyncio
import inspect
import builtins
import threading
import contextlib
from typing import Any

import pytest

# Re-exports
sleep = time.sleep
Event = threading.Event
closing = contextlib.closing

# Markers to decorate tests to run only in async or only in sync version.
skip_sync = pytest.mark.skipif("'async' not in __name__", reason="async test only")
skip_async = pytest.mark.skipif("'async' in __name__", reason="sync test only")


def is_async(obj):
    """Return true if obj is an async object (class, instance, module name)"""
    if isinstance(obj, str):
        # coming from is_async(__name__)
        return "async" in obj

    if not isinstance(obj, type):
        obj = type(obj)
    return "Async" in obj.__name__


if sys.version_info >= (3, 10):
    anext = builtins.anext
    aclosing = contextlib.aclosing

else:

    async def anext(it):
        return await it.__anext__()

    @contextlib.asynccontextmanager
    async def aclosing(thing):
        try:
            yield thing
        finally:
            await thing.aclose()


async def alist(it):
    """Consume an async iterator into a list. Async equivalent of list(it)."""
    return [i async for i in it]


def spawn(f, args=None):
    """
    Equivalent to asyncio.create_task or creating and running a Thread.
    """
    if not args:
        args = ()

    if inspect.iscoroutinefunction(f):
        return asyncio.create_task(f(*args))
    else:
        t = threading.Thread(target=f, args=args, daemon=True)
        t.start()
        return t


def gather(*ts, return_exceptions=False, timeout=None):
    """
    Equivalent to asyncio.gather or Thread.join()
    """
    if ts and inspect.isawaitable(ts[0]):
        rv: Any = asyncio.gather(*ts, return_exceptions=return_exceptions)
        if timeout is None:
            rv = asyncio.wait_for(rv, timeout)
        return rv
    else:
        for t in ts:
            t.join(timeout)
            assert not t.is_alive()


def asleep(s):
    """
    Equivalent to asyncio.sleep(), converted to time.sleep() by async_to_sync.
    """
    return asyncio.sleep(s)


def is_alive(t):
    """
    Return true if an asyncio.Task or threading.Thread is alive.
    """
    return t.is_alive() if isinstance(t, threading.Thread) else not t.done()


class AEvent(asyncio.Event):
    """
    Subclass of asyncio.Event adding a wait with timeout like threading.Event.

    wait_timeout() is converted to wait() by async_to_sync.
    """

    async def wait_timeout(self, timeout):
        await asyncio.wait_for(self.wait(), timeout)
