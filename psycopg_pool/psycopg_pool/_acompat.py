"""
Utilities to ease the differences between async and sync code.

These object offer a similar interface between sync and async versions; the
script async_to_sync.py will replace the async names with the sync names
when generating the sync version.
"""

# Copyright (C) 2023 The Psycopg Team

import asyncio
import threading

Event = threading.Event
Condition = threading.Condition
Lock = threading.RLock
ALock = asyncio.Lock


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
