"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys
import asyncio
from typing import Any, Awaitable, Generator, Optional, Union, TypeVar

T = TypeVar("T")
FutureT = Union["asyncio.Future[T]", Generator[Any, None, T], Awaitable[T]]

if sys.version_info >= (3, 7):
    from contextlib import asynccontextmanager
else:

    def asynccontextmanager(func):
        def helper(*args, **kwds):
            raise NotImplementedError(
                "async pool not implemented on Python 3.6"
            )

        return helper


if sys.version_info >= (3, 8):
    create_task = asyncio.create_task
    Task = asyncio.Task

elif sys.version_info >= (3, 7):

    def create_task(
        coro: FutureT[T], name: Optional[str] = None
    ) -> "asyncio.Future[T]":
        return asyncio.create_task(coro)

    Task = asyncio.Future

else:

    def create_task(
        coro: FutureT[T], name: Optional[str] = None
    ) -> "asyncio.Future[T]":
        return asyncio.ensure_future(coro)

    Task = asyncio.Future

if sys.version_info >= (3, 9):
    from collections import Counter, deque as Deque
else:
    from typing import Counter, Deque

__all__ = [
    "Counter",
    "Deque",
    "Task",
    "asynccontextmanager",
    "create_task",
]
