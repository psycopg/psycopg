"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys
import asyncio
from typing import Any, Awaitable, Generator, Optional, Union, TypeVar

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

T = TypeVar("T")
FutureT: "TypeAlias" = Union["asyncio.Future[T]", Generator[Any, None, T], Awaitable[T]]

if sys.version_info >= (3, 8):
    create_task = asyncio.create_task

else:

    def create_task(
        coro: FutureT[T], name: Optional[str] = None
    ) -> "asyncio.Future[T]":
        return asyncio.create_task(coro)


if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
    from functools import cache
    from collections import Counter, deque as Deque
else:
    from typing import Counter, Deque
    from functools import lru_cache
    from backports.zoneinfo import ZoneInfo

    cache = lru_cache(maxsize=None)

if sys.version_info >= (3, 10):
    from typing import TypeAlias, TypeGuard
else:
    from typing_extensions import TypeAlias, TypeGuard

if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    from typing_extensions import LiteralString

__all__ = [
    "Counter",
    "Deque",
    "LiteralString",
    "Protocol",
    "TypeAlias",
    "TypeGuard",
    "ZoneInfo",
    "cache",
    "create_task",
]
