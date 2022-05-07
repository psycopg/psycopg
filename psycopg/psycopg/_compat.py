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
    from collections import Counter, deque as Deque
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
else:
    from backports.zoneinfo import ZoneInfo
    from typing import (
        Counter,
        Deque,
        AsyncContextManager as AbstractAsyncContextManager,
        ContextManager as AbstractContextManager,
    )

if sys.version_info >= (3, 10):
    from contextlib import nullcontext
    from typing import TypeAlias, TypeGuard
else:
    from contextlib import nullcontext as _nullcontext
    from typing_extensions import TypeAlias, TypeGuard

    if sys.version_info >= (3, 9):

        class nullcontext(_nullcontext[T]):
            async def __aenter__(self) -> T:
                return self.enter_result

            async def __aexit__(self, *excinfo: Any) -> None:
                pass

    else:

        class nullcontext(_nullcontext):
            async def __aenter__(self) -> Any:
                return self.enter_result

            async def __aexit__(self, *excinfo: Any) -> None:
                pass


__all__ = [
    "AbstractAsyncContextManager",
    "AbstractContextManager",
    "Counter",
    "Deque",
    "Protocol",
    "TypeAlias",
    "TypeGuard",
    "ZoneInfo",
    "create_task",
    "nullcontext",
]
