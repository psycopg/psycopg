"""
Psycopg AsyncPipeline object implementation.
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any

from . import errors as e
from ._compat import Self
from ._pipeline_base import BasePipeline

if TYPE_CHECKING:
    from .connection_async import AsyncConnection

logger = logging.getLogger("psycopg")


class _DummyLock:
    async def __aenter__(self) -> None:
        pass

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass


class AsyncPipeline(BasePipeline):
    """Handler for (async) connection in pipeline mode."""

    __module__ = "psycopg"
    _conn: AsyncConnection[Any]

    def __init__(self, conn: AsyncConnection[Any], _no_lock: bool = False) -> None:
        super().__init__(conn)
        self._lock = _DummyLock() if _no_lock else conn.lock

    async def sync(self) -> None:
        """Sync the pipeline, send any pending command and receive and process
        all available results.
        """
        try:
            async with self._lock:
                await self._conn.wait(self._sync_gen())
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    async def __aenter__(self) -> Self:
        async with self._lock:
            await self._conn.wait(self._enter_gen())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            async with self._lock:
                await self._conn.wait(self._exit_gen())
        except Exception as exc2:
            # Don't clobber an exception raised in the block with this one
            if exc_val:
                logger.warning("error ignored terminating %r: %s", self, exc2)
            else:
                raise exc2.with_traceback(None)
        finally:
            self._exit(exc_val)
