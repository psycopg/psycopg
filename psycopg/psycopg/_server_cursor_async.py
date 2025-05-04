"""
psycopg server-side cursor (async).
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload
from warnings import warn
from collections.abc import AsyncIterator, Iterable

from . import errors as e
from .abc import Params, Query
from .rows import AsyncRowFactory, Row
from ._compat import Self
from .cursor_async import AsyncCursor
from ._server_cursor_base import ServerCursorMixin

if TYPE_CHECKING:
    from .connection_async import AsyncConnection


class AsyncServerCursor(
    ServerCursorMixin["AsyncConnection[Any]", Row], AsyncCursor[Row]
):
    __module__ = "psycopg"
    __slots__ = ()

    @overload
    def __init__(
        self,
        connection: AsyncConnection[Row],
        name: str,
        *,
        scrollable: bool | None = None,
        withhold: bool = False,
    ): ...

    @overload
    def __init__(
        self,
        connection: AsyncConnection[Any],
        name: str,
        *,
        row_factory: AsyncRowFactory[Row],
        scrollable: bool | None = None,
        withhold: bool = False,
    ): ...

    def __init__(
        self,
        connection: AsyncConnection[Any],
        name: str,
        *,
        row_factory: AsyncRowFactory[Row] | None = None,
        scrollable: bool | None = None,
        withhold: bool = False,
    ):
        AsyncCursor.__init__(
            self, connection, row_factory=row_factory or connection.row_factory
        )
        ServerCursorMixin.__init__(self, name, scrollable, withhold)

    def __del__(self) -> None:
        if not self.closed:
            warn(
                f"the server-side cursor {self} was deleted while still open."
                " Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    async def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        async with self._conn.lock:
            if self.closed:
                return
            if not self._conn.closed:
                await self._conn.wait(self._close_gen())
            await super().close()

    async def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        binary: bool | None = None,
        **kwargs: Any,
    ) -> Self:
        """
        Open a cursor to execute a query to the database.
        """
        if kwargs:
            raise TypeError(f"keyword not supported: {list(kwargs)[0]}")
        if self._pgconn.pipeline_status:
            raise e.NotSupportedError(
                "server-side cursors not supported in pipeline mode"
            )

        try:
            async with self._conn.lock:
                await self._conn.wait(self._declare_gen(query, params, binary))
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

        return self

    async def executemany(
        self, query: Query, params_seq: Iterable[Params], *, returning: bool = True
    ) -> None:
        """Method not implemented for server-side cursors."""
        raise e.NotSupportedError("executemany not supported on server-side cursors")

    async def fetchone(self) -> Row | None:
        async with self._conn.lock:
            recs = await self._conn.wait(self._fetch_gen(1))
        if recs:
            self._pos += 1
            return recs[0]
        else:
            return None

    async def fetchmany(self, size: int = 0) -> list[Row]:
        if not size:
            size = self.arraysize
        async with self._conn.lock:
            recs = await self._conn.wait(self._fetch_gen(size))
        self._pos += len(recs)
        return recs

    async def fetchall(self) -> list[Row]:
        async with self._conn.lock:
            recs = await self._conn.wait(self._fetch_gen(None))
        self._pos += len(recs)
        return recs

    async def __aiter__(self) -> AsyncIterator[Row]:
        while True:
            async with self._conn.lock:
                recs = await self._conn.wait(self._fetch_gen(self.itersize))
            for rec in recs:
                self._pos += 1
                yield rec
            if len(recs) < self.itersize:
                break

    async def scroll(self, value: int, mode: str = "relative") -> None:
        async with self._conn.lock:
            await self._conn.wait(self._scroll_gen(value, mode))
        # Postgres doesn't have a reliable way to report a cursor out of bound
        if mode == "relative":
            self._pos += value
        else:
            self._pos = value
