"""
psycopg3 named cursor objects (server-side cursors)
"""

# Copyright (C) 2020-2021 The Psycopg Team

import weakref
import warnings
from types import TracebackType
from typing import Any, Generic, Optional, Type, TYPE_CHECKING

from . import sql
from .pq import Format
from .cursor import BaseCursor, execute
from .proto import ConnectionType, Query, Params, PQGen

if TYPE_CHECKING:
    from .connection import BaseConnection  # noqa: F401
    from .connection import Connection, AsyncConnection  # noqa: F401


class NamedCursorHelper(Generic[ConnectionType]):
    __slots__ = ("name", "_wcur")

    def __init__(
        self,
        name: str,
        cursor: BaseCursor[ConnectionType],
    ):
        self.name = name
        self._wcur = weakref.ref(cursor)

    @property
    def _cur(self) -> BaseCursor[Any]:
        cur = self._wcur()
        assert cur
        return cur

    def _declare_gen(
        self, query: Query, params: Optional[Params] = None
    ) -> PQGen[None]:
        """Generator implementing `NamedCursor.execute()`."""
        cur = self._cur
        yield from cur._start_query(query)
        pgq = cur._convert_query(query, params)
        cur._execute_send(pgq)
        results = yield from execute(cur._conn.pgconn)
        cur._execute_results(results)

        # The above result is an COMMAND_OK. Get the cursor result shape
        cur._conn.pgconn.send_describe_portal(
            self.name.encode(cur._conn.client_encoding)
        )
        results = yield from execute(cur._conn.pgconn)
        cur._execute_results(results)

    def _make_declare_statement(
        self, query: Query, scrollable: bool, hold: bool
    ) -> sql.Composable:
        cur = self._cur
        if isinstance(query, bytes):
            query = query.decode(cur._conn.client_encoding)
        if not isinstance(query, sql.Composable):
            query = sql.SQL(query)

        return sql.SQL(
            "declare {name} {scroll} cursor{hold} for {query}"
        ).format(
            name=sql.Identifier(self.name),
            scroll=sql.SQL("scroll" if scrollable else "no scroll"),
            hold=sql.SQL(" with hold" if hold else ""),
            query=query,
        )


class NamedCursor(BaseCursor["Connection"]):
    __module__ = "psycopg3"
    __slots__ = ("_helper",)

    def __init__(
        self,
        connection: "Connection",
        name: str,
        *,
        format: Format = Format.TEXT,
    ):
        super().__init__(connection, format=format)
        self._helper = NamedCursorHelper(name, self)

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"named cursor {self} was deleted while still open."
                f" Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __enter__(self) -> "NamedCursor":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    @property
    def name(self) -> str:
        return self._helper.name

    def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        # TODO close the cursor for real
        self._close()

    def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        scrollable: bool = True,
        hold: bool = False,
    ) -> "NamedCursor":
        """
        Execute a query or command to the database.
        """
        query = self._helper._make_declare_statement(
            query, scrollable=scrollable, hold=hold
        )
        with self._conn.lock:
            self._conn.wait(self._helper._declare_gen(query, params))
        return self


class AsyncNamedCursor(BaseCursor["AsyncConnection"]):
    __module__ = "psycopg3"
    __slots__ = ("_helper",)

    def __init__(
        self,
        connection: "AsyncConnection",
        name: str,
        *,
        format: Format = Format.TEXT,
    ):
        super().__init__(connection, format=format)
        self._helper = NamedCursorHelper(name, self)

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"named cursor {self} was deleted while still open."
                f" Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    async def __aenter__(self) -> "AsyncNamedCursor":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    @property
    def name(self) -> str:
        return self._helper.name

    async def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        # TODO close the cursor for real
        self._close()

    async def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        scrollable: bool = True,
        hold: bool = False,
    ) -> "AsyncNamedCursor":
        """
        Execute a query or command to the database.
        """
        query = self._helper._make_declare_statement(
            query, scrollable=scrollable, hold=hold
        )
        async with self._conn.lock:
            await self._conn.wait(self._helper._declare_gen(query, params))
        return self
