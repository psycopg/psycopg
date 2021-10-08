"""
psycopg async connection objects
"""

# Copyright (C) 2020-2021 The Psycopg Team

import sys
import asyncio
import logging
from types import TracebackType
from typing import Any, AsyncIterator, Dict, Optional, Type, Union
from typing import cast, overload, TYPE_CHECKING

from . import errors as e
from . import waiting
from .pq import Format
from .abc import AdaptContext, Params, PQGen, PQGenConn, Query, RV
from .rows import Row, AsyncRowFactory, tuple_row, TupleRow
from .adapt import AdaptersMap
from ._enums import IsolationLevel
from ._compat import asynccontextmanager, get_running_loop
from .conninfo import make_conninfo, conninfo_to_dict
from ._encodings import pgconn_encoding
from .connection import BaseConnection, CursorRow, Notify
from .generators import notifies
from .transaction import AsyncTransaction
from .cursor_async import AsyncCursor
from .server_cursor import AsyncServerCursor

if TYPE_CHECKING:
    from .pq.abc import PGconn


logger = logging.getLogger("psycopg")


class AsyncConnection(BaseConnection[Row]):
    """
    Asynchronous wrapper for a connection to the database.
    """

    __module__ = "psycopg"

    cursor_factory: Type[AsyncCursor[Row]]
    server_cursor_factory: Type[AsyncServerCursor[Row]]
    row_factory: AsyncRowFactory[Row]

    def __init__(
        self,
        pgconn: "PGconn",
        row_factory: Optional[AsyncRowFactory[Row]] = None,
    ):
        super().__init__(pgconn)
        self.row_factory = row_factory or cast(AsyncRowFactory[Row], tuple_row)
        self.lock = asyncio.Lock()
        self.cursor_factory = AsyncCursor
        self.server_cursor_factory = AsyncServerCursor

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: AsyncRowFactory[Row],
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AsyncConnection[Row]":
        ...

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AsyncConnection[TupleRow]":
        ...

    @classmethod  # type: ignore[misc] # https://github.com/python/mypy/issues/11004
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        context: Optional[AdaptContext] = None,
        row_factory: Optional[AsyncRowFactory[Row]] = None,
        **kwargs: Any,
    ) -> "AsyncConnection[Any]":

        if sys.platform == "win32":
            loop = get_running_loop()
            if isinstance(loop, asyncio.ProactorEventLoop):
                raise e.InterfaceError(
                    "Psycopg cannot use the 'ProactorEventLoop' to run in async"
                    " mode. Please use a compatible event loop, for instance by"
                    " setting 'asyncio.set_event_loop_policy"
                    "(WindowsSelectorEventLoopPolicy())'"
                )

        params = await cls._get_connection_params(conninfo, **kwargs)
        conninfo = make_conninfo(**params)

        try:
            rv = await cls._wait_conn(
                cls._connect_gen(conninfo, autocommit=autocommit),
                timeout=params["connect_timeout"],
            )
        except e.Error as ex:
            raise ex.with_traceback(None)

        if row_factory:
            rv.row_factory = row_factory
        if context:
            rv._adapters = AdaptersMap(context.adapters)
        return rv

    async def __aenter__(self) -> "AsyncConnection[Row]":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.closed:
            return

        if exc_type:
            # try to rollback, but if there are problems (connection in a bad
            # state) just warn without clobbering the exception bubbling up.
            try:
                await self.rollback()
            except Exception as exc2:
                logger.warning(
                    "error ignored rolling back transaction on %s: %s",
                    self,
                    exc2,
                )
        else:
            await self.commit()

        # Close the connection only if it doesn't belong to a pool.
        if not getattr(self, "_pool", None):
            await self.close()

    @classmethod
    async def _get_connection_params(
        cls, conninfo: str, **kwargs: Any
    ) -> Dict[str, Any]:
        """Manipulate connection parameters before connecting."""
        params = conninfo_to_dict(conninfo, **kwargs)

        # Make sure there is an usable connect_timeout
        if "connect_timeout" in params:
            params["connect_timeout"] = int(params["connect_timeout"])
        else:
            params["connect_timeout"] = None

        return params

    async def close(self) -> None:
        if self.closed:
            return
        self._closed = True
        self.pgconn.finish()

    @overload
    def cursor(self, *, binary: bool = False) -> AsyncCursor[Row]:
        ...

    @overload
    def cursor(
        self, *, binary: bool = False, row_factory: AsyncRowFactory[CursorRow]
    ) -> AsyncCursor[CursorRow]:
        ...

    @overload
    def cursor(
        self,
        name: str,
        *,
        binary: bool = False,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> AsyncServerCursor[Row]:
        ...

    @overload
    def cursor(
        self,
        name: str,
        *,
        binary: bool = False,
        row_factory: AsyncRowFactory[CursorRow],
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> AsyncServerCursor[CursorRow]:
        ...

    def cursor(
        self,
        name: str = "",
        *,
        binary: bool = False,
        row_factory: Optional[AsyncRowFactory[Any]] = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> Union[AsyncCursor[Any], AsyncServerCursor[Any]]:
        """
        Return a new `AsyncCursor` to send commands and queries to the connection.
        """
        if not row_factory:
            row_factory = self.row_factory

        cur: Union[AsyncCursor[Any], AsyncServerCursor[Any]]
        if name:
            cur = self.server_cursor_factory(
                self,
                name=name,
                row_factory=row_factory,
                scrollable=scrollable,
                withhold=withhold,
            )
        else:
            cur = self.cursor_factory(self, row_factory=row_factory)

        if binary:
            cur.format = Format.BINARY

        return cur

    async def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
        binary: bool = False,
    ) -> AsyncCursor[Row]:
        cur = self.cursor()
        if binary:
            cur.format = Format.BINARY

        try:
            return await cur.execute(query, params, prepare=prepare)
        except e.Error as ex:
            raise ex.with_traceback(None)

    async def commit(self) -> None:
        async with self.lock:
            await self.wait(self._commit_gen())

    async def rollback(self) -> None:
        async with self.lock:
            await self.wait(self._rollback_gen())

    @asynccontextmanager
    async def transaction(
        self,
        savepoint_name: Optional[str] = None,
        force_rollback: bool = False,
    ) -> AsyncIterator[AsyncTransaction]:
        """
        Start a context block with a new transaction or nested transaction.

        :rtype: AsyncTransaction
        """
        tx = AsyncTransaction(self, savepoint_name, force_rollback)
        async with tx:
            yield tx

    async def notifies(self) -> AsyncIterator[Notify]:
        while 1:
            async with self.lock:
                ns = await self.wait(notifies(self.pgconn))
            enc = pgconn_encoding(self.pgconn)
            for pgn in ns:
                n = Notify(
                    pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid
                )
                yield n

    async def wait(self, gen: PQGen[RV]) -> RV:
        return await waiting.wait_async(gen, self.pgconn.socket)

    @classmethod
    async def _wait_conn(
        cls, gen: PQGenConn[RV], timeout: Optional[int]
    ) -> RV:
        return await waiting.wait_conn_async(gen, timeout)

    def _set_autocommit(self, value: bool) -> None:
        self._no_set_async("autocommit")

    async def set_autocommit(self, value: bool) -> None:
        """Async version of the `~Connection.autocommit` setter."""
        async with self.lock:
            super()._set_autocommit(value)

    def _set_isolation_level(self, value: Optional[IsolationLevel]) -> None:
        self._no_set_async("isolation_level")

    async def set_isolation_level(
        self, value: Optional[IsolationLevel]
    ) -> None:
        """Async version of the `~Connection.isolation_level` setter."""
        async with self.lock:
            super()._set_isolation_level(value)

    def _set_read_only(self, value: Optional[bool]) -> None:
        self._no_set_async("read_only")

    async def set_read_only(self, value: Optional[bool]) -> None:
        """Async version of the `~Connection.read_only` setter."""
        async with self.lock:
            super()._set_read_only(value)

    def _set_deferrable(self, value: Optional[bool]) -> None:
        self._no_set_async("deferrable")

    async def set_deferrable(self, value: Optional[bool]) -> None:
        """Async version of the `~Connection.deferrable` setter."""
        async with self.lock:
            super()._set_deferrable(value)

    def _no_set_async(self, attribute: str) -> None:
        raise AttributeError(
            f"'the {attribute!r} property is read-only on async connections:"
            f" please use 'await .set_{attribute}()' instead."
        )
