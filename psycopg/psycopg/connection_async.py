"""
psycopg async connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import sys
import asyncio
import logging
from types import TracebackType
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List, Optional
from typing import Type, Union, cast, overload, TYPE_CHECKING
from contextlib import asynccontextmanager

from . import errors as e
from . import waiting
from .pq import Format, PipelineStatus, TransactionStatus
from .abc import AdaptContext, Params, PQGen, PQGenConn, Query, RV
from ._tpc import Xid
from .rows import Row, AsyncRowFactory, tuple_row, TupleRow, args_row
from .adapt import AdaptersMap
from ._enums import IsolationLevel
from .conninfo import make_conninfo, conninfo_to_dict
from ._pipeline import AsyncPipeline
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

    _pipeline: "Optional[AsyncPipeline]"

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
        prepare_threshold: Optional[int] = 5,
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
        prepare_threshold: Optional[int] = 5,
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
        prepare_threshold: Optional[int] = 5,
        context: Optional[AdaptContext] = None,
        row_factory: Optional[AsyncRowFactory[Row]] = None,
        **kwargs: Any,
    ) -> "AsyncConnection[Any]":

        if sys.platform == "win32":
            loop = asyncio.get_running_loop()
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
        rv.prepare_threshold = prepare_threshold
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
                    "error ignored in rollback on %s: %s",
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
        self._check_connection_ok()

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
        try:
            cur = self.cursor()
            if binary:
                cur.format = Format.BINARY

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

    async def notifies(self) -> AsyncGenerator[Notify, None]:
        while 1:
            async with self.lock:
                ns = await self.wait(notifies(self.pgconn))
            enc = pgconn_encoding(self.pgconn)
            for pgn in ns:
                n = Notify(pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid)
                yield n

    @asynccontextmanager
    async def pipeline(self) -> AsyncIterator[AsyncPipeline]:
        """Context manager to switch the connection into pipeline mode."""
        async with self.lock:
            if self._pipeline is None:
                # We must enter pipeline mode: create a new one
                pipeline = self._pipeline = AsyncPipeline(self.pgconn)
            else:
                # we are already in pipeline mode: bail out as soon as we
                # leave the lock block.
                pipeline = None

        if not pipeline:
            # No-op re-entered inner pipeline block.
            yield self._pipeline
            return

        try:
            async with pipeline:
                try:
                    yield pipeline
                finally:
                    async with self.lock:
                        pipeline.sync()
                        try:
                            # Send an pending commands (e.g. COMMIT or Sync);
                            # while processing results, we might get errors...
                            await self.wait(pipeline._communicate_gen())
                        finally:
                            # then fetch all remaining results but without forcing
                            # flush since we emitted a sync just before.
                            await self.wait(pipeline._fetch_gen(flush=False))
        finally:
            assert pipeline.status == PipelineStatus.OFF, pipeline.status
            self._pipeline = None

    async def wait(self, gen: PQGen[RV]) -> RV:
        try:
            return await waiting.wait_async(gen, self.pgconn.socket)
        except KeyboardInterrupt:
            # TODO: this doesn't seem to work as it does for sync connections
            # see tests/test_concurrency_async.py::test_ctrl_c
            # In the test, the code doesn't reach this branch.

            # On Ctrl-C, try to cancel the query in the server, otherwise
            # otherwise the connection will be stuck in ACTIVE state
            c = self.pgconn.get_cancel()
            c.cancel()
            try:
                await waiting.wait_async(gen, self.pgconn.socket)
            except e.QueryCanceled:
                pass  # as expected
            raise

    @classmethod
    async def _wait_conn(cls, gen: PQGenConn[RV], timeout: Optional[int]) -> RV:
        return await waiting.wait_conn_async(gen, timeout)

    def _set_autocommit(self, value: bool) -> None:
        self._no_set_async("autocommit")

    async def set_autocommit(self, value: bool) -> None:
        """Async version of the `~Connection.autocommit` setter."""
        async with self.lock:
            super()._set_autocommit(value)

    def _set_isolation_level(self, value: Optional[IsolationLevel]) -> None:
        self._no_set_async("isolation_level")

    async def set_isolation_level(self, value: Optional[IsolationLevel]) -> None:
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

    async def tpc_begin(self, xid: Union[Xid, str]) -> None:
        async with self.lock:
            await self.wait(self._tpc_begin_gen(xid))

    async def tpc_prepare(self) -> None:
        try:
            async with self.lock:
                await self.wait(self._tpc_prepare_gen())
        except e.ObjectNotInPrerequisiteState as ex:
            raise e.NotSupportedError(str(ex)) from None

    async def tpc_commit(self, xid: Union[Xid, str, None] = None) -> None:
        async with self.lock:
            await self.wait(self._tpc_finish_gen("commit", xid))

    async def tpc_rollback(self, xid: Union[Xid, str, None] = None) -> None:
        async with self.lock:
            await self.wait(self._tpc_finish_gen("rollback", xid))

    async def tpc_recover(self) -> List[Xid]:
        status = self.info.transaction_status
        async with self.cursor(row_factory=args_row(Xid._from_record)) as cur:
            await cur.execute(Xid._get_recover_query())
            res = await cur.fetchall()

        if (
            status == TransactionStatus.IDLE
            and self.info.transaction_status == TransactionStatus.INTRANS
        ):
            await self.rollback()

        return res
