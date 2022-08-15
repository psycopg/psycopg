"""
psycopg server-side cursor objects.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, AsyncIterator, Generic, List, Iterable, Iterator
from typing import Optional, TypeVar, TYPE_CHECKING
from warnings import warn

from . import pq
from . import sql
from . import errors as e
from .abc import ConnectionType, Query, Params, PQGen
from .rows import Row, RowFactory, AsyncRowFactory
from .cursor import BaseCursor, Cursor
from .generators import execute
from .cursor_async import AsyncCursor

if TYPE_CHECKING:
    from .connection import Connection
    from .connection_async import AsyncConnection

DEFAULT_ITERSIZE = 100


class ServerCursorHelper(Generic[ConnectionType, Row]):
    __slots__ = ("name", "scrollable", "withhold", "described", "_format")
    """Helper object for common ServerCursor code.

    TODO: this should be a mixin, but couldn't find a way to work it
    correctly with the generic.
    """

    def __init__(
        self,
        name: str,
        scrollable: Optional[bool],
        withhold: bool,
    ):
        self.name = name
        self.scrollable = scrollable
        self.withhold = withhold
        self.described = False
        self._format = pq.Format.TEXT

    def _repr(self, cur: BaseCursor[ConnectionType, Row]) -> str:
        # Insert the name as the second word
        parts = parts = BaseCursor.__repr__(cur).split(None, 1)
        parts.insert(1, f"{self.name!r}")
        return " ".join(parts)

    def _declare_gen(
        self,
        cur: BaseCursor[ConnectionType, Row],
        query: Query,
        params: Optional[Params] = None,
        binary: Optional[bool] = None,
    ) -> PQGen[None]:
        """Generator implementing `ServerCursor.execute()`."""

        query = self._make_declare_statement(cur, query)

        # If the cursor is being reused, the previous one must be closed.
        if self.described:
            yield from self._close_gen(cur)
            self.described = False

        yield from cur._start_query(query)
        pgq = cur._convert_query(query, params)
        cur._execute_send(pgq, no_pqexec=True)
        results = yield from execute(cur._conn.pgconn)
        if results[-1].status != pq.ExecStatus.COMMAND_OK:
            cur._raise_for_result(results[-1])

        # Set the format, which will be used by describe and fetch operations
        if binary is None:
            self._format = cur.format
        else:
            self._format = pq.Format.BINARY if binary else pq.Format.TEXT

        # The above result only returned COMMAND_OK. Get the cursor shape
        yield from self._describe_gen(cur)

    def _describe_gen(self, cur: BaseCursor[ConnectionType, Row]) -> PQGen[None]:
        conn = cur._conn
        conn.pgconn.send_describe_portal(self.name.encode(cur._encoding))
        results = yield from execute(conn.pgconn)
        cur._check_results(results)
        cur._results = results
        cur._set_current_result(0, format=self._format)
        self.described = True

    def _close_gen(self, cur: BaseCursor[ConnectionType, Row]) -> PQGen[None]:
        ts = cur._conn.pgconn.transaction_status

        # if the connection is not in a sane state, don't even try
        if ts not in (pq.TransactionStatus.IDLE, pq.TransactionStatus.INTRANS):
            return

        # If we are IDLE, a WITHOUT HOLD cursor will surely have gone already.
        if not self.withhold and ts == pq.TransactionStatus.IDLE:
            return

        # if we didn't declare the cursor ourselves we still have to close it
        # but we must make sure it exists.
        if not self.described:
            query = sql.SQL(
                "SELECT 1 FROM pg_catalog.pg_cursors WHERE name = {}"
            ).format(sql.Literal(self.name))
            res = yield from cur._conn._exec_command(query)
            if res.ntuples == 0:
                return

        query = sql.SQL("CLOSE {}").format(sql.Identifier(self.name))
        yield from cur._conn._exec_command(query)

    def _fetch_gen(
        self, cur: BaseCursor[ConnectionType, Row], num: Optional[int]
    ) -> PQGen[List[Row]]:
        if cur.closed:
            raise e.InterfaceError("the cursor is closed")
        # If we are stealing the cursor, make sure we know its shape
        if not self.described:
            yield from cur._start_query()
            yield from self._describe_gen(cur)

        query = sql.SQL("FETCH FORWARD {} FROM {}").format(
            sql.SQL("ALL") if num is None else sql.Literal(num),
            sql.Identifier(self.name),
        )
        res = yield from cur._conn._exec_command(query, result_format=self._format)

        cur.pgresult = res
        cur._tx.set_pgresult(res, set_loaders=False)
        return cur._tx.load_rows(0, res.ntuples, cur._make_row)

    def _scroll_gen(
        self, cur: BaseCursor[ConnectionType, Row], value: int, mode: str
    ) -> PQGen[None]:
        if mode not in ("relative", "absolute"):
            raise ValueError(f"bad mode: {mode}. It should be 'relative' or 'absolute'")
        query = sql.SQL("MOVE{} {} FROM {}").format(
            sql.SQL(" ABSOLUTE" if mode == "absolute" else ""),
            sql.Literal(value),
            sql.Identifier(self.name),
        )
        yield from cur._conn._exec_command(query)

    def _make_declare_statement(
        self, cur: BaseCursor[ConnectionType, Row], query: Query
    ) -> sql.Composable:

        if isinstance(query, bytes):
            query = query.decode(cur._encoding)
        if not isinstance(query, sql.Composable):
            query = sql.SQL(query)

        parts = [
            sql.SQL("DECLARE"),
            sql.Identifier(self.name),
        ]
        if self.scrollable is not None:
            parts.append(sql.SQL("SCROLL" if self.scrollable else "NO SCROLL"))
        parts.append(sql.SQL("CURSOR"))
        if self.withhold:
            parts.append(sql.SQL("WITH HOLD"))
        parts.append(sql.SQL("FOR"))
        parts.append(query)

        return sql.SQL(" ").join(parts)


_C = TypeVar("_C", bound="ServerCursor[Any]")
_AC = TypeVar("_AC", bound="AsyncServerCursor[Any]")


class ServerCursor(Cursor[Row]):
    __module__ = "psycopg"
    __slots__ = ("_helper", "itersize")

    def __init__(
        self,
        connection: "Connection[Any]",
        name: str,
        *,
        row_factory: RowFactory[Row],
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ):
        super().__init__(connection, row_factory=row_factory)
        self._helper: ServerCursorHelper["Connection[Any]", Row]
        self._helper = ServerCursorHelper(name, scrollable, withhold)
        self.itersize: int = DEFAULT_ITERSIZE

    def __del__(self) -> None:
        if not self.closed:
            warn(
                f"the server-side cursor {self} was deleted while still open."
                " Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __repr__(self) -> str:
        return self._helper._repr(self)

    @property
    def name(self) -> str:
        """The name of the cursor."""
        return self._helper.name

    @property
    def scrollable(self) -> Optional[bool]:
        """
        Whether the cursor is scrollable or not.

        If `!None` leave the choice to the server. Use `!True` if you want to
        use `scroll()` on the cursor.
        """
        return self._helper.scrollable

    @property
    def withhold(self) -> bool:
        """
        If the cursor can be used after the creating transaction has committed.
        """
        return self._helper.withhold

    def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        with self._conn.lock:
            if self.closed:
                return
            if not self._conn.closed:
                self._conn.wait(self._helper._close_gen(self))
            super().close()

    def execute(
        self: _C,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
        **kwargs: Any,
    ) -> _C:
        """
        Open a cursor to execute a query to the database.
        """
        if kwargs:
            raise TypeError(f"keyword not supported: {list(kwargs)[0]}")

        try:
            with self._conn.lock:
                self._conn.wait(self._helper._declare_gen(self, query, params, binary))
        except e.Error as ex:
            raise ex.with_traceback(None)

        return self

    def executemany(self, query: Query, params_seq: Iterable[Params]) -> None:
        """Method not implemented for server-side cursors."""
        raise e.NotSupportedError("executemany not supported on server-side cursors")

    def fetchone(self) -> Optional[Row]:
        with self._conn.lock:
            recs = self._conn.wait(self._helper._fetch_gen(self, 1))
        if recs:
            self._pos += 1
            return recs[0]
        else:
            return None

    def fetchmany(self, size: int = 0) -> List[Row]:
        if not size:
            size = self.arraysize
        with self._conn.lock:
            recs = self._conn.wait(self._helper._fetch_gen(self, size))
        self._pos += len(recs)
        return recs

    def fetchall(self) -> List[Row]:
        with self._conn.lock:
            recs = self._conn.wait(self._helper._fetch_gen(self, None))
        self._pos += len(recs)
        return recs

    def __iter__(self) -> Iterator[Row]:
        while True:
            with self._conn.lock:
                recs = self._conn.wait(self._helper._fetch_gen(self, self.itersize))
            for rec in recs:
                self._pos += 1
                yield rec
            if len(recs) < self.itersize:
                break

    def scroll(self, value: int, mode: str = "relative") -> None:
        with self._conn.lock:
            self._conn.wait(self._helper._scroll_gen(self, value, mode))
        # Postgres doesn't have a reliable way to report a cursor out of bound
        if mode == "relative":
            self._pos += value
        else:
            self._pos = value


class AsyncServerCursor(AsyncCursor[Row]):
    __module__ = "psycopg"
    __slots__ = ("_helper", "itersize")

    def __init__(
        self,
        connection: "AsyncConnection[Any]",
        name: str,
        *,
        row_factory: AsyncRowFactory[Row],
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ):
        super().__init__(connection, row_factory=row_factory)
        self._helper: ServerCursorHelper["AsyncConnection[Any]", Row]
        self._helper = ServerCursorHelper(name, scrollable, withhold)
        self.itersize: int = DEFAULT_ITERSIZE

    def __del__(self) -> None:
        if not self.closed:
            warn(
                f"the server-side cursor {self} was deleted while still open."
                " Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __repr__(self) -> str:
        return self._helper._repr(self)

    @property
    def name(self) -> str:
        return self._helper.name

    @property
    def scrollable(self) -> Optional[bool]:
        return self._helper.scrollable

    @property
    def withhold(self) -> bool:
        return self._helper.withhold

    async def close(self) -> None:
        async with self._conn.lock:
            if self.closed:
                return
            if not self._conn.closed:
                await self._conn.wait(self._helper._close_gen(self))
            await super().close()

    async def execute(
        self: _AC,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
        **kwargs: Any,
    ) -> _AC:
        if kwargs:
            raise TypeError(f"keyword not supported: {list(kwargs)[0]}")
        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._helper._declare_gen(self, query, params, binary)
                )
        except e.Error as ex:
            raise ex.with_traceback(None)

        return self

    async def executemany(self, query: Query, params_seq: Iterable[Params]) -> None:
        raise e.NotSupportedError("executemany not supported on server-side cursors")

    async def fetchone(self) -> Optional[Row]:
        async with self._conn.lock:
            recs = await self._conn.wait(self._helper._fetch_gen(self, 1))
        if recs:
            self._pos += 1
            return recs[0]
        else:
            return None

    async def fetchmany(self, size: int = 0) -> List[Row]:
        if not size:
            size = self.arraysize
        async with self._conn.lock:
            recs = await self._conn.wait(self._helper._fetch_gen(self, size))
        self._pos += len(recs)
        return recs

    async def fetchall(self) -> List[Row]:
        async with self._conn.lock:
            recs = await self._conn.wait(self._helper._fetch_gen(self, None))
        self._pos += len(recs)
        return recs

    async def __aiter__(self) -> AsyncIterator[Row]:
        while True:
            async with self._conn.lock:
                recs = await self._conn.wait(
                    self._helper._fetch_gen(self, self.itersize)
                )
            for rec in recs:
                self._pos += 1
                yield rec
            if len(recs) < self.itersize:
                break

    async def scroll(self, value: int, mode: str = "relative") -> None:
        async with self._conn.lock:
            await self._conn.wait(self._helper._scroll_gen(self, value, mode))
