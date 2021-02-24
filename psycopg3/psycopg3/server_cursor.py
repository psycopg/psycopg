"""
psycopg3 server-side cursor objects.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import warnings
from types import TracebackType
from typing import Any, AsyncIterator, Generic, List, Iterator, Optional
from typing import Sequence, Type, Tuple, TYPE_CHECKING

from . import pq
from . import sql
from . import errors as e
from .rows import tuple_row
from .cursor import BaseCursor, execute
from .proto import ConnectionType, Query, Params, PQGen, Row, RowFactory

if TYPE_CHECKING:
    from .connection import BaseConnection  # noqa: F401
    from .connection import Connection, AsyncConnection  # noqa: F401

DEFAULT_ITERSIZE = 100


class ServerCursorHelper(Generic[ConnectionType]):
    __slots__ = ("name", "described")
    """Helper object for common ServerCursor code.

    TODO: this should be a mixin, but couldn't find a way to work it
    correctly with the generic.
    """

    def __init__(self, name: str):
        self.name = name
        self.described = False

    def _repr(self, cur: BaseCursor[ConnectionType]) -> str:
        cls = f"{cur.__class__.__module__}.{cur.__class__.__qualname__}"
        info = pq.misc.connection_summary(cur._conn.pgconn)
        if cur._closed:
            status = "closed"
        elif not cur.pgresult:
            status = "no result"
        else:
            status = pq.ExecStatus(cur.pgresult.status).name
        return f"<{cls} {self.name!r} [{status}] {info} at 0x{id(cur):x}>"

    def _declare_gen(
        self,
        cur: BaseCursor[ConnectionType],
        query: Query,
        params: Optional[Params] = None,
    ) -> PQGen[None]:
        """Generator implementing `ServerCursor.execute()`."""
        conn = cur._conn

        # If the cursor is being reused, the previous one must be closed.
        if self.described:
            yield from self._close_gen(cur)
            self.described = False

        yield from cur._start_query(query)
        pgq = cur._convert_query(query, params)
        cur._execute_send(pgq, no_pqexec=True)
        results = yield from execute(conn.pgconn)
        if results[-1].status != pq.ExecStatus.COMMAND_OK:
            cur._raise_from_results(results)

        # The above result only returned COMMAND_OK. Get the cursor shape
        yield from self._describe_gen(cur)

    def _describe_gen(self, cur: BaseCursor[ConnectionType]) -> PQGen[None]:
        conn = cur._conn
        conn.pgconn.send_describe_portal(
            self.name.encode(conn.client_encoding)
        )
        results = yield from execute(conn.pgconn)
        cur._execute_results(results)
        self.described = True

    def _close_gen(self, cur: BaseCursor[ConnectionType]) -> PQGen[None]:
        # if the connection is not in a sane state, don't even try
        if cur._conn.pgconn.transaction_status not in (
            pq.TransactionStatus.IDLE,
            pq.TransactionStatus.INTRANS,
        ):
            return

        # if we didn't declare the cursor ourselves we still have to close it
        # but we must make sure it exists.
        if not self.described:
            query = sql.SQL(
                "select 1 from pg_catalog.pg_cursors where name = {}"
            ).format(sql.Literal(self.name))
            res = yield from cur._conn._exec_command(query)
            if res.ntuples == 0:
                return

        query = sql.SQL("close {}").format(sql.Identifier(self.name))
        yield from cur._conn._exec_command(query)

    def _fetch_gen(
        self, cur: BaseCursor[ConnectionType], num: Optional[int]
    ) -> PQGen[List[Tuple[Any, ...]]]:
        # If we are stealing the cursor, make sure we know its shape
        if not self.described:
            yield from cur._start_query()
            yield from self._describe_gen(cur)

        if num is not None:
            howmuch: sql.Composable = sql.Literal(num)
        else:
            howmuch = sql.SQL("all")

        query = sql.SQL("fetch forward {} from {}").format(
            howmuch, sql.Identifier(self.name)
        )
        res = yield from cur._conn._exec_command(query)

        cur.pgresult = res
        cur._tx.set_pgresult(res, set_loaders=False)
        return cur._tx.load_rows(0, res.ntuples)

    def _scroll_gen(
        self, cur: BaseCursor[ConnectionType], value: int, mode: str
    ) -> PQGen[None]:
        if mode not in ("relative", "absolute"):
            raise ValueError(
                f"bad mode: {mode}. It should be 'relative' or 'absolute'"
            )
        query = sql.SQL("move{} {} from {}").format(
            sql.SQL(" absolute" if mode == "absolute" else ""),
            sql.Literal(value),
            sql.Identifier(self.name),
        )
        yield from cur._conn._exec_command(query)

    def _make_declare_statement(
        self,
        cur: BaseCursor[ConnectionType],
        query: Query,
        scrollable: Optional[bool],
        hold: bool,
    ) -> sql.Composable:

        if isinstance(query, bytes):
            query = query.decode(cur._conn.client_encoding)
        if not isinstance(query, sql.Composable):
            query = sql.SQL(query)

        parts = [
            sql.SQL("declare"),
            sql.Identifier(self.name),
        ]
        if scrollable is not None:
            parts.append(sql.SQL("scroll" if scrollable else "no scroll"))
        parts.append(sql.SQL("cursor"))
        if hold:
            parts.append(sql.SQL("with hold"))
        parts.append(sql.SQL("for"))
        parts.append(query)

        return sql.SQL(" ").join(parts)


class ServerCursor(BaseCursor["Connection"]):
    __module__ = "psycopg3"
    __slots__ = ("_helper", "itersize")

    def __init__(
        self,
        connection: "Connection",
        name: str,
        *,
        format: pq.Format = pq.Format.TEXT,
        row_factory: RowFactory = tuple_row,
    ):
        super().__init__(connection, format=format, row_factory=row_factory)
        self._helper: ServerCursorHelper["Connection"]
        self._helper = ServerCursorHelper(name)
        self.itersize = DEFAULT_ITERSIZE

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"the server-side cursor {self} was deleted while still open."
                f" Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __repr__(self) -> str:
        return self._helper._repr(self)

    def __enter__(self) -> "ServerCursor":
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
        """The name of the cursor."""
        return self._helper.name

    def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        with self._conn.lock:
            self._conn.wait(self._helper._close_gen(self))
        self._close()

    def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        scrollable: Optional[bool] = None,
        hold: bool = False,
    ) -> "ServerCursor":
        """
        Open a cursor to execute a query to the database.
        """
        query = self._helper._make_declare_statement(
            self, query, scrollable=scrollable, hold=hold
        )
        with self._conn.lock:
            self._conn.wait(self._helper._declare_gen(self, query, params))
        return self

    def executemany(self, query: Query, params_seq: Sequence[Params]) -> None:
        """Method not implemented for server-side cursors."""
        raise e.NotSupportedError(
            "executemany not supported on server-side cursors"
        )

    def fetchone(self) -> Optional[Row]:
        with self._conn.lock:
            recs = self._conn.wait(self._helper._fetch_gen(self, 1))
        if recs:
            self._pos += 1
            return recs[0]
        else:
            return None

    def fetchmany(self, size: int = 0) -> Sequence[Row]:
        if not size:
            size = self.arraysize
        with self._conn.lock:
            recs = self._conn.wait(self._helper._fetch_gen(self, size))
        self._pos += len(recs)
        return recs

    def fetchall(self) -> Sequence[Row]:
        with self._conn.lock:
            recs = self._conn.wait(self._helper._fetch_gen(self, None))
        self._pos += len(recs)
        return recs

    def __iter__(self) -> Iterator[Row]:
        while True:
            with self._conn.lock:
                recs = self._conn.wait(
                    self._helper._fetch_gen(self, self.itersize)
                )
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


class AsyncServerCursor(BaseCursor["AsyncConnection"]):
    __module__ = "psycopg3"
    __slots__ = ("_helper", "itersize")

    def __init__(
        self,
        connection: "AsyncConnection",
        name: str,
        *,
        format: pq.Format = pq.Format.TEXT,
        row_factory: RowFactory = tuple_row,
    ):
        super().__init__(connection, format=format, row_factory=row_factory)
        self._helper: ServerCursorHelper["AsyncConnection"]
        self._helper = ServerCursorHelper(name)
        self.itersize = DEFAULT_ITERSIZE

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"the server-side cursor {self} was deleted while still open."
                f" Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __repr__(self) -> str:
        return self._helper._repr(self)

    async def __aenter__(self) -> "AsyncServerCursor":
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
        async with self._conn.lock:
            await self._conn.wait(self._helper._close_gen(self))
        self._close()

    async def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        scrollable: Optional[bool] = None,
        hold: bool = False,
    ) -> "AsyncServerCursor":
        query = self._helper._make_declare_statement(
            self, query, scrollable=scrollable, hold=hold
        )
        async with self._conn.lock:
            await self._conn.wait(
                self._helper._declare_gen(self, query, params)
            )
        return self

    async def executemany(
        self, query: Query, params_seq: Sequence[Params]
    ) -> None:
        raise e.NotSupportedError(
            "executemany not supported on server-side cursors"
        )

    async def fetchone(self) -> Optional[Row]:
        async with self._conn.lock:
            recs = await self._conn.wait(self._helper._fetch_gen(self, 1))
        if recs:
            self._pos += 1
            return recs[0]
        else:
            return None

    async def fetchmany(self, size: int = 0) -> Sequence[Row]:
        if not size:
            size = self.arraysize
        async with self._conn.lock:
            recs = await self._conn.wait(self._helper._fetch_gen(self, size))
        self._pos += len(recs)
        return recs

    async def fetchall(self) -> Sequence[Row]:
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
