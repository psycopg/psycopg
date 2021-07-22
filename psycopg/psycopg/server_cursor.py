"""
psycopg server-side cursor objects.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import warnings
from types import TracebackType
from typing import AsyncIterator, Generic, List, Iterator, Optional
from typing import Sequence, Type, TYPE_CHECKING

from . import pq
from . import sql
from . import errors as e
from .abc import ConnectionType, Query, Params, PQGen
from .rows import Row, RowFactory
from .cursor import BaseCursor, execute

if TYPE_CHECKING:
    from typing import Any  # noqa: F401
    from .connection import BaseConnection  # noqa: F401
    from .connection import Connection, AsyncConnection  # noqa: F401

DEFAULT_ITERSIZE = 100


class ServerCursorHelper(Generic[ConnectionType, Row]):
    __slots__ = ("name", "described")
    """Helper object for common ServerCursor code.

    TODO: this should be a mixin, but couldn't find a way to work it
    correctly with the generic.
    """

    def __init__(self, name: str):
        self.name = name
        self.described = False

    def _repr(self, cur: BaseCursor[ConnectionType, Row]) -> str:
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
        cur: BaseCursor[ConnectionType, Row],
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

    def _describe_gen(
        self, cur: BaseCursor[ConnectionType, Row]
    ) -> PQGen[None]:
        conn = cur._conn
        conn.pgconn.send_describe_portal(
            self.name.encode(conn.client_encoding)
        )
        results = yield from execute(conn.pgconn)
        cur._execute_results(results)
        self.described = True

    def _close_gen(self, cur: BaseCursor[ConnectionType, Row]) -> PQGen[None]:
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
        self, cur: BaseCursor[ConnectionType, Row], num: Optional[int]
    ) -> PQGen[List[Row]]:
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
        return cur._tx.load_rows(0, res.ntuples, cur._make_row)

    def _scroll_gen(
        self, cur: BaseCursor[ConnectionType, Row], value: int, mode: str
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
        cur: BaseCursor[ConnectionType, Row],
        query: Query,
        scrollable: Optional[bool],
        withhold: bool,
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
        if withhold:
            parts.append(sql.SQL("with hold"))
        parts.append(sql.SQL("for"))
        parts.append(query)

        return sql.SQL(" ").join(parts)


class ServerCursor(BaseCursor["Connection[Any]", Row]):
    __module__ = "psycopg"
    __slots__ = ("_helper", "itersize")

    def __init__(
        self,
        connection: "Connection[Any]",
        name: str,
        *,
        row_factory: RowFactory[Row],
    ):
        super().__init__(connection, row_factory=row_factory)
        self._helper: ServerCursorHelper["Connection[Any]", Row]
        self._helper = ServerCursorHelper(name)
        self.itersize: int = DEFAULT_ITERSIZE

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"the server-side cursor {self} was deleted while still open."
                f" Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __repr__(self) -> str:
        return self._helper._repr(self)

    def __enter__(self) -> "ServerCursor[Row]":
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
        withhold: bool = False,
    ) -> "ServerCursor[Row]":
        """
        Open a cursor to execute a query to the database.
        """
        query = self._helper._make_declare_statement(
            self, query, scrollable=scrollable, withhold=withhold
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


class AsyncServerCursor(BaseCursor["AsyncConnection[Any]", Row]):
    __module__ = "psycopg"
    __slots__ = ("_helper", "itersize")

    def __init__(
        self,
        connection: "AsyncConnection[Any]",
        name: str,
        *,
        row_factory: RowFactory[Row],
    ):
        super().__init__(connection, row_factory=row_factory)
        self._helper: ServerCursorHelper["AsyncConnection[Any]", Row]
        self._helper = ServerCursorHelper(name)
        self.itersize: int = DEFAULT_ITERSIZE

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"the server-side cursor {self} was deleted while still open."
                f" Please use 'with' or '.close()' to close the cursor properly",
                ResourceWarning,
            )

    def __repr__(self) -> str:
        return self._helper._repr(self)

    async def __aenter__(self) -> "AsyncServerCursor[Row]":
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
        withhold: bool = False,
    ) -> "AsyncServerCursor[Row]":
        query = self._helper._make_declare_statement(
            self, query, scrollable=scrollable, withhold=withhold
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
