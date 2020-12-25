"""
psycopg3 cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

import sys
from enum import IntEnum, auto
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Generic, Iterator, List
from typing import Optional, Sequence, Tuple, Type, TYPE_CHECKING, Union
from contextlib import contextmanager

from . import errors as e
from . import pq
from .pq import ExecStatus, Format
from .copy import Copy, AsyncCopy
from .proto import ConnectionType, Query, Params, DumpersMap, LoadersMap, PQGen
from ._column import Column
from ._queries import PostgresQuery

if sys.version_info >= (3, 7):
    from contextlib import asynccontextmanager
else:
    from .utils.context import asynccontextmanager

if TYPE_CHECKING:
    from .proto import Transformer
    from .pq.proto import PGconn, PGresult
    from .connection import Connection, AsyncConnection  # noqa: F401

execute: Callable[["PGconn"], PQGen[List["PGresult"]]]

if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    execute = _psycopg3.execute

else:
    from . import generators

    execute = generators.execute


class Prepare(IntEnum):
    NO = auto()
    YES = auto()
    SHOULD = auto()


class BaseCursor(Generic[ConnectionType]):
    ExecStatus = pq.ExecStatus

    _transformer: "Transformer"
    _rowcount: int

    def __init__(
        self,
        connection: ConnectionType,
        format: Format = Format.TEXT,
    ):
        self._conn = connection
        self.format = format
        self.dumpers: DumpersMap = {}
        self.loaders: LoadersMap = {}
        self._reset()
        self.arraysize = 1
        self._closed = False

    def _reset(self) -> None:
        self._results: List["PGresult"] = []
        self.pgresult = None
        self._pos = 0
        self._iresult = 0
        self._rowcount = -1
        self._query: Optional[bytes] = None
        self._params: Optional[List[Optional[bytes]]] = None

    @property
    def connection(self) -> ConnectionType:
        """The connection this cursor is using."""
        return self._conn

    @property
    def closed(self) -> bool:
        """`True` if the cursor is closed."""
        return self._closed

    @property
    def status(self) -> Optional[pq.ExecStatus]:
        # TODO: do we want this?
        res = self.pgresult
        return res.status if res else None

    @property
    def query(self) -> Optional[bytes]:
        """The last query sent to the server, if available."""
        return self._query

    @property
    def params(self) -> Optional[List[Optional[bytes]]]:
        """The last set of parameters sent to the server, if available."""
        return self._params

    @property
    def pgresult(self) -> Optional["PGresult"]:
        """The `~psycopg3.pq.PGresult` exposed by the cursor."""
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional["PGresult"]) -> None:
        self._pgresult = result
        if result and self._transformer:
            self._transformer.pgresult = result

    @property
    def description(self) -> Optional[List[Column]]:
        """
        A list of `Column` objects describing the current resultset.

        `!None` if the current resultset didn't return tuples.
        """
        res = self.pgresult
        if not res or res.status != ExecStatus.TUPLES_OK:
            return None
        return [Column(self, i) for i in range(res.nfields)]

    @property
    def rowcount(self) -> int:
        """Number of records affected by the precedent operation."""
        return self._rowcount

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        # no-op
        pass

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        # no-op
        pass

    def nextset(self) -> Optional[bool]:
        """
        Move to the next result set if `execute()` returned more than one.

        Return `!True` if a new result is available, which will be the one
        methods `!fetch*()` will operate on.
        """
        self._iresult += 1
        if self._iresult < len(self._results):
            self.pgresult = self._results[self._iresult]
            self._pos = 0
            nrows = self.pgresult.command_tuples
            self._rowcount = nrows if nrows is not None else -1
            return True
        else:
            return None

    #
    # Generators for the high level operations on the cursor
    #
    # Like for sync/async connections, these are implemented as generators
    # so that different concurrency strategies (threads,asyncio) can use their
    # own way of waiting (or better, `connection.wait()`).
    #

    def _execute_gen(
        self,
        query: Query,
        params: Optional[Params] = None,
        prepare: Optional[bool] = None,
    ) -> PQGen[None]:
        """Generator implementing `Cursor.execute()`."""
        yield from self._start_query()
        pgq = self._convert_query(query, params)

        # Check if the query is prepared or needs preparing
        prep, name = self._get_prepared(pgq, prepare)
        if prep is Prepare.YES:
            # The query is already prepared
            self._send_query_prepared(name, pgq)

        elif prep is Prepare.NO:
            # The query must be executed without preparing
            self._execute_send(pgq)

        else:
            # The query must be prepared and executed
            self._send_prepare(name, pgq)
            (result,) = yield from execute(self._conn.pgconn)
            if result.status == ExecStatus.FATAL_ERROR:
                raise e.error_from_result(
                    result, encoding=self._conn.client_encoding
                )
            self._send_query_prepared(name, pgq)

        # run the query
        results = yield from execute(self._conn.pgconn)

        # Update the prepare state of the query
        if prepare is not False:
            cmd = self._maintain_prepared(pgq, results, prep, name)
            if cmd:
                yield from self._conn._exec_command(cmd)

        self._execute_results(results)

    def _get_prepared(
        self, query: PostgresQuery, prepare: Optional[bool] = None
    ) -> Tuple[Prepare, bytes]:
        """
        Check if a query is prepared, tell back whether to prepare it.
        """
        conn = self._conn
        if prepare is False or conn.prepare_threshold is None:
            # The user doesn't want this query to be prepared
            return Prepare.NO, b""

        key = (query.query, query.types)
        value: Union[bytes, int] = conn._prepared_statements.get(key, 0)
        if isinstance(value, bytes):
            # The query was already prepared in this session
            return Prepare.YES, value

        if value >= conn.prepare_threshold or prepare:
            # The query has been executed enough times and needs to be prepared
            name = f"_pg3_{conn._prepared_idx}".encode("utf-8")
            conn._prepared_idx += 1
            return Prepare.SHOULD, name
        else:
            # The query is not to be prepared yet
            return Prepare.NO, b""

    def _maintain_prepared(
        self,
        query: PostgresQuery,
        results: Sequence["PGresult"],
        prep: Prepare,
        name: bytes,
    ) -> Optional[bytes]:
        """Maintain the cache of he prepared statements."""
        # don't do anything if prepared statements are disabled
        if self._conn.prepare_threshold is None:
            return None

        cache = self._conn._prepared_statements
        key = (query.query, query.types)

        # If we know the query already the cache size won't change
        # So just update the count and record as last used
        if key in cache:
            if isinstance(cache[key], int):
                if prep is Prepare.SHOULD:
                    cache[key] = name
                else:
                    cache[key] += 1  # type: ignore  # operator
            cache.move_to_end(key)
            return None

        # The query is not in cache. Let's see if we must add it
        if len(results) != 1:
            # We cannot prepare a multiple statement
            return None

        result = results[0]
        if (
            result.status != ExecStatus.TUPLES_OK
            and result.status != ExecStatus.COMMAND_OK
        ):
            # We don't prepare failed queries or other weird results
            return None

        # Ok, we got to the conclusion that this query is genuinely to prepare
        cache[key] = name if prep is Prepare.SHOULD else 1

        # Evict an old value from the cache; if it was prepared, deallocate it
        # Do it only once: if the cache was resized, deallocate gradually
        if len(cache) <= self._conn.prepared_max:
            return None

        old_val = cache.popitem(last=False)[1]
        if isinstance(old_val, bytes):
            return b"DEALLOCATE " + old_val
        else:
            return None

    def _executemany_gen(
        self, query: Query, params_seq: Sequence[Params]
    ) -> PQGen[None]:
        """Generator implementing `Cursor.executemany()`."""
        yield from self._start_query()
        first = True
        for params in params_seq:
            if first:
                pgq = self._convert_query(query, params)
                # TODO: prepare more statements if the types tuples change
                self._send_prepare(b"", pgq)
                (result,) = yield from execute(self._conn.pgconn)
                if result.status == ExecStatus.FATAL_ERROR:
                    raise e.error_from_result(
                        result, encoding=self._conn.client_encoding
                    )
            else:
                pgq.dump(params)

            self._send_query_prepared(b"", pgq)
            (result,) = yield from execute(self._conn.pgconn)
            self._execute_results((result,))

    def _start_query(self) -> PQGen[None]:
        """Generator to start the processing of a query.

        It is implemented as generator because it may send additional queries,
        such as `begin`.
        """
        from . import adapt

        if self.closed:
            raise e.InterfaceError("the cursor is closed")

        self._reset()
        self._transformer = adapt.Transformer(self)
        yield from self._conn._start_query()

    def _start_copy_gen(self, statement: Query) -> PQGen[None]:
        """Generator implementing sending a command for `Cursor.copy()."""
        yield from self._start_query()
        query = self._convert_query(statement)

        # Make sure to avoid PQexec to avoid receiving a mix of COPY and
        # other operations.
        self._execute_send(query, no_pqexec=True)
        (result,) = yield from execute(self._conn.pgconn)
        self._check_copy_result(result)
        self.pgresult = result  # will set it on the transformer too

    def _execute_send(
        self, query: PostgresQuery, no_pqexec: bool = False
    ) -> None:
        """
        Implement part of execute() before waiting common to sync and async.

        This is not a generator, but a normal non-blocking function.
        """
        if query.params or no_pqexec or self.format == Format.BINARY:
            self._query = query.query
            self._params = query.params
            self._conn.pgconn.send_query_params(
                query.query,
                query.params,
                param_formats=query.formats,
                param_types=query.types,
                result_format=self.format,
            )
        else:
            # if we don't have to, let's use exec_ as it can run more than
            # one query in one go
            self._query = query.query
            self._params = None
            self._conn.pgconn.send_query(query.query)

    def _convert_query(
        self, query: Query, params: Optional[Params] = None
    ) -> PostgresQuery:
        pgq = PostgresQuery(self._transformer)
        pgq.convert(query, params)
        return pgq

    _status_ok = {
        ExecStatus.TUPLES_OK,
        ExecStatus.COMMAND_OK,
        ExecStatus.EMPTY_QUERY,
    }
    _status_copy = {
        ExecStatus.COPY_IN,
        ExecStatus.COPY_OUT,
        ExecStatus.COPY_BOTH,
    }

    def _execute_results(self, results: Sequence["PGresult"]) -> None:
        """
        Implement part of execute() after waiting common to sync and async

        This is not a generator, but a normal non-blocking function.
        """
        if not results:
            raise e.InternalError("got no result from the query")

        statuses = {res.status for res in results}
        badstats = statuses - self._status_ok
        if not badstats:
            self._results = list(results)
            self.pgresult = results[0]
            nrows = self.pgresult.command_tuples
            if nrows is not None:
                if self._rowcount < 0:
                    self._rowcount = nrows
                else:
                    self._rowcount += nrows

            return

        if results[-1].status == ExecStatus.FATAL_ERROR:
            raise e.error_from_result(
                results[-1], encoding=self._conn.client_encoding
            )
        elif badstats & self._status_copy:
            raise e.ProgrammingError(
                "COPY cannot be used with execute(); use copy() insead"
            )
        else:
            raise e.InternalError(
                f"got unexpected status from query:"
                f" {', '.join(sorted(s.name for s in sorted(badstats)))}"
            )

    def _send_prepare(self, name: bytes, query: PostgresQuery) -> None:
        self._query = query.query
        self._conn.pgconn.send_prepare(
            name, query.query, param_types=query.types
        )

    def _send_query_prepared(self, name: bytes, pgq: PostgresQuery) -> None:
        self._params = pgq.params
        self._conn.pgconn.send_query_prepared(
            name,
            pgq.params,
            param_formats=pgq.formats,
            result_format=self.format,
        )

    def _check_result(self) -> None:
        res = self.pgresult
        if not res:
            raise e.ProgrammingError("no result available")
        elif res.status != ExecStatus.TUPLES_OK:
            raise e.ProgrammingError(
                "the last operation didn't produce a result"
            )

    def _check_copy_result(self, result: "PGresult") -> None:
        """
        Check that the value returned in a copy() operation is a legit COPY.
        """
        status = result.status
        if status in (ExecStatus.COPY_IN, ExecStatus.COPY_OUT):
            return
        elif status == ExecStatus.FATAL_ERROR:
            raise e.error_from_result(
                result, encoding=self._conn.client_encoding
            )
        else:
            raise e.ProgrammingError(
                "copy() should be used only with COPY ... TO STDOUT or COPY ..."
                f" FROM STDIN statements, got {ExecStatus(status).name}"
            )


class Cursor(BaseCursor["Connection"]):
    __module__ = "psycopg3"

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        self._closed = True
        self._reset()

    def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        prepare: Optional[bool] = None,
    ) -> "Cursor":
        """
        Execute a query or command to the database.
        """
        with self._conn.lock:
            self._conn.wait(self._execute_gen(query, params, prepare=prepare))
        return self

    def executemany(self, query: Query, params_seq: Sequence[Params]) -> None:
        """
        Execute the same command with a sequence of input data.
        """
        with self._conn.lock:
            self._conn.wait(self._executemany_gen(query, params_seq))

    def fetchone(self) -> Optional[Sequence[Any]]:
        """
        Return the next record from the current recordset.

        Return `!None` the recordset is finished.
        """
        self._check_result()
        rv = self._transformer.load_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    def fetchmany(self, size: int = 0) -> List[Sequence[Any]]:
        """
        Return the next *size* records from the current recordset.

        *size* default to `!self.arraysize` if not specified.
        """
        self._check_result()
        if not size:
            size = self.arraysize

        assert self.pgresult
        load = self._transformer.load_row
        rv: List[Any] = [None] * (min(size, self.pgresult.ntuples - self._pos))

        for i in range(len(rv)):
            rv[i] = load(i + self._pos)

        self._pos += len(rv)
        return rv

    def fetchall(self) -> List[Sequence[Any]]:
        """
        Return all the remaining records from the current recordset.
        """
        self._check_result()

        assert self.pgresult
        load = self._transformer.load_row

        rv: List[Any] = [None] * (self.pgresult.ntuples - self._pos)
        for i in range(len(rv)):
            rv[i] = load(i + self._pos)

        self._pos += len(rv)
        return rv

    def __iter__(self) -> Iterator[Sequence[Any]]:
        self._check_result()

        load = self._transformer.load_row

        while 1:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    @contextmanager
    def copy(self, statement: Query) -> Iterator[Copy]:
        """
        Initiate a :sql:`COPY` operation and return an object to manage it.
        """
        with self._conn.lock:
            self._conn.wait(self._start_copy_gen(statement))

        with Copy(self) as copy:
            yield copy


class AsyncCursor(BaseCursor["AsyncConnection"]):
    __module__ = "psycopg3"

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def close(self) -> None:
        self._closed = True
        self._reset()

    async def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        prepare: Optional[bool] = None,
    ) -> "AsyncCursor":
        async with self._conn.lock:
            await self._conn.wait(
                self._execute_gen(query, params, prepare=prepare)
            )
        return self

    async def executemany(
        self, query: Query, params_seq: Sequence[Params]
    ) -> None:
        async with self._conn.lock:
            await self._conn.wait(self._executemany_gen(query, params_seq))

    async def fetchone(self) -> Optional[Sequence[Any]]:
        self._check_result()
        rv = self._transformer.load_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    async def fetchmany(self, size: int = 0) -> List[Sequence[Any]]:
        self._check_result()
        if not size:
            size = self.arraysize

        assert self.pgresult
        load = self._transformer.load_row
        rv: List[Any] = [None] * (min(size, self.pgresult.ntuples - self._pos))

        for i in range(len(rv)):
            rv[i] = load(i + self._pos)

        self._pos += len(rv)
        return rv

    async def fetchall(self) -> List[Sequence[Any]]:
        self._check_result()

        assert self.pgresult
        load = self._transformer.load_row

        rv: List[Any] = [None] * (self.pgresult.ntuples - self._pos)
        for i in range(len(rv)):
            rv[i] = load(i + self._pos)

        self._pos += len(rv)
        return rv

    async def __aiter__(self) -> AsyncIterator[Sequence[Any]]:
        self._check_result()

        load = self._transformer.load_row

        while 1:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    @asynccontextmanager
    async def copy(self, statement: Query) -> AsyncIterator[AsyncCopy]:
        async with self._conn.lock:
            await self._conn.wait(self._start_copy_gen(statement))

        async with AsyncCopy(self) as copy:
            yield copy


class NamedCursorMixin:
    pass


class NamedCursor(NamedCursorMixin, Cursor):
    pass


class AsyncNamedCursor(NamedCursorMixin, AsyncCursor):
    pass
