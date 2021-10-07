"""
psycopg cursor objects
"""

# Copyright (C) 2020-2021 The Psycopg Team

import sys
from types import TracebackType
from typing import Any, Callable, Generic, Iterator, List
from typing import Optional, NoReturn, Sequence, Type, TypeVar, TYPE_CHECKING
from contextlib import contextmanager

from . import pq
from . import adapt
from . import errors as e

from .pq import ExecStatus, Format
from .abc import ConnectionType, Query, Params, PQGen
from .copy import Copy
from .rows import Row, RowMaker, RowFactory
from ._column import Column
from ._cmodule import _psycopg
from ._queries import PostgresQuery
from ._encodings import pgconn_encoding
from ._preparing import Prepare

if TYPE_CHECKING:
    from .abc import Transformer
    from .pq.abc import PGconn, PGresult
    from .connection import Connection

execute: Callable[["PGconn"], PQGen[List["PGresult"]]]

if _psycopg:
    execute = _psycopg.execute
    fetch = _psycopg.fetch
    send = _psycopg.send

else:
    from . import generators

    execute = generators.execute
    fetch = generators.fetch
    send = generators.send

_C = TypeVar("_C", bound="Cursor[Any]")


class BaseCursor(Generic[ConnectionType, Row]):
    # Slots with __weakref__ and generic bases don't work on Py 3.6
    # https://bugs.python.org/issue41451
    if sys.version_info >= (3, 7):
        __slots__ = """
            _conn format _adapters arraysize _closed _results pgresult _pos
            _iresult _rowcount _query _tx _last_query _row_factory _make_row
            _pgconn
            __weakref__
            """.split()

    ExecStatus = pq.ExecStatus

    _tx: "Transformer"
    _make_row: RowMaker[Row]
    _pgconn: "PGconn"

    def __init__(self, connection: ConnectionType):
        self._conn = connection
        self.format = Format.TEXT
        self._pgconn = connection.pgconn
        self._adapters = adapt.AdaptersMap(connection.adapters)
        self.arraysize = 1
        self._closed = False
        self._last_query: Optional[Query] = None
        self._reset()

    def _reset(self) -> None:
        self._results: List["PGresult"] = []
        self.pgresult: Optional["PGresult"] = None
        self._pos = 0
        self._iresult = 0
        self._rowcount = -1
        self._query: Optional[PostgresQuery] = None

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self._pgconn)
        if self._closed:
            status = "closed"
        elif self.pgresult:
            status = pq.ExecStatus(self.pgresult.status).name
        else:
            status = "no result"
        return f"<{cls} [{status}] {info} at 0x{id(self):x}>"

    @property
    def connection(self) -> ConnectionType:
        """The connection this cursor is using."""
        return self._conn

    @property
    def adapters(self) -> adapt.AdaptersMap:
        return self._adapters

    @property
    def closed(self) -> bool:
        """`True` if the cursor is closed."""
        return self._closed

    @property
    def description(self) -> Optional[List[Column]]:
        """
        A list of `Column` objects describing the current resultset.

        `!None` if the current resultset didn't return tuples.
        """
        res = self.pgresult

        # We return columns if we have nfields, but also if we don't but
        # the query said we got tuples (mostly to handle the super useful
        # query "SELECT ;"
        if res and (
            res.nfields
            or res.status == ExecStatus.TUPLES_OK
            or res.status == ExecStatus.SINGLE_TUPLE
        ):
            return [Column(self, i) for i in range(res.nfields)]
        else:
            return None

    @property
    def rowcount(self) -> int:
        """Number of records affected by the precedent operation."""
        return self._rowcount

    @property
    def rownumber(self) -> Optional[int]:
        """Index of the next row to fetch in the current result.

        `!None` if there is no result to fetch.
        """
        return self._pos if self.pgresult else None

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
            self._tx.set_pgresult(self._results[self._iresult])
            self._make_row = self._make_row_maker()
            self._pos = 0
            nrows = self.pgresult.command_tuples
            self._rowcount = nrows if nrows is not None else -1
            return True
        else:
            return None

    @property
    def statusmessage(self) -> Optional[str]:
        """
        The command status tag from the last SQL command executed.

        `!None` if the cursor doesn't have a result available.
        """
        msg = self.pgresult.command_status if self.pgresult else None
        return msg.decode() if msg else None

    def _make_row_maker(self) -> RowMaker[Row]:
        raise NotImplementedError

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
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> PQGen[None]:
        """Generator implementing `Cursor.execute()`."""
        yield from self._start_query(query)
        pgq = self._convert_query(query, params)
        results = yield from self._maybe_prepare_gen(
            pgq, prepare=prepare, binary=binary
        )
        self._execute_results(results)
        self._last_query = query

    def _executemany_gen(
        self, query: Query, params_seq: Sequence[Params]
    ) -> PQGen[None]:
        """Generator implementing `Cursor.executemany()`."""
        yield from self._start_query(query)
        first = True
        for params in params_seq:
            if first:
                pgq = self._convert_query(query, params)
                self._query = pgq
                first = False
            else:
                pgq.dump(params)

            results = yield from self._maybe_prepare_gen(pgq, prepare=True)
            self._execute_results(results)

        self._last_query = query

    def _maybe_prepare_gen(
        self,
        pgq: PostgresQuery,
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> PQGen[Sequence["PGresult"]]:
        # Check if the query is prepared or needs preparing
        prep, name = self._conn._prepared.get(pgq, prepare)
        if prep is Prepare.YES:
            # The query is already prepared
            self._send_query_prepared(name, pgq, binary=binary)

        elif prep is Prepare.NO:
            # The query must be executed without preparing
            self._execute_send(pgq, binary=binary)

        else:
            # The query must be prepared and executed
            self._send_prepare(name, pgq)
            (result,) = yield from execute(self._pgconn)
            if result.status == ExecStatus.FATAL_ERROR:
                raise e.error_from_result(
                    result, encoding=pgconn_encoding(self._pgconn)
                )
            self._send_query_prepared(name, pgq, binary=binary)

        # run the query
        results = yield from execute(self._pgconn)

        # Update the prepare state of the query.
        # If an operation requires to flush our prepared statements cache, do it.
        cmd = self._conn._prepared.maintain(pgq, results, prep, name)
        if cmd:
            yield from self._conn._exec_command(cmd)

        return results

    def _stream_send_gen(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
    ) -> PQGen[None]:
        """Generator to send the query for `Cursor.stream()`."""
        yield from self._start_query(query)
        pgq = self._convert_query(query, params)
        self._execute_send(pgq, binary=binary, no_pqexec=True)
        self._pgconn.set_single_row_mode()
        self._last_query = query

    def _stream_fetchone_gen(self, first: bool) -> PQGen[Optional["PGresult"]]:
        yield from send(self._pgconn)
        res = yield from fetch(self._pgconn)
        if res is None:
            return None

        elif res.status == ExecStatus.SINGLE_TUPLE:
            self.pgresult = res
            self._tx.set_pgresult(res, set_loaders=first)
            if first:
                self._make_row = self._make_row_maker()
            return res

        elif res.status in (ExecStatus.TUPLES_OK, ExecStatus.COMMAND_OK):
            # End of single row results
            status = res.status
            while res:
                res = yield from fetch(self._pgconn)
            if status != ExecStatus.TUPLES_OK:
                raise e.ProgrammingError(
                    "the operation in stream() didn't produce a result"
                )
            return None

        else:
            # Errors, unexpected values
            return self._raise_from_results([res])

    def _start_query(self, query: Optional[Query] = None) -> PQGen[None]:
        """Generator to start the processing of a query.

        It is implemented as generator because it may send additional queries,
        such as `begin`.
        """
        if self.closed:
            raise e.InterfaceError("the cursor is closed")

        self._reset()
        if not self._last_query or (self._last_query is not query):
            self._last_query = None
            self._tx = adapt.Transformer(self)
        yield from self._conn._start_query()

    def _start_copy_gen(self, statement: Query) -> PQGen[None]:
        """Generator implementing sending a command for `Cursor.copy()."""
        yield from self._start_query()
        query = self._convert_query(statement)

        self._execute_send(query, binary=False)
        results = yield from execute(self._pgconn)
        if len(results) != 1:
            raise e.ProgrammingError(
                "COPY cannot be mixed with other operations"
            )

        result = results[0]
        self._check_copy_result(result)
        self.pgresult = result
        self._tx.set_pgresult(result)

    def _execute_send(
        self,
        query: PostgresQuery,
        *,
        no_pqexec: bool = False,
        binary: Optional[bool] = None,
    ) -> None:
        """
        Implement part of execute() before waiting common to sync and async.

        This is not a generator, but a normal non-blocking function.
        """
        if binary is None:
            fmt = self.format
        else:
            fmt = Format.BINARY if binary else Format.TEXT

        self._query = query
        if query.params or no_pqexec or fmt == Format.BINARY:
            self._pgconn.send_query_params(
                query.query,
                query.params,
                param_formats=query.formats,
                param_types=query.types,
                result_format=fmt,
            )
        else:
            # if we don't have to, let's use exec_ as it can run more than
            # one query in one go
            self._pgconn.send_query(query.query)

    def _convert_query(
        self, query: Query, params: Optional[Params] = None
    ) -> PostgresQuery:
        pgq = PostgresQuery(self._tx)
        pgq.convert(query, params)
        return pgq

    _status_ok = (
        ExecStatus.TUPLES_OK,
        ExecStatus.COMMAND_OK,
        ExecStatus.EMPTY_QUERY,
    )
    _status_copy = (
        ExecStatus.COPY_IN,
        ExecStatus.COPY_OUT,
        ExecStatus.COPY_BOTH,
    )

    def _execute_results(
        self, results: Sequence["PGresult"], format: Optional[Format] = None
    ) -> None:
        """
        Implement part of execute() after waiting common to sync and async

        This is not a generator, but a normal non-blocking function.
        """
        if not results:
            raise e.InternalError("got no result from the query")

        for res in results:
            if res.status not in self._status_ok:
                self._raise_from_results(results)

        self._results = list(results)
        self.pgresult = results[0]

        # Note: the only reason to override format is to correclty set
        # binary loaders on server-side cursors, because send_describe_portal
        # only returns a text result.
        self._tx.set_pgresult(results[0], format=format)

        self._make_row = self._make_row_maker()
        nrows = self.pgresult.command_tuples
        if nrows is not None:
            if self._rowcount < 0:
                self._rowcount = nrows
            else:
                self._rowcount += nrows

    def _raise_from_results(self, results: Sequence["PGresult"]) -> NoReturn:
        statuses = {res.status for res in results}
        badstats = statuses.difference(self._status_ok)
        if results[-1].status == ExecStatus.FATAL_ERROR:
            raise e.error_from_result(
                results[-1], encoding=pgconn_encoding(self._pgconn)
            )
        elif statuses.intersection(self._status_copy):
            raise e.ProgrammingError(
                "COPY cannot be used with this method; use copy() insead"
            )
        else:
            raise e.InternalError(
                f"got unexpected status from query:"
                f" {', '.join(sorted(ExecStatus(s).name for s in badstats))}"
            )

    def _send_prepare(self, name: bytes, query: PostgresQuery) -> None:
        self._pgconn.send_prepare(name, query.query, param_types=query.types)

    def _send_query_prepared(
        self, name: bytes, pgq: PostgresQuery, *, binary: Optional[bool] = None
    ) -> None:
        if binary is None:
            fmt = self.format
        else:
            fmt = Format.BINARY if binary else Format.TEXT

        self._pgconn.send_query_prepared(
            name, pgq.params, param_formats=pgq.formats, result_format=fmt
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
                result, encoding=pgconn_encoding(self._pgconn)
            )
        else:
            raise e.ProgrammingError(
                "copy() should be used only with COPY ... TO STDOUT or COPY ..."
                f" FROM STDIN statements, got {ExecStatus(status).name}"
            )

    def _scroll(self, value: int, mode: str) -> None:
        self._check_result()
        assert self.pgresult
        if mode == "relative":
            newpos = self._pos + value
        elif mode == "absolute":
            newpos = value
        else:
            raise ValueError(
                f"bad mode: {mode}. It should be 'relative' or 'absolute'"
            )
        if not 0 <= newpos < self.pgresult.ntuples:
            raise IndexError("position out of bound")
        self._pos = newpos

    def _close(self) -> None:
        self._closed = True


class Cursor(BaseCursor["Connection[Any]", Row]):
    __module__ = "psycopg"
    __slots__ = ()

    def __init__(
        self, connection: "Connection[Any]", *, row_factory: RowFactory[Row]
    ):
        super().__init__(connection)
        self._row_factory = row_factory

    def __enter__(self: _C) -> _C:
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
        self._close()

    @property
    def row_factory(self) -> RowFactory[Row]:
        """Writable attribute to control how result rows are formed."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, row_factory: RowFactory[Row]) -> None:
        self._row_factory = row_factory
        if self.pgresult:
            self._make_row = row_factory(self)

    def _make_row_maker(self) -> RowMaker[Row]:
        return self._row_factory(self)

    def execute(
        self: _C,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> _C:
        """
        Execute a query or command to the database.
        """
        try:
            with self._conn.lock:
                self._conn.wait(
                    self._execute_gen(
                        query, params, prepare=prepare, binary=binary
                    )
                )
        except e.Error as ex:
            raise ex.with_traceback(None)
        return self

    def executemany(self, query: Query, params_seq: Sequence[Params]) -> None:
        """
        Execute the same command with a sequence of input data.
        """
        with self._conn.lock:
            self._conn.wait(self._executemany_gen(query, params_seq))

    def stream(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
    ) -> Iterator[Row]:
        """
        Iterate row-by-row on a result from the database.
        """
        with self._conn.lock:
            self._conn.wait(
                self._stream_send_gen(query, params, binary=binary)
            )
            first = True
            while self._conn.wait(self._stream_fetchone_gen(first)):
                rec = self._tx.load_row(0, self._make_row)
                assert rec is not None
                yield rec
                first = False

    def fetchone(self) -> Optional[Row]:
        """
        Return the next record from the current recordset.

        Return `!None` the recordset is finished.

        :rtype: Optional[Row], with Row defined by `row_factory`
        """
        self._check_result()
        record = self._tx.load_row(self._pos, self._make_row)
        if record is not None:
            self._pos += 1
        return record

    def fetchmany(self, size: int = 0) -> List[Row]:
        """
        Return the next *size* records from the current recordset.

        *size* default to `!self.arraysize` if not specified.

        :rtype: Sequence[Row], with Row defined by `row_factory`
        """
        self._check_result()
        assert self.pgresult

        if not size:
            size = self.arraysize
        records = self._tx.load_rows(
            self._pos,
            min(self._pos + size, self.pgresult.ntuples),
            self._make_row,
        )
        self._pos += len(records)
        return records

    def fetchall(self) -> List[Row]:
        """
        Return all the remaining records from the current recordset.

        :rtype: Sequence[Row], with Row defined by `row_factory`
        """
        self._check_result()
        assert self.pgresult
        records = self._tx.load_rows(
            self._pos, self.pgresult.ntuples, self._make_row
        )
        self._pos = self.pgresult.ntuples
        return records

    def __iter__(self) -> Iterator[Row]:
        self._check_result()

        def load(pos: int) -> Optional[Row]:
            return self._tx.load_row(pos, self._make_row)

        while 1:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    def scroll(self, value: int, mode: str = "relative") -> None:
        """
        Move the cursor in the result set to a new position according to mode.

        If *mode* is ``relative`` (default), value is taken as offset to the
        current position in the result set, if set to ``absolute``, *value*
        states an absolute target position.

        Raise `!IndexError` in case a scroll operation would leave the result
        set. In this case the position will not change.
        """
        self._scroll(value, mode)

    @contextmanager
    def copy(self, statement: Query) -> Iterator[Copy]:
        """
        Initiate a :sql:`COPY` operation and return an object to manage it.

        :rtype: Copy
        """
        with self._conn.lock:
            self._conn.wait(self._start_copy_gen(statement))

        with Copy(self) as copy:
            yield copy
