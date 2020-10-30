"""
psycopg3 cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

from types import TracebackType
from typing import Any, Callable, List, Optional, Sequence, Type, TYPE_CHECKING
from operator import attrgetter

from . import errors as e
from . import pq
from . import proto
from .oids import builtins
from .copy import Copy, AsyncCopy
from .proto import Query, Params, DumpersMap, LoadersMap, PQGen
from .utils.queries import PostgresQuery

if TYPE_CHECKING:
    from .connection import BaseConnection, Connection, AsyncConnection

execute: Callable[[pq.proto.PGconn], PQGen[List[pq.proto.PGresult]]]

if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    execute = _psycopg3.execute

else:
    from . import generators

    execute = generators.execute


class Column(Sequence[Any]):
    def __init__(self, pgresult: pq.proto.PGresult, index: int, encoding: str):
        self._pgresult = pgresult
        self._index = index
        self._encoding = encoding

    _attrs = tuple(
        map(
            attrgetter,
            """
            name type_code display_size internal_size precision scale null_ok
            """.split(),
        )
    )

    def __len__(self) -> int:
        return 7

    def __getitem__(self, index: Any) -> Any:
        return self._attrs[index](self)

    @property
    def name(self) -> str:
        rv = self._pgresult.fname(self._index)
        if rv is not None:
            return rv.decode(self._encoding)
        else:
            raise e.InterfaceError(
                f"no name available for column {self._index}"
            )

    @property
    def type_code(self) -> int:
        return self._pgresult.ftype(self._index)


class BaseCursor:
    ExecStatus = pq.ExecStatus

    _transformer: proto.Transformer

    def __init__(
        self, connection: "BaseConnection", format: pq.Format = pq.Format.TEXT
    ):
        self.connection = connection
        self.format = format
        self.dumpers: DumpersMap = {}
        self.loaders: LoadersMap = {}
        self._reset()
        self.arraysize = 1
        self._closed = False

    def _reset(self) -> None:
        self._results: List[pq.proto.PGresult] = []
        self.pgresult = None
        self._pos = 0
        self._iresult = 0
        self._rowcount = -1

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def status(self) -> Optional[pq.ExecStatus]:
        res = self.pgresult
        if res is not None:
            return res.status
        else:
            return None

    @property
    def pgresult(self) -> Optional[pq.proto.PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[pq.proto.PGresult]) -> None:
        self._pgresult = result
        if result is not None:
            if self._transformer is not None:
                self._transformer.pgresult = result

    @property
    def description(self) -> Optional[List[Column]]:
        res = self.pgresult
        if res is None or res.status != self.ExecStatus.TUPLES_OK:
            return None
        return [
            Column(res, i, self.connection.codec.name)
            for i in range(res.nfields)
        ]

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        # no-op
        pass

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        # no-op
        pass

    def _start_query(self) -> None:
        from .adapt import Transformer

        if self.closed:
            raise e.InterfaceError("the cursor is closed")

        if self.connection.closed:
            raise e.InterfaceError("the connection is closed")

        if self.connection.status != self.connection.ConnStatus.OK:
            raise e.InterfaceError(
                f"cannot execute operations: the connection is"
                f" in status {self.connection.status}"
            )

        self._reset()
        self._transformer = Transformer(self)

    def _execute_send(
        self, query: Query, vars: Optional[Params], no_pqexec: bool = False
    ) -> None:
        """
        Implement part of execute() before waiting common to sync and async
        """
        pgq = PostgresQuery(self._transformer)
        pgq.convert(query, vars)

        if pgq.params or no_pqexec or self.format == pq.Format.BINARY:
            self.connection.pgconn.send_query_params(
                pgq.query,
                pgq.params,
                param_formats=pgq.formats,
                param_types=pgq.types,
                result_format=self.format,
            )
        else:
            # if we don't have to, let's use exec_ as it can run more than
            # one query in one go
            self.connection.pgconn.send_query(pgq.query)

    def _execute_results(self, results: Sequence[pq.proto.PGresult]) -> None:
        """
        Implement part of execute() after waiting common to sync and async
        """
        if not results:
            raise e.InternalError("got no result from the query")

        S = self.ExecStatus
        statuses = {res.status for res in results}
        badstats = statuses - {S.TUPLES_OK, S.COMMAND_OK, S.EMPTY_QUERY}
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

        if results[-1].status == S.FATAL_ERROR:
            raise e.error_from_result(
                results[-1], encoding=self.connection.codec.name
            )

        elif badstats & {S.COPY_IN, S.COPY_OUT, S.COPY_BOTH}:
            raise e.ProgrammingError(
                "COPY cannot be used with execute(); use copy() insead"
            )
        else:
            raise e.InternalError(
                f"got unexpected status from query:"
                f" {', '.join(sorted(s.name for s in sorted(badstats)))}"
            )

    def _send_prepare(
        self, name: bytes, query: Query, vars: Optional[Params]
    ) -> PostgresQuery:
        """
        Implement part of execute() before waiting common to sync and async
        """
        pgq = PostgresQuery(self._transformer)
        pgq.convert(query, vars)

        self.connection.pgconn.send_prepare(
            name, pgq.query, param_types=pgq.types
        )

        return pgq

    def _send_query_prepared(self, name: bytes, pgq: PostgresQuery) -> None:
        self.connection.pgconn.send_query_prepared(
            name,
            pgq.params,
            param_formats=pgq.formats,
            result_format=self.format,
        )

    def nextset(self) -> Optional[bool]:
        self._iresult += 1
        if self._iresult < len(self._results):
            self.pgresult = self._results[self._iresult]
            self._pos = 0
            nrows = self.pgresult.command_tuples
            self._rowcount = nrows if nrows is not None else -1
            return True
        else:
            return None

    def _check_result(self) -> None:
        res = self.pgresult
        if res is None:
            raise e.ProgrammingError("no result available")
        elif res.status != self.ExecStatus.TUPLES_OK:
            raise e.ProgrammingError(
                "the last operation didn't produce a result"
            )

    def _check_copy_results(
        self, results: Sequence[pq.proto.PGresult]
    ) -> None:
        """
        Check that the value returned in a copy() operation is a legit COPY.
        """
        if len(results) != 1:
            raise e.InternalError(
                f"expected 1 result from copy, got {len(results)} instead"
            )

        result = results[0]
        status = result.status
        if status in (pq.ExecStatus.COPY_IN, pq.ExecStatus.COPY_OUT):
            return
        elif status == pq.ExecStatus.FATAL_ERROR:
            raise e.error_from_result(
                result, encoding=self.connection.codec.name
            )
        else:
            raise e.ProgrammingError(
                "copy() should be used only with COPY ... TO STDOUT or COPY ..."
                f" FROM STDIN statements, got {pq.ExecStatus(status).name}"
            )


class Cursor(BaseCursor):
    connection: "Connection"

    def __init__(
        self, connection: "Connection", format: pq.Format = pq.Format.TEXT
    ):
        super().__init__(connection, format=format)

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
        self._closed = True
        self._reset()

    def execute(self, query: Query, vars: Optional[Params] = None) -> "Cursor":
        with self.connection.lock:
            self._start_query()
            self.connection._start_query()
            self._execute_send(query, vars)
            gen = execute(self.connection.pgconn)
            results = self.connection.wait(gen)
            self._execute_results(results)
        return self

    def executemany(
        self, query: Query, vars_seq: Sequence[Params]
    ) -> "Cursor":
        with self.connection.lock:
            self._start_query()
            self.connection._start_query()
            first = True
            for vars in vars_seq:
                if first:
                    pgq = self._send_prepare(b"", query, vars)
                    gen = execute(self.connection.pgconn)
                    (result,) = self.connection.wait(gen)
                    if result.status == self.ExecStatus.FATAL_ERROR:
                        raise e.error_from_result(
                            result, encoding=self.connection.codec.name
                        )
                else:
                    pgq.dump(vars)

                self._send_query_prepared(b"", pgq)
                gen = execute(self.connection.pgconn)
                (result,) = self.connection.wait(gen)
                self._execute_results((result,))

        return self

    def fetchone(self) -> Optional[Sequence[Any]]:
        self._check_result()
        rv = self._transformer.load_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    def fetchmany(self, size: Optional[int] = None) -> List[Sequence[Any]]:
        self._check_result()
        if size is None:
            size = self.arraysize

        rv: List[Sequence[Any]] = []
        pos = self._pos
        load = self._transformer.load_row

        for _ in range(size):
            row = load(pos)
            if row is None:
                break
            pos += 1
            rv.append(row)

        self._pos = pos
        return rv

    def fetchall(self) -> List[Sequence[Any]]:
        self._check_result()

        rv: List[Sequence[Any]] = []
        pos = self._pos
        load = self._transformer.load_row

        while 1:
            row = load(pos)
            if row is None:
                break
            pos += 1
            rv.append(row)

        self._pos = pos
        return rv

    def copy(self, statement: Query, vars: Optional[Params] = None) -> Copy:
        with self.connection.lock:
            self._start_query()
            self.connection._start_query()
            # Make sure to avoid PQexec to avoid receiving a mix of COPY and
            # other operations.
            self._execute_send(statement, vars, no_pqexec=True)
            gen = execute(self.connection.pgconn)
            results = self.connection.wait(gen)
            tx = self._transformer

        self._check_copy_results(results)
        return Copy(
            context=tx, result=results[0], format=results[0].binary_tuples
        )


class AsyncCursor(BaseCursor):
    connection: "AsyncConnection"

    def __init__(
        self, connection: "AsyncConnection", format: pq.Format = pq.Format.TEXT
    ):
        super().__init__(connection, format=format)

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
        self, query: Query, vars: Optional[Params] = None
    ) -> "AsyncCursor":
        async with self.connection.lock:
            self._start_query()
            await self.connection._start_query()
            self._execute_send(query, vars)
            gen = execute(self.connection.pgconn)
            results = await self.connection.wait(gen)
            self._execute_results(results)
        return self

    async def executemany(
        self, query: Query, vars_seq: Sequence[Params]
    ) -> "AsyncCursor":
        async with self.connection.lock:
            self._start_query()
            await self.connection._start_query()
            first = True
            for vars in vars_seq:
                if first:
                    pgq = self._send_prepare(b"", query, vars)
                    gen = execute(self.connection.pgconn)
                    (result,) = await self.connection.wait(gen)
                    if result.status == self.ExecStatus.FATAL_ERROR:
                        raise e.error_from_result(
                            result, encoding=self.connection.codec.name
                        )
                else:
                    pgq.dump(vars)

                self._send_query_prepared(b"", pgq)
                gen = execute(self.connection.pgconn)
                (result,) = await self.connection.wait(gen)
                self._execute_results((result,))

        return self

    async def fetchone(self) -> Optional[Sequence[Any]]:
        self._check_result()
        rv = self._transformer.load_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    async def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Sequence[Any]]:
        self._check_result()
        if size is None:
            size = self.arraysize

        pos = self._pos
        load = self._transformer.load_row
        rv: List[Sequence[Any]] = []

        for i in range(size):
            row = load(pos)
            if row is None:
                break
            pos += 1
            rv.append(row)

        self._pos = pos
        return rv

    async def fetchall(self) -> List[Sequence[Any]]:
        self._check_result()

        rv: List[Sequence[Any]] = []
        pos = self._pos
        load = self._transformer.load_row

        while 1:
            row = load(pos)
            if row is None:
                break
            pos += 1
            rv.append(row)

        self._pos = pos
        return rv

    async def copy(
        self, statement: Query, vars: Optional[Params] = None
    ) -> AsyncCopy:
        async with self.connection.lock:
            self._start_query()
            await self.connection._start_query()
            # Make sure to avoid PQexec to avoid receiving a mix of COPY and
            # other operations.
            self._execute_send(statement, vars, no_pqexec=True)
            gen = execute(self.connection.pgconn)
            results = await self.connection.wait(gen)
            tx = self._transformer

        self._check_copy_results(results)
        return AsyncCopy(
            context=tx, result=results[0], format=results[0].binary_tuples
        )


class NamedCursorMixin:
    pass


class NamedCursor(NamedCursorMixin, Cursor):
    pass


class AsyncNamedCursor(NamedCursorMixin, AsyncCursor):
    pass
