"""
psycopg3 cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from . import errors as e
from .pq import ExecStatus, PGresult, Format
from .utils.queries import query2pg, reorder_params
from .utils.typing import Query, Params

if TYPE_CHECKING:
    from .connection import (
        BaseConnection,
        Connection,
        AsyncConnection,
        QueryGen,
    )
    from .adapt import AdaptersMap, TypecastersMap


class BaseCursor:
    def __init__(self, conn: "BaseConnection", binary: bool = False):
        self.conn = conn
        self.binary = binary
        self.adapters: AdaptersMap = {}
        self.casters: TypecastersMap = {}
        self._reset()

    def _reset(self) -> None:
        from .adapt import Transformer

        self._results: List[PGresult] = []
        self.pgresult: Optional[PGresult] = None
        self._pos = 0
        self._iresult = 0
        self._transformer = Transformer(self)

    def _execute_send(
        self, query: Query, vars: Optional[Params]
    ) -> "QueryGen":
        # Implement part of execute() before waiting common to sync and async
        self._reset()

        codec = self.conn.codec

        if isinstance(query, str):
            query = codec.encode(query)[0]

        # process %% -> % only if there are paramters, even if empty list
        if vars is not None:
            query, formats, order = query2pg(query, vars, codec)
        if vars:
            if order is not None:
                assert isinstance(vars, Mapping)
                vars = reorder_params(vars, order)
            assert isinstance(vars, Sequence)
            params, types = self._transformer.adapt_sequence(vars, formats)
            self.conn.pgconn.send_query_params(
                query,
                params,
                param_formats=formats,
                param_types=types,
                result_format=Format(self.binary),
            )
        else:
            # if we don't have to, let's use exec_ as it can run more than
            # one query in one go
            if self.binary:
                self.conn.pgconn.send_query_params(
                    query, (), result_format=Format(self.binary)
                )
            else:
                self.conn.pgconn.send_query(query)

        return self.conn._exec_gen(self.conn.pgconn)

    def _execute_results(self, results: List[PGresult]) -> None:
        # Implement part of execute() after waiting common to sync and async
        if not results:
            raise e.InternalError("got no result from the query")

        badstats = {res.status for res in results} - {
            ExecStatus.TUPLES_OK,
            ExecStatus.COMMAND_OK,
            ExecStatus.EMPTY_QUERY,
        }
        if not badstats:
            self._results = results
            self.pgresult = results[0]
            return

        if results[-1].status == ExecStatus.FATAL_ERROR:
            raise e.error_from_result(results[-1])

        elif badstats & {
            ExecStatus.COPY_IN,
            ExecStatus.COPY_OUT,
            ExecStatus.COPY_BOTH,
        }:
            raise e.ProgrammingError(
                "COPY cannot be used with execute(); use copy() insead"
            )
        else:
            raise e.InternalError(
                f"got unexpected status from query:"
                f" {', '.join(sorted(s.name for s in sorted(badstats)))}"
            )

    def nextset(self) -> Optional[bool]:
        self._iresult += 1
        if self._iresult < len(self._results):
            self.pgresult = self._results[self._iresult]
            self._pos = 0
            return True
        else:
            return None

    def fetchone(self) -> Optional[Sequence[Any]]:
        rv = self._cast_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    def _cast_row(self, n: int) -> Optional[Tuple[Any, ...]]:
        if self.pgresult is None:
            return None
        if n >= self.pgresult.ntuples:
            return None

        return tuple(self._transformer.cast_row(self.pgresult, n))


class Cursor(BaseCursor):
    conn: "Connection"

    def __init__(self, conn: "Connection", binary: bool = False):
        super().__init__(conn, binary)

    def execute(self, query: Query, vars: Optional[Params] = None) -> "Cursor":
        with self.conn.lock:
            gen = self._execute_send(query, vars)
            results = self.conn.wait(gen)
            self._execute_results(results)
        return self


class AsyncCursor(BaseCursor):
    conn: "AsyncConnection"

    def __init__(self, conn: "AsyncConnection", binary: bool = False):
        super().__init__(conn, binary)

    async def execute(
        self, query: Query, vars: Optional[Params] = None
    ) -> "AsyncCursor":
        async with self.conn.lock:
            gen = self._execute_send(query, vars)
            results = await self.conn.wait(gen)
            self._execute_results(results)
        return self


class NamedCursorMixin:
    pass


class NamedCursor(NamedCursorMixin, Cursor):
    pass


class AsyncNamedCursor(NamedCursorMixin, AsyncCursor):
    pass
