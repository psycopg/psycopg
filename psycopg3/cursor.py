"""
psycopg3 cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

from . import exceptions as exc
from .pq import error_message, DiagnosticField, ExecStatus
from .utils.queries import query2pg, reorder_params


class BaseCursor:
    def __init__(self, conn, binary=False):
        self.conn = conn
        self.binary = binary
        self.adapters = {}
        self.casters = {}
        self._reset()

    def _reset(self):
        from .adaptation import Transformer

        self._results = []
        self._result = None
        self._pos = 0
        self._iresult = 0
        self._transformer = Transformer(self)

    def _execute_send(self, query, vars):
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
                vars = reorder_params(vars, order)
            params, types = self._transformer.adapt_sequence(vars, formats)
            self.conn.pgconn.send_query_params(
                query,
                params,
                param_formats=formats,
                param_types=types,
                result_format=int(self.binary),
            )
        else:
            self.conn.pgconn.send_query(query)

        return self.conn._exec_gen(self.conn.pgconn)

    def _execute_results(self, results):
        # Implement part of execute() after waiting common to sync and async
        if not results:
            raise exc.InternalError("got no result from the query")

        badstats = {res.status for res in results} - {
            ExecStatus.TUPLES_OK,
            ExecStatus.COMMAND_OK,
            ExecStatus.EMPTY_QUERY,
        }
        if not badstats:
            self._results = results
            self._result = results[0]
            return

        if results[-1].status == ExecStatus.FATAL_ERROR:
            ecls = exc.class_for_state(
                results[-1].error_field(DiagnosticField.SQLSTATE)
            )
            raise ecls(error_message(results[-1]))

        elif badstats & {
            ExecStatus.COPY_IN,
            ExecStatus.COPY_OUT,
            ExecStatus.COPY_BOTH,
        }:
            raise exc.ProgrammingError(
                "COPY cannot be used with execute(); use copy() insead"
            )
        else:
            raise exc.InternalError(
                f"got unexpected status from query:"
                f" {', '.join(sorted(s.name for s in sorted(badstats)))}"
            )

    def nextset(self):
        self._iresult += 1
        if self._iresult < len(self._results):
            self._result = self._results[self._iresult]
            self._pos = 0
            return True

    def fetchone(self):
        rv = self._cast_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    def _cast_row(self, n):
        if self._result is None:
            return None
        if n >= self._result.ntuples:
            return None

        return tuple(self._transformer.cast_row(self._result, n))


class Cursor(BaseCursor):
    def execute(self, query, vars=None):
        with self.conn.lock:
            gen = self._execute_send(query, vars)
            results = self.conn.wait(gen)
            self._execute_results(results)
        return self


class AsyncCursor(BaseCursor):
    async def execute(self, query, vars=None):
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
