"""
psycopg3 cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from operator import attrgetter
from typing import Any, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from . import errors as e
from . import pq
from . import generators
from .utils.queries import query2pg, reorder_params
from .utils.typing import Query, Params

if TYPE_CHECKING:
    from .adapt import DumpersMap, LoadersMap, Transformer
    from .connection import BaseConnection, Connection, AsyncConnection
    from .generators import QueryGen


class Column(Sequence[Any]):
    def __init__(
        self, pgresult: pq.PGresult, index: int, codec: codecs.CodecInfo
    ):
        self._pgresult = pgresult
        self._index = index
        self._codec = codec

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
            return self._codec.decode(rv)[0]
        else:
            raise e.InterfaceError(
                f"no name available for column {self._index}"
            )

    @property
    def type_code(self) -> int:
        return self._pgresult.ftype(self._index)


class BaseCursor:
    ExecStatus = pq.ExecStatus

    _transformer: "Transformer"

    def __init__(self, connection: "BaseConnection", binary: bool = False):
        self.connection = connection
        self.binary = binary
        self.dumpers: DumpersMap = {}
        self.loaders: LoadersMap = {}
        self._reset()
        self.arraysize = 1
        self._closed = False

    def _reset(self) -> None:
        self._results: List[pq.PGresult] = []
        self.pgresult = None
        self._pos = 0
        self._iresult = 0

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        self._closed = True
        self._reset()

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
    def pgresult(self) -> Optional[pq.PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[pq.PGresult]) -> None:
        self._pgresult = result
        if result is not None and self._transformer is not None:
            self._transformer.set_row_types(
                (result.ftype(i), result.fformat(i))
                for i in range(result.nfields)
            )

    @property
    def description(self) -> Optional[List[Column]]:
        res = self.pgresult
        if res is None or res.status != self.ExecStatus.TUPLES_OK:
            return None
        return [
            Column(res, i, self.connection.codec) for i in range(res.nfields)
        ]

    @property
    def rowcount(self) -> int:
        res = self.pgresult
        if res is None or res.status != self.ExecStatus.TUPLES_OK:
            return -1
        else:
            return res.ntuples

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        # no-op
        pass

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        # no-op
        pass

    def _execute_send(
        self, query: Query, vars: Optional[Params]
    ) -> "QueryGen":
        """
        Implement part of execute() before waiting common to sync and async
        """
        from .adapt import Transformer

        if self.closed:
            raise e.OperationalError("the cursor is closed")

        if self.connection.closed:
            raise e.OperationalError("the connection is closed")

        if self.connection.status != self.connection.ConnStatus.OK:
            raise e.InterfaceError(
                f"cannot execute operations: the connection is"
                f" in status {self.connection.status}"
            )

        self._reset()
        self._transformer = Transformer(self)

        codec = self.connection.codec

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
            params, types = self._transformer.dump_sequence(vars, formats)
            self.connection.pgconn.send_query_params(
                query,
                params,
                param_formats=formats,
                param_types=types,
                result_format=pq.Format(self.binary),
            )
        else:
            # if we don't have to, let's use exec_ as it can run more than
            # one query in one go
            if self.binary:
                self.connection.pgconn.send_query_params(
                    query, (), result_format=pq.Format(self.binary)
                )
            else:
                self.connection.pgconn.send_query(query)

        return generators.execute(self.connection.pgconn)

    def _execute_results(self, results: List[pq.PGresult]) -> None:
        """
        Implement part of execute() after waiting common to sync and async
        """
        if not results:
            raise e.InternalError("got no result from the query")

        S = self.ExecStatus
        statuses = {res.status for res in results}
        badstats = statuses - {S.TUPLES_OK, S.COMMAND_OK, S.EMPTY_QUERY}
        if not badstats:
            self._results = results
            self.pgresult = results[0]
            return

        if results[-1].status == S.FATAL_ERROR:
            raise e.error_from_result(results[-1])

        elif badstats & {S.COPY_IN, S.COPY_OUT, S.COPY_BOTH}:
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

    def _load_row(self, n: int) -> Optional[Tuple[Any, ...]]:
        res = self.pgresult
        if res is None:
            raise e.ProgrammingError("no result available")
        elif res.status != self.ExecStatus.TUPLES_OK:
            raise e.ProgrammingError(
                "the last operation didn't produce a result"
            )

        if n >= res.ntuples:
            return None

        return tuple(
            self._transformer.load_sequence(
                res.get_value(n, i) for i in range(res.nfields)
            )
        )


class Cursor(BaseCursor):
    connection: "Connection"

    def __init__(self, connection: "Connection", binary: bool = False):
        super().__init__(connection, binary)

    def execute(self, query: Query, vars: Optional[Params] = None) -> "Cursor":
        with self.connection.lock:
            gen = self._execute_send(query, vars)
            results = self.connection.wait(gen)
            self._execute_results(results)
        return self

    def executemany(
        self, query: Query, vars_seq: Sequence[Params]
    ) -> "Cursor":
        with self.connection.lock:
            for vars in vars_seq:
                gen = self._execute_send(query, vars)
                results = self.connection.wait(gen)
                self._execute_results(results)
        return self

    def fetchone(self) -> Optional[Sequence[Any]]:
        rv = self._load_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv

    def fetchmany(self, size: Optional[int] = None) -> List[Sequence[Any]]:
        if size is None:
            size = self.arraysize

        rv: List[Sequence[Any]] = []
        while len(rv) < size:
            row = self._load_row(self._pos)
            if row is None:
                break
            self._pos += 1
            rv.append(row)

        return rv

    def fetchall(self) -> List[Sequence[Any]]:
        rv: List[Sequence[Any]] = []
        while 1:
            row = self._load_row(self._pos)
            if row is None:
                break
            self._pos += 1
            rv.append(row)

        return rv


class AsyncCursor(BaseCursor):
    connection: "AsyncConnection"

    def __init__(self, connection: "AsyncConnection", binary: bool = False):
        super().__init__(connection, binary)

    async def execute(
        self, query: Query, vars: Optional[Params] = None
    ) -> "AsyncCursor":
        async with self.connection.lock:
            gen = self._execute_send(query, vars)
            results = await self.connection.wait(gen)
            self._execute_results(results)
        return self

    async def fetchone(self) -> Optional[Sequence[Any]]:
        rv = self._load_row(self._pos)
        if rv is not None:
            self._pos += 1
        return rv


class NamedCursorMixin:
    pass


class NamedCursor(NamedCursorMixin, Cursor):
    pass


class AsyncNamedCursor(NamedCursorMixin, AsyncCursor):
    pass
