"""
psycopg3 cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Generic,
    Iterator,
    List,
    Mapping,
)
from typing import Optional, Sequence, Type, TYPE_CHECKING, Union
from operator import attrgetter

from . import errors as e
from . import pq
from . import sql
from .oids import builtins
from .copy import Copy, AsyncCopy
from .proto import ConnectionType, Query, Params, DumpersMap, LoadersMap, PQGen
from .utils.queries import PostgresQuery

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


class Column(Sequence[Any]):
    def __init__(self, pgresult: "PGresult", index: int, encoding: str):
        self._pgresult = pgresult
        self._index = index
        self._encoding = encoding

    _attrs = tuple(
        attrgetter(attr)
        for attr in """
            name type_code display_size internal_size precision scale null_ok
            """.split()
    )

    def __repr__(self) -> str:
        return f"<Column {self.name}, type: {self._type_display()}>"

    def __len__(self) -> int:
        return 7

    def _type_display(self) -> str:
        parts = []
        t = builtins.get(self.type_code)
        parts.append(t.name if t else str(self.type_code))

        mod1 = self.precision
        if mod1 is None:
            mod1 = self.display_size
        if mod1:
            parts.append(f"({mod1}")
            if self.scale:
                parts.append(f", {self.scale}")
            parts.append(")")

        return "".join(parts)

    def __getitem__(self, index: Any) -> Any:
        if isinstance(index, slice):
            return tuple(getter(self) for getter in self._attrs[index])
        else:
            return self._attrs[index](self)

    @property
    def name(self) -> str:
        """The name of the column."""
        rv = self._pgresult.fname(self._index)
        if rv:
            return rv.decode(self._encoding)
        else:
            raise e.InterfaceError(
                f"no name available for column {self._index}"
            )

    @property
    def type_code(self) -> int:
        """The numeric OID of the column."""
        return self._pgresult.ftype(self._index)

    @property
    def display_size(self) -> Optional[int]:
        """The field size, for :sql:`varchar(n)`, None otherwise."""
        t = builtins.get(self.type_code)
        if not t:
            return None

        if t.name in ("varchar", "char"):
            fmod = self._pgresult.fmod(self._index)
            if fmod >= 0:
                return fmod - 4

        return None

    @property
    def internal_size(self) -> Optional[int]:
        """The interal field size for fixed-size types, None otherwise."""
        fsize = self._pgresult.fsize(self._index)
        return fsize if fsize >= 0 else None

    @property
    def precision(self) -> Optional[int]:
        """The number of digits for fixed precision types."""
        t = builtins.get(self.type_code)
        if not t:
            return None

        dttypes = ("time", "timetz", "timestamp", "timestamptz", "interval")
        if t.name == "numeric":
            fmod = self._pgresult.fmod(self._index)
            if fmod >= 0:
                return fmod >> 16

        elif t.name in dttypes:
            fmod = self._pgresult.fmod(self._index)
            if fmod >= 0:
                return fmod & 0xFFFF

        return None

    @property
    def scale(self) -> Optional[int]:
        """The number of digits after the decimal point if available.

        TODO: probably better than precision for datetime objects? review.
        """
        if self.type_code == builtins["numeric"].oid:
            fmod = self._pgresult.fmod(self._index) - 4
            if fmod >= 0:
                return fmod & 0xFFFF

        return None

    @property
    def null_ok(self) -> Optional[bool]:
        """Always `None`"""
        return None


class BaseCursor(Generic[ConnectionType]):
    ExecStatus = pq.ExecStatus

    _transformer: "Transformer"

    def __init__(
        self,
        connection: ConnectionType,
        format: pq.Format = pq.Format.TEXT,
    ):
        self.connection = connection
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
        A list of `Column` object describing the current resultset.

        `None` if the current resulset didn't return tuples.
        """
        res = self.pgresult
        if not res or res.status != self.ExecStatus.TUPLES_OK:
            return None
        encoding = self.connection.client_encoding
        return [Column(res, i, encoding) for i in range(res.nfields)]

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

    def _start_query(self) -> None:
        from . import adapt

        if self.closed:
            raise e.InterfaceError("the cursor is closed")

        if self.connection.closed:
            raise e.InterfaceError("the connection is closed")

        if self.connection.pgconn.status != self.connection.ConnStatus.OK:
            raise e.InterfaceError(
                f"cannot execute operations: the connection is"
                f" in status {self.connection.pgconn.status}"
            )

        self._reset()
        self._transformer = adapt.Transformer(self)

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

    def _execute_results(self, results: Sequence["PGresult"]) -> None:
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
                results[-1], encoding=self.connection.client_encoding
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

    def _check_result(self) -> None:
        res = self.pgresult
        if not res:
            raise e.ProgrammingError("no result available")
        elif res.status != self.ExecStatus.TUPLES_OK:
            raise e.ProgrammingError(
                "the last operation didn't produce a result"
            )

    def _callproc_sql(
        self,
        name: Union[str, sql.Identifier],
        args: Optional[Params] = None,
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> sql.Composable:
        if args and not isinstance(args, (Sequence, Mapping)):
            raise TypeError(
                f"callproc args should be a sequence or a mapping,"
                f" got {type(args).__name__}"
            )
        if isinstance(args, Mapping) and kwargs:
            raise TypeError(
                "callproc supports only one args sequence and one kwargs mapping"
            )

        if not kwargs and isinstance(args, Mapping):
            kwargs = args
            args = None

        if kwargs and not isinstance(kwargs, Mapping):
            raise TypeError(
                f"callproc kwargs should be a mapping,"
                f" got {type(kwargs).__name__}"
            )

        qparts: List[sql.Composable] = [
            sql.SQL("select * from "),
            name if isinstance(name, sql.Identifier) else sql.Identifier(name),
            sql.SQL("("),
        ]

        if args:
            for i, item in enumerate(args):
                if i:
                    qparts.append(sql.SQL(","))
                qparts.append(sql.Literal(item))

        if kwargs:
            for i, (k, v) in enumerate(kwargs.items()):
                if i:
                    qparts.append(sql.SQL(","))
                qparts.extend(
                    [sql.Identifier(k), sql.SQL(":="), sql.Literal(v)]
                )

        qparts.append(sql.SQL(")"))
        return sql.Composed(qparts)

    def _check_copy_results(self, results: Sequence["PGresult"]) -> None:
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
                result, encoding=self.connection.client_encoding
            )
        else:
            raise e.ProgrammingError(
                "copy() should be used only with COPY ... TO STDOUT or COPY ..."
                f" FROM STDIN statements, got {pq.ExecStatus(status).name}"
            )


class Cursor(BaseCursor["Connection"]):
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

    def execute(self, query: Query, vars: Optional[Params] = None) -> "Cursor":
        """
        Execute a query or command to the database.
        """
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
        """
        Execute the same command with a sequence of input data.
        """
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
                            result, encoding=self.connection.client_encoding
                        )
                else:
                    pgq.dump(vars)

                self._send_query_prepared(b"", pgq)
                gen = execute(self.connection.pgconn)
                (result,) = self.connection.wait(gen)
                self._execute_results((result,))

        return self

    def callproc(
        self,
        name: Union[str, sql.Identifier],
        args: Optional[Params] = None,
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Params]:
        """
        Call a stored procedure to the database.
        """
        self.execute(self._callproc_sql(name, args))
        return args

    def fetchone(self) -> Optional[Sequence[Any]]:
        """
        Return the next record from the current recordset, `None` if not available.
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
        """
        Return all the remaining records from the current recordset.
        """
        return list(self)

    def __iter__(self) -> Iterator[Sequence[Any]]:
        self._check_result()

        load = self._transformer.load_row

        while 1:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    def copy(self, statement: Query, vars: Optional[Params] = None) -> Copy:
        """
        Initiate a :sql:`COPY` operation and return a `Copy` object to manage it.
        """
        with self.connection.lock:
            self._start_query()
            self.connection._start_query()
            # Make sure to avoid PQexec to avoid receiving a mix of COPY and
            # other operations.
            self._execute_send(statement, vars, no_pqexec=True)
            gen = execute(self.connection.pgconn)
            results = self.connection.wait(gen)
            self._check_copy_results(results)
            self.pgresult = results[0]  # will set it on the transformer too

        return Copy(connection=self.connection, transformer=self._transformer)


class AsyncCursor(BaseCursor["AsyncConnection"]):
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
                            result, encoding=self.connection.client_encoding
                        )
                else:
                    pgq.dump(vars)

                self._send_query_prepared(b"", pgq)
                gen = execute(self.connection.pgconn)
                (result,) = await self.connection.wait(gen)
                self._execute_results((result,))

        return self

    async def callproc(
        self,
        name: Union[str, sql.Identifier],
        args: Optional[Params] = None,
        kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Params]:
        await self.execute(self._callproc_sql(name, args, kwargs))
        return args

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
        res = []
        async for rec in self:
            res.append(rec)

        return res

    async def __aiter__(self) -> AsyncIterator[Sequence[Any]]:
        self._check_result()

        load = self._transformer.load_row

        while 1:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    async def copy(
        self, statement: Query, vars: Optional[Params] = None
    ) -> AsyncCopy:
        """
        Initiate a :sql:`COPY` operation and return an `AsyncCopy` object.
        """
        async with self.connection.lock:
            self._start_query()
            await self.connection._start_query()
            # Make sure to avoid PQexec to avoid receiving a mix of COPY and
            # other operations.
            self._execute_send(statement, vars, no_pqexec=True)
            gen = execute(self.connection.pgconn)
            results = await self.connection.wait(gen)
            self._check_copy_results(results)
            self.pgresult = results[0]  # will set it on the transformer too

        return AsyncCopy(
            connection=self.connection,
            transformer=self._transformer,
        )


class NamedCursorMixin:
    pass


class NamedCursor(NamedCursorMixin, Cursor):
    pass


class AsyncNamedCursor(NamedCursorMixin, AsyncCursor):
    pass
