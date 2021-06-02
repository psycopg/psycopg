"""
psycopg3 connection objects
"""

# Copyright (C) 2020-2021 The Psycopg Team

import asyncio
import logging
import warnings
import threading
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Generic, Iterator, List
from typing import NamedTuple, Optional, Type, TypeVar, Union
from typing import overload, TYPE_CHECKING
from weakref import ref, ReferenceType
from functools import partial
from contextlib import contextmanager

from . import pq
from . import adapt
from . import errors as e
from . import waiting
from . import encodings
from .pq import ConnStatus, ExecStatus, TransactionStatus, Format
from .sql import Composable
from .rows import Row, RowFactory, tuple_row, TupleRow
from .proto import AdaptContext, ConnectionType, Params, PQGen, PQGenConn
from .proto import Query, RV
from .cursor import Cursor, AsyncCursor
from .conninfo import _conninfo_connect_timeout, ConnectionInfo
from .generators import notifies
from ._preparing import PrepareManager
from .transaction import Transaction, AsyncTransaction
from .utils.compat import asynccontextmanager
from .server_cursor import ServerCursor, AsyncServerCursor

logger = logging.getLogger("psycopg3")

connect: Callable[[str], PQGenConn["PGconn"]]
execute: Callable[["PGconn"], PQGen[List["PGresult"]]]

# Row Type variable for Cursor (when it needs to be distinguished from the
# connection's one)
CursorRow = TypeVar("CursorRow")

if TYPE_CHECKING:
    from .pq.proto import PGconn, PGresult
    from .pool.base import BasePool

if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    connect = _psycopg3.connect
    execute = _psycopg3.execute

else:
    from . import generators

    connect = generators.connect
    execute = generators.execute


class Notify(NamedTuple):
    """An asynchronous notification received from the database."""

    channel: str
    """The name of the channel on which the notification was received."""

    payload: str
    """The message attached to the notification."""

    pid: int
    """The PID of the backend process which sent the notification."""


Notify.__module__ = "psycopg3"

NoticeHandler = Callable[[e.Diagnostic], None]
NotifyHandler = Callable[[Notify], None]


class BaseConnection(AdaptContext, Generic[Row]):
    """
    Base class for different types of connections.

    Share common functionalities such as access to the wrapped PGconn, but
    allow different interfaces (sync/async).
    """

    # DBAPI2 exposed exceptions
    Warning = e.Warning
    Error = e.Error
    InterfaceError = e.InterfaceError
    DatabaseError = e.DatabaseError
    DataError = e.DataError
    OperationalError = e.OperationalError
    IntegrityError = e.IntegrityError
    InternalError = e.InternalError
    ProgrammingError = e.ProgrammingError
    NotSupportedError = e.NotSupportedError

    # Enums useful for the connection
    ConnStatus = pq.ConnStatus
    TransactionStatus = pq.TransactionStatus

    def __init__(self, pgconn: "PGconn", row_factory: RowFactory[Row]):
        self.pgconn = pgconn  # TODO: document this
        self._row_factory = row_factory
        self._autocommit = False
        self._adapters = adapt.AdaptersMap(adapt.global_adapters)
        self._notice_handlers: List[NoticeHandler] = []
        self._notify_handlers: List[NotifyHandler] = []

        # Stack of savepoint names managed by current transaction blocks.
        # the first item is "" in case the outermost Transaction must manage
        # only a begin/commit and not a savepoint.
        self._savepoints: List[str] = []

        self._closed = False  # closed by an explicit close()
        self._prepared: PrepareManager = PrepareManager()

        wself = ref(self)
        pgconn.notice_handler = partial(BaseConnection._notice_handler, wself)
        pgconn.notify_handler = partial(BaseConnection._notify_handler, wself)

        # Attribute is only set if the connection is from a pool so we can tell
        # apart a connection in the pool too (when _pool = None)
        self._pool: Optional["BasePool[Any]"]

        # Time after which the connection should be closed
        self._expire_at: float

    def __del__(self) -> None:
        # If fails on connection we might not have this attribute yet
        if not hasattr(self, "pgconn"):
            return

        # Connection correctly closed
        if self.closed:
            return

        # Connection in a pool so terminating with the program is normal
        if hasattr(self, "_pool"):
            return

        warnings.warn(
            f"connection {self} was deleted while still open."
            f" Please use 'with' or '.close()' to close the connection",
            ResourceWarning,
        )

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self.pgconn)
        return f"<{cls} {info} at 0x{id(self):x}>"

    @property
    def closed(self) -> bool:
        """`!True` if the connection is closed."""
        return self.pgconn.status == ConnStatus.BAD

    @property
    def broken(self) -> bool:
        """
        `!True` if the connection was interrupted.

        A broken connection is always `closed`, but wasn't closed in a clean
        way, such as using `close()` or a ``with`` block.
        """
        return self.pgconn.status == ConnStatus.BAD and not self._closed

    @property
    def autocommit(self) -> bool:
        """The autocommit state of the connection."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._set_autocommit(value)

    def _set_autocommit(self, value: bool) -> None:
        # Base implementation, not thread safe
        # subclasses must call it holding a lock
        status = self.pgconn.transaction_status
        if status != TransactionStatus.IDLE:
            if self._savepoints:
                raise e.ProgrammingError(
                    "couldn't change autocommit state: "
                    "connection.transaction() context in progress"
                )
            else:
                raise e.ProgrammingError(
                    "couldn't change autocommit state: "
                    "connection in transaction status "
                    f"{TransactionStatus(status).name}"
                )

        self._autocommit = value

    @property
    def client_encoding(self) -> str:
        """The Python codec name of the connection's client encoding."""
        pgenc = self.pgconn.parameter_status(b"client_encoding") or b"UTF8"
        return encodings.pg2py(pgenc)

    @client_encoding.setter
    def client_encoding(self, name: str) -> None:
        self._set_client_encoding(name)

    def _set_client_encoding(self, name: str) -> None:
        raise NotImplementedError

    def _set_client_encoding_gen(self, name: str) -> PQGen[None]:
        self.pgconn.send_query_params(
            b"select set_config('client_encoding', $1, false)",
            [encodings.py2pg(name)],
        )
        (result,) = yield from execute(self.pgconn)
        if result.status != ExecStatus.TUPLES_OK:
            raise e.error_from_result(result, encoding=self.client_encoding)

    @property
    def info(self) -> ConnectionInfo:
        """A `ConnectionInfo` attribute to inspect connection properties."""
        return ConnectionInfo(self.pgconn)

    @property
    def adapters(self) -> adapt.AdaptersMap:
        return self._adapters

    @property
    def connection(self) -> "BaseConnection[Row]":
        # implement the AdaptContext protocol
        return self

    @property
    def row_factory(self) -> RowFactory[Row]:
        """Writable attribute to control how result rows are formed."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, row_factory: RowFactory[Row]) -> None:
        self._row_factory = row_factory

    def fileno(self) -> int:
        """Return the file descriptor of the connection.

        This function allows to use the connection as file-like object in
        functions waiting for readiness, such as the ones defined in the
        `selectors` module.
        """
        return self.pgconn.socket

    def cancel(self) -> None:
        """Cancel the current operation on the connection."""
        c = self.pgconn.get_cancel()
        c.cancel()

    def add_notice_handler(self, callback: NoticeHandler) -> None:
        """
        Register a callable to be invoked when a notice message is received.
        """
        self._notice_handlers.append(callback)

    def remove_notice_handler(self, callback: NoticeHandler) -> None:
        """
        Unregister a notice message callable previously registered.
        """
        self._notice_handlers.remove(callback)

    @staticmethod
    def _notice_handler(
        wself: "ReferenceType[BaseConnection[Row]]", res: "PGresult"
    ) -> None:
        self = wself()
        if not (self and self._notice_handler):
            return

        diag = e.Diagnostic(res, self.client_encoding)
        for cb in self._notice_handlers:
            try:
                cb(diag)
            except Exception as ex:
                logger.exception(
                    "error processing notice callback '%s': %s", cb, ex
                )

    def add_notify_handler(self, callback: NotifyHandler) -> None:
        """
        Register a callable to be invoked whenever a notification is received.
        """
        self._notify_handlers.append(callback)

    def remove_notify_handler(self, callback: NotifyHandler) -> None:
        """
        Unregister a notification callable previously registered.
        """
        self._notify_handlers.remove(callback)

    @staticmethod
    def _notify_handler(
        wself: "ReferenceType[BaseConnection[Row]]", pgn: pq.PGnotify
    ) -> None:
        self = wself()
        if not (self and self._notify_handlers):
            return

        enc = self.client_encoding
        n = Notify(pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid)
        for cb in self._notify_handlers:
            cb(n)

    @property
    def prepare_threshold(self) -> Optional[int]:
        """
        Number of times a query is executed before it is prepared.

        - If it is set to 0, every query is prepared the first time is
          executed.
        - If it is set to `!None`, prepared statements are disabled on the
          connection.

        Default value: 5
        """
        return self._prepared.prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: Optional[int]) -> None:
        self._prepared.prepare_threshold = value

    @property
    def prepared_max(self) -> int:
        """
        Maximum number of prepared statements on the connection.

        Default value: 100
        """
        return self._prepared.prepared_max

    @prepared_max.setter
    def prepared_max(self, value: int) -> None:
        self._prepared.prepared_max = value

    # Generators to perform high-level operations on the connection
    #
    # These operations are expressed in terms of non-blocking generators
    # and the task of waiting when needed (when the generators yield) is left
    # to the connections subclass, which might wait either in blocking mode
    # or through asyncio.
    #
    # All these generators assume exclusive acces to the connection: subclasses
    # should have a lock and hold it before calling and consuming them.

    @classmethod
    def _connect_gen(
        cls: Type[ConnectionType],
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: Optional[RowFactory[Any]] = None,
    ) -> PQGenConn[ConnectionType]:
        """Generator to connect to the database and create a new instance."""
        pgconn = yield from connect(conninfo)
        if not row_factory:
            row_factory = tuple_row
        conn = cls(pgconn, row_factory)
        conn._autocommit = autocommit
        return conn

    def _exec_command(self, command: Query) -> PQGen["PGresult"]:
        """
        Generator to send a command and receive the result to the backend.

        Only used to implement internal commands such as "commit", with eventual
        arguments bound client-side. The cursor can do more complex stuff.
        """
        if self.pgconn.status != ConnStatus.OK:
            if self.pgconn.status == ConnStatus.BAD:
                raise e.OperationalError("the connection is closed")
            raise e.InterfaceError(
                f"cannot execute operations: the connection is"
                f" in status {self.pgconn.status}"
            )

        if isinstance(command, str):
            command = command.encode(self.client_encoding)
        elif isinstance(command, Composable):
            command = command.as_bytes(self)

        self.pgconn.send_query(command)
        result = (yield from execute(self.pgconn))[-1]
        if result.status not in (ExecStatus.COMMAND_OK, ExecStatus.TUPLES_OK):
            if result.status == ExecStatus.FATAL_ERROR:
                raise e.error_from_result(
                    result, encoding=self.client_encoding
                )
            else:
                raise e.InterfaceError(
                    f"unexpected result {ExecStatus(result.status).name}"
                    f" from command {command.decode('utf8')!r}"
                )
        return result

    def _start_query(self) -> PQGen[None]:
        """Generator to start a transaction if necessary."""
        if self._autocommit:
            return

        if self.pgconn.transaction_status != TransactionStatus.IDLE:
            return

        yield from self._exec_command(b"begin")

    def _commit_gen(self) -> PQGen[None]:
        """Generator implementing `Connection.commit()`."""
        if self._savepoints:
            raise e.ProgrammingError(
                "Explicit commit() forbidden within a Transaction "
                "context. (Transaction will be automatically committed "
                "on successful exit from context.)"
            )
        if self.pgconn.transaction_status == TransactionStatus.IDLE:
            return

        yield from self._exec_command(b"commit")

    def _rollback_gen(self) -> PQGen[None]:
        """Generator implementing `Connection.rollback()`."""
        if self._savepoints:
            raise e.ProgrammingError(
                "Explicit rollback() forbidden within a Transaction "
                "context. (Either raise Rollback() or allow "
                "an exception to propagate out of the context.)"
            )
        if self.pgconn.transaction_status == TransactionStatus.IDLE:
            return

        yield from self._exec_command(b"rollback")


class Connection(BaseConnection[Row]):
    """
    Wrapper for a connection to the database.
    """

    __module__ = "psycopg3"

    def __init__(self, pgconn: "PGconn", row_factory: RowFactory[Row]):
        super().__init__(pgconn, row_factory)
        self.lock = threading.Lock()

    @overload
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: RowFactory[Row],
        **kwargs: Union[None, int, str],
    ) -> "Connection[Row]":
        ...

    @overload
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        **kwargs: Union[None, int, str],
    ) -> "Connection[TupleRow]":
        ...

    @classmethod  # type: ignore[misc]
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: Optional[RowFactory[Row]] = None,
        **kwargs: Any,
    ) -> "Connection[Any]":
        """
        Connect to a database server and return a new `Connection` instance.
        """
        conninfo, timeout = _conninfo_connect_timeout(conninfo, **kwargs)
        return cls._wait_conn(
            cls._connect_gen(
                conninfo, autocommit=autocommit, row_factory=row_factory
            ),
            timeout,
        )

    def __enter__(self) -> "Connection[Row]":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.closed:
            return

        if exc_type:
            # try to rollback, but if there are problems (connection in a bad
            # state) just warn without clobbering the exception bubbling up.
            try:
                self.rollback()
            except Exception as exc2:
                warnings.warn(
                    f"error rolling back the transaction on {self}: {exc2}",
                    RuntimeWarning,
                )
        else:
            self.commit()

        # Close the connection only if it doesn't belong to a pool.
        if not getattr(self, "_pool", None):
            self.close()

    def close(self) -> None:
        """Close the database connection."""
        if self.closed:
            return
        self._closed = True
        self.pgconn.finish()

    @overload
    def cursor(self, *, binary: bool = False) -> Cursor[Row]:
        ...

    @overload
    def cursor(
        self, *, binary: bool = False, row_factory: RowFactory[CursorRow]
    ) -> Cursor[CursorRow]:
        ...

    @overload
    def cursor(self, name: str, *, binary: bool = False) -> ServerCursor[Row]:
        ...

    @overload
    def cursor(
        self,
        name: str,
        *,
        binary: bool = False,
        row_factory: RowFactory[CursorRow],
    ) -> ServerCursor[CursorRow]:
        ...

    def cursor(
        self,
        name: str = "",
        *,
        binary: bool = False,
        row_factory: Optional[RowFactory[Any]] = None,
    ) -> Union[Cursor[Any], ServerCursor[Any]]:
        """
        Return a new cursor to send commands and queries to the connection.
        """
        format = Format.BINARY if binary else Format.TEXT
        if not row_factory:
            row_factory = self.row_factory
        if name:
            return ServerCursor(
                self, name=name, format=format, row_factory=row_factory
            )
        else:
            return Cursor(self, format=format, row_factory=row_factory)

    def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
    ) -> Cursor[Row]:
        """Execute a query and return a cursor to read its results."""
        cur = self.cursor()
        try:
            return cur.execute(query, params, prepare=prepare)
        except e.Error as ex:
            raise ex.with_traceback(None)

    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        with self.lock:
            self.wait(self._commit_gen())

    def rollback(self) -> None:
        """Roll back to the start of any pending transaction."""
        with self.lock:
            self.wait(self._rollback_gen())

    @contextmanager
    def transaction(
        self,
        savepoint_name: Optional[str] = None,
        force_rollback: bool = False,
    ) -> Iterator[Transaction]:
        """
        Start a context block with a new transaction or nested transaction.

        :param savepoint_name: Name of the savepoint used to manage a nested
            transaction. If `!None`, one will be chosen automatically.
        :param force_rollback: Roll back the transaction at the end of the
            block even if there were no error (e.g. to try a no-op process).
        :rtype: Transaction
        """
        with Transaction(self, savepoint_name, force_rollback) as tx:
            yield tx

    def notifies(self) -> Iterator[Notify]:
        """
        Yield `Notify` objects as soon as they are received from the database.
        """
        while 1:
            with self.lock:
                ns = self.wait(notifies(self.pgconn))
            enc = self.client_encoding
            for pgn in ns:
                n = Notify(
                    pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid
                )
                yield n

    def wait(self, gen: PQGen[RV], timeout: Optional[float] = 0.1) -> RV:
        """
        Consume a generator operating on the connection.

        The function must be used on generators that don't change connection
        fd (i.e. not on connect and reset).
        """
        return waiting.wait(gen, self.pgconn.socket, timeout=timeout)

    @classmethod
    def _wait_conn(cls, gen: PQGenConn[RV], timeout: Optional[int]) -> RV:
        """Consume a connection generator."""
        return waiting.wait_conn(gen, timeout=timeout)

    def _set_autocommit(self, value: bool) -> None:
        with self.lock:
            super()._set_autocommit(value)

    def _set_client_encoding(self, name: str) -> None:
        with self.lock:
            self.wait(self._set_client_encoding_gen(name))


class AsyncConnection(BaseConnection[Row]):
    """
    Asynchronous wrapper for a connection to the database.
    """

    __module__ = "psycopg3"

    def __init__(self, pgconn: "PGconn", row_factory: RowFactory[Row]):
        super().__init__(pgconn, row_factory)
        self.lock = asyncio.Lock()

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: RowFactory[Row],
        **kwargs: Union[None, int, str],
    ) -> "AsyncConnection[Row]":
        ...

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        **kwargs: Union[None, int, str],
    ) -> "AsyncConnection[TupleRow]":
        ...

    @classmethod  # type: ignore[misc]
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: Optional[RowFactory[Row]] = None,
        **kwargs: Any,
    ) -> "AsyncConnection[Any]":
        conninfo, timeout = _conninfo_connect_timeout(conninfo, **kwargs)
        return await cls._wait_conn(
            cls._connect_gen(
                conninfo, autocommit=autocommit, row_factory=row_factory
            ),
            timeout,
        )

    async def __aenter__(self) -> "AsyncConnection[Row]":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.closed:
            return

        if exc_type:
            # try to rollback, but if there are problems (connection in a bad
            # state) just warn without clobbering the exception bubbling up.
            try:
                await self.rollback()
            except Exception as exc2:
                warnings.warn(
                    f"error rolling back the transaction on {self}: {exc2}",
                    RuntimeWarning,
                )
        else:
            await self.commit()

        # Close the connection only if it doesn't belong to a pool.
        if not getattr(self, "_pool", None):
            await self.close()

    async def close(self) -> None:
        if self.closed:
            return
        self._closed = True
        self.pgconn.finish()

    @overload
    def cursor(self, *, binary: bool = False) -> AsyncCursor[Row]:
        ...

    @overload
    def cursor(
        self, *, binary: bool = False, row_factory: RowFactory[CursorRow]
    ) -> AsyncCursor[CursorRow]:
        ...

    @overload
    def cursor(
        self, name: str, *, binary: bool = False
    ) -> AsyncServerCursor[Row]:
        ...

    @overload
    def cursor(
        self,
        name: str,
        *,
        binary: bool = False,
        row_factory: RowFactory[CursorRow],
    ) -> AsyncServerCursor[CursorRow]:
        ...

    def cursor(
        self,
        name: str = "",
        *,
        binary: bool = False,
        row_factory: Optional[RowFactory[Any]] = None,
    ) -> Union[AsyncCursor[Any], AsyncServerCursor[Any]]:
        """
        Return a new `AsyncCursor` to send commands and queries to the connection.
        """
        format = Format.BINARY if binary else Format.TEXT
        if not row_factory:
            row_factory = self.row_factory
        if name:
            return AsyncServerCursor(
                self, name=name, format=format, row_factory=row_factory
            )
        else:
            return AsyncCursor(self, format=format, row_factory=row_factory)

    async def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
    ) -> AsyncCursor[Row]:
        cur = self.cursor()
        try:
            return await cur.execute(query, params, prepare=prepare)
        except e.Error as ex:
            raise ex.with_traceback(None)

    async def commit(self) -> None:
        async with self.lock:
            await self.wait(self._commit_gen())

    async def rollback(self) -> None:
        async with self.lock:
            await self.wait(self._rollback_gen())

    @asynccontextmanager
    async def transaction(
        self,
        savepoint_name: Optional[str] = None,
        force_rollback: bool = False,
    ) -> AsyncIterator[AsyncTransaction]:
        """
        Start a context block with a new transaction or nested transaction.

        :rtype: AsyncTransaction
        """
        tx = AsyncTransaction(self, savepoint_name, force_rollback)
        async with tx:
            yield tx

    async def notifies(self) -> AsyncIterator[Notify]:
        while 1:
            async with self.lock:
                ns = await self.wait(notifies(self.pgconn))
            enc = self.client_encoding
            for pgn in ns:
                n = Notify(
                    pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid
                )
                yield n

    async def wait(self, gen: PQGen[RV]) -> RV:
        return await waiting.wait_async(gen, self.pgconn.socket)

    @classmethod
    async def _wait_conn(
        cls, gen: PQGenConn[RV], timeout: Optional[int]
    ) -> RV:
        return await waiting.wait_conn_async(gen, timeout)

    def _set_client_encoding(self, name: str) -> None:
        raise AttributeError(
            "'client_encoding' is read-only on async connections:"
            " please use await .set_client_encoding() instead."
        )

    async def set_client_encoding(self, name: str) -> None:
        """Async version of the `~Connection.client_encoding` setter."""
        async with self.lock:
            await self.wait(self._set_client_encoding_gen(name))

    def _set_autocommit(self, value: bool) -> None:
        raise AttributeError(
            "autocommit is read-only on async connections:"
            " please use await connection.set_autocommit() instead."
            " Note that you can pass an 'autocommit' value to 'connect()'"
            " if it doesn't need to change during the connection's lifetime."
        )

    async def set_autocommit(self, value: bool) -> None:
        """Async version of the `~Connection.autocommit` setter."""
        async with self.lock:
            super()._set_autocommit(value)
