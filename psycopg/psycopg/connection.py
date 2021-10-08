"""
psycopg connection objects
"""

# Copyright (C) 2020-2021 The Psycopg Team

import logging
import warnings
import threading
from types import TracebackType
from typing import Any, Callable, cast, Dict, Generic, Iterator, List
from typing import NamedTuple, Optional, Type, TypeVar, Union
from typing import overload, TYPE_CHECKING
from weakref import ref, ReferenceType
from functools import partial
from contextlib import contextmanager

from . import pq
from . import errors as e
from . import waiting
from . import postgres
from .pq import ConnStatus, ExecStatus, TransactionStatus, Format
from .abc import AdaptContext, ConnectionType, Params, Query, RV
from .abc import PQGen, PQGenConn
from .sql import Composable
from .rows import Row, RowFactory, tuple_row, TupleRow
from .adapt import AdaptersMap
from ._enums import IsolationLevel
from .cursor import Cursor
from ._cmodule import _psycopg
from .conninfo import make_conninfo, conninfo_to_dict, ConnectionInfo
from .generators import notifies
from ._encodings import pgconn_encoding
from ._preparing import PrepareManager
from .transaction import Transaction
from .server_cursor import ServerCursor

if TYPE_CHECKING:
    from .pq.abc import PGconn, PGresult
    from psycopg_pool.base import BasePool

logger = logging.getLogger("psycopg")

connect: Callable[[str], PQGenConn["PGconn"]]
execute: Callable[["PGconn"], PQGen[List["PGresult"]]]

# Row Type variable for Cursor (when it needs to be distinguished from the
# connection's one)
CursorRow = TypeVar("CursorRow")

if _psycopg:
    connect = _psycopg.connect
    execute = _psycopg.execute

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


Notify.__module__ = "psycopg"

NoticeHandler = Callable[[e.Diagnostic], None]
NotifyHandler = Callable[[Notify], None]


class BaseConnection(Generic[Row]):
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

    def __init__(self, pgconn: "PGconn"):
        self.pgconn = pgconn
        self._autocommit = False
        self._adapters = AdaptersMap(postgres.adapters)
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

        self._isolation_level: Optional[IsolationLevel] = None
        self._read_only: Optional[bool] = None
        self._deferrable: Optional[bool] = None
        self._begin_statement = b""

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
        # Base implementation, not thread safe.
        # Subclasses must call it holding a lock
        self._check_intrans("autocommit")
        self._autocommit = bool(value)

    @property
    def isolation_level(self) -> Optional[IsolationLevel]:
        """
        The isolation level of the new transactions started on the connection.
        """
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, value: Optional[IsolationLevel]) -> None:
        self._set_isolation_level(value)

    def _set_isolation_level(self, value: Optional[IsolationLevel]) -> None:
        # Base implementation, not thread safe.
        # Subclasses must call it holding a lock
        self._check_intrans("isolation_level")
        self._isolation_level = (
            IsolationLevel(value) if value is not None else None
        )
        self._begin_statement = b""

    @property
    def read_only(self) -> Optional[bool]:
        """
        The read-only state of the new transactions started on the connection.
        """
        return self._read_only

    @read_only.setter
    def read_only(self, value: Optional[bool]) -> None:
        self._set_read_only(value)

    def _set_read_only(self, value: Optional[bool]) -> None:
        # Base implementation, not thread safe.
        # Subclasses must call it holding a lock
        self._check_intrans("read_only")
        self._read_only = bool(value)
        self._begin_statement = b""

    @property
    def deferrable(self) -> Optional[bool]:
        """
        The deferrable state of the new transactions started on the connection.
        """
        return self._deferrable

    @deferrable.setter
    def deferrable(self, value: Optional[bool]) -> None:
        self._set_deferrable(value)

    def _set_deferrable(self, value: Optional[bool]) -> None:
        # Base implementation, not thread safe.
        # Subclasses must call it holding a lock
        self._check_intrans("deferrable")
        self._deferrable = bool(value)
        self._begin_statement = b""

    def _check_intrans(self, attribute: str) -> None:
        # Raise an exception if we are in a transaction
        status = self.pgconn.transaction_status
        if status != TransactionStatus.IDLE:
            if self._savepoints:
                raise e.ProgrammingError(
                    f"can't change {attribute!r} now: "
                    "connection.transaction() context in progress"
                )
            else:
                raise e.ProgrammingError(
                    f"can't change {attribute!r} now: "
                    "connection in transaction status "
                    f"{TransactionStatus(status).name}"
                )

    @property
    def info(self) -> ConnectionInfo:
        """A `ConnectionInfo` attribute to inspect connection properties."""
        return ConnectionInfo(self.pgconn)

    @property
    def adapters(self) -> AdaptersMap:
        return self._adapters

    @property
    def connection(self) -> "BaseConnection[Row]":
        # implement the AdaptContext protocol
        return self

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

        diag = e.Diagnostic(res, pgconn_encoding(self.pgconn))
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

        enc = pgconn_encoding(self.pgconn)
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
    ) -> PQGenConn[ConnectionType]:
        """Generator to connect to the database and create a new instance."""
        pgconn = yield from connect(conninfo)
        conn = cls(pgconn)
        conn._autocommit = bool(autocommit)
        return conn

    def _exec_command(
        self, command: Query, result_format: Format = Format.TEXT
    ) -> PQGen["PGresult"]:
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
            command = command.encode(pgconn_encoding(self.pgconn))
        elif isinstance(command, Composable):
            command = command.as_bytes(self)

        if result_format == Format.TEXT:
            self.pgconn.send_query(command)
        else:
            self.pgconn.send_query_params(
                command, None, result_format=result_format
            )

        result = (yield from execute(self.pgconn))[-1]
        if result.status not in (ExecStatus.COMMAND_OK, ExecStatus.TUPLES_OK):
            if result.status == ExecStatus.FATAL_ERROR:
                raise e.error_from_result(
                    result, encoding=pgconn_encoding(self.pgconn)
                )
            else:
                raise e.InterfaceError(
                    f"unexpected result {ExecStatus(result.status).name}"
                    f" from command {command.decode()!r}"
                )
        return result

    def _start_query(self) -> PQGen[None]:
        """Generator to start a transaction if necessary."""
        if self._autocommit:
            return

        if self.pgconn.transaction_status != TransactionStatus.IDLE:
            return

        yield from self._exec_command(self._get_tx_start_command())

    def _get_tx_start_command(self) -> bytes:
        if self._begin_statement:
            return self._begin_statement

        parts = [b"BEGIN"]

        if self.isolation_level is not None:
            val = IsolationLevel(self.isolation_level)
            parts.append(b"ISOLATION LEVEL")
            parts.append(val.name.replace("_", " ").encode())

        if self.read_only is not None:
            parts.append(b"READ ONLY" if self.read_only else b"READ WRITE")

        if self.deferrable is not None:
            parts.append(
                b"DEFERRABLE" if self.deferrable else b"NOT DEFERRABLE"
            )

        self._begin_statement = b" ".join(parts)
        return self._begin_statement

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

        yield from self._exec_command(b"COMMIT")

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

        yield from self._exec_command(b"ROLLBACK")
        cmd = self._prepared.clear()
        if cmd:
            yield from self._exec_command(cmd)


class Connection(BaseConnection[Row]):
    """
    Wrapper for a connection to the database.
    """

    __module__ = "psycopg"

    cursor_factory: Type[Cursor[Row]]
    server_cursor_factory: Type[ServerCursor[Row]]
    row_factory: RowFactory[Row]

    def __init__(
        self, pgconn: "PGconn", row_factory: Optional[RowFactory[Row]] = None
    ):
        super().__init__(pgconn)
        self.row_factory = row_factory or cast(RowFactory[Row], tuple_row)
        self.lock = threading.Lock()
        self.cursor_factory = Cursor
        self.server_cursor_factory = ServerCursor

    @overload
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: RowFactory[Row],
        context: Optional[AdaptContext] = None,
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
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "Connection[TupleRow]":
        ...

    @classmethod  # type: ignore[misc] # https://github.com/python/mypy/issues/11004
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: Optional[RowFactory[Row]] = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Any,
    ) -> "Connection[Any]":
        """
        Connect to a database server and return a new `Connection` instance.
        """
        params = cls._get_connection_params(conninfo, **kwargs)
        conninfo = make_conninfo(**params)

        try:
            rv = cls._wait_conn(
                cls._connect_gen(conninfo, autocommit=autocommit),
                timeout=params["connect_timeout"],
            )
        except e.Error as ex:
            raise ex.with_traceback(None)

        if row_factory:
            rv.row_factory = row_factory
        if context:
            rv._adapters = AdaptersMap(context.adapters)
        return rv

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
                logger.warning(
                    "error ignored rolling back transaction on %s: %s",
                    self,
                    exc2,
                )
        else:
            self.commit()

        # Close the connection only if it doesn't belong to a pool.
        if not getattr(self, "_pool", None):
            self.close()

    @classmethod
    def _get_connection_params(
        cls, conninfo: str, **kwargs: Any
    ) -> Dict[str, Any]:
        """Manipulate connection parameters before connecting.

        :param conninfo: Connection string as received by `~Connection.connect()`.
        :param kwargs: Overriding connection arguments as received by `!connect()`.
        :return: Connection arguments merged and eventually modified, in a
            format similar to `~conninfo.conninfo_to_dict()`.
        """
        params = conninfo_to_dict(conninfo, **kwargs)

        # Make sure there is an usable connect_timeout
        if "connect_timeout" in params:
            params["connect_timeout"] = int(params["connect_timeout"])
        else:
            params["connect_timeout"] = None

        return params

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
    def cursor(
        self,
        name: str,
        *,
        binary: bool = False,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> ServerCursor[Row]:
        ...

    @overload
    def cursor(
        self,
        name: str,
        *,
        binary: bool = False,
        row_factory: RowFactory[CursorRow],
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> ServerCursor[CursorRow]:
        ...

    def cursor(
        self,
        name: str = "",
        *,
        binary: bool = False,
        row_factory: Optional[RowFactory[Any]] = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> Union[Cursor[Any], ServerCursor[Any]]:
        """
        Return a new cursor to send commands and queries to the connection.
        """
        if not row_factory:
            row_factory = self.row_factory

        cur: Union[Cursor[Any], ServerCursor[Any]]
        if name:
            cur = self.server_cursor_factory(
                self,
                name=name,
                row_factory=row_factory,
                scrollable=scrollable,
                withhold=withhold,
            )
        else:
            cur = self.cursor_factory(self, row_factory=row_factory)

        if binary:
            cur.format = Format.BINARY

        return cur

    def execute(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
        binary: bool = False,
    ) -> Cursor[Row]:
        """Execute a query and return a cursor to read its results."""
        cur = self.cursor()
        if binary:
            cur.format = Format.BINARY

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
            enc = pgconn_encoding(self.pgconn)
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

    def _set_isolation_level(self, value: Optional[IsolationLevel]) -> None:
        with self.lock:
            super()._set_isolation_level(value)

    def _set_read_only(self, value: Optional[bool]) -> None:
        with self.lock:
            super()._set_read_only(value)

    def _set_deferrable(self, value: Optional[bool]) -> None:
        with self.lock:
            super()._set_deferrable(value)
