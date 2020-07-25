"""
psycopg3 connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import logging
import asyncio
import threading
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Iterator, List, NamedTuple
from typing import Optional, Type, TYPE_CHECKING, Union
from weakref import ref, ReferenceType
from functools import partial

from . import pq
from . import cursor
from . import errors as e
from . import encodings
from .pq import TransactionStatus, ExecStatus
from .proto import DumpersMap, LoadersMap, PQGen, RV
from .waiting import wait, wait_async
from .conninfo import make_conninfo
from .generators import notifies
from .transaction import Transaction

logger = logging.getLogger(__name__)
package_logger = logging.getLogger("psycopg3")

connect: Callable[[str], PQGen["PGconn"]]
execute: Callable[["PGconn"], PQGen[List["PGresult"]]]

if TYPE_CHECKING:
    from .pq.proto import PGconn, PGresult
    from .cursor import Cursor, AsyncCursor

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


NoticeHandler = Callable[[e.Diagnostic], None]
NotifyHandler = Callable[[Notify], None]


class BaseConnection:
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

    cursor_factory: Union[Type["Cursor"], Type["AsyncCursor"]]

    def __init__(self, pgconn: "PGconn"):
        self.pgconn = pgconn  # TODO: document this
        self._autocommit = False
        self.dumpers: DumpersMap = {}
        self.loaders: LoadersMap = {}
        self._notice_handlers: List[NoticeHandler] = []
        self._notify_handlers: List[NotifyHandler] = []

        # stack of savepoint names managed by active Transaction() blocks
        self._savepoints: Optional[List[bytes]] = None
        # (None when there no active Transaction blocks; [] when there is only
        # one Transaction block, with a top-level transaction and no savepoint)

        wself = ref(self)

        pgconn.notice_handler = partial(BaseConnection._notice_handler, wself)
        pgconn.notify_handler = partial(BaseConnection._notify_handler, wself)

    @property
    def closed(self) -> bool:
        """`True` if the connection is closed."""
        return self.pgconn.status == self.ConnStatus.BAD

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
            if self._savepoints is not None:
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
        wself: "ReferenceType[BaseConnection]", res: "PGresult"
    ) -> None:
        self = wself()
        if not (self and self._notice_handler):
            return

        diag = e.Diagnostic(res, self.client_encoding)
        for cb in self._notice_handlers:
            try:
                cb(diag)
            except Exception as ex:
                package_logger.exception(
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
        wself: "ReferenceType[BaseConnection]", pgn: pq.PGnotify
    ) -> None:
        self = wself()
        if not (self and self._notify_handlers):
            return

        enc = self.client_encoding
        n = Notify(pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid)
        for cb in self._notify_handlers:
            cb(n)


class Connection(BaseConnection):
    """
    Wrapper for a connection to the database.
    """

    cursor_factory: Type[cursor.Cursor]

    def __init__(self, pgconn: "PGconn"):
        super().__init__(pgconn)
        self.lock = threading.Lock()
        self.cursor_factory = cursor.Cursor

    @classmethod
    def connect(
        cls, conninfo: str = "", *, autocommit: bool = False, **kwargs: Any
    ) -> "Connection":
        """
        Connect to a database server and return a new `Connection` instance.

        TODO: connection_timeout to be implemented.
        """

        conninfo = make_conninfo(conninfo, **kwargs)
        gen = connect(conninfo)
        pgconn = cls.wait(gen)
        conn = cls(pgconn)
        conn._autocommit = autocommit
        return conn

    def __enter__(self) -> "Connection":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_type:
            self.rollback()
        else:
            self.commit()

        self.close()

    def close(self) -> None:
        """Close the database connection."""
        self.pgconn.finish()

    def cursor(
        self, name: str = "", format: pq.Format = pq.Format.TEXT
    ) -> cursor.Cursor:
        """
        Return a new `Cursor` to send commands and queries to the connection.
        """
        if name:
            raise NotImplementedError

        return self.cursor_factory(self, format=format)

    def _start_query(self) -> None:
        # the function is meant to be called by a cursor once the lock is taken
        if self._autocommit:
            return

        if self.pgconn.transaction_status != TransactionStatus.IDLE:
            return

        self._exec_command(b"begin")

    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        with self.lock:
            if self._savepoints is not None:
                raise e.ProgrammingError(
                    "Explicit commit() forbidden within a Transaction "
                    "context. (Transaction will be automatically committed "
                    "on successful exit from context.)"
                )
            if self.pgconn.transaction_status == TransactionStatus.IDLE:
                return
            self._exec_command(b"commit")

    def rollback(self) -> None:
        """Roll back to the start of any pending transaction."""
        with self.lock:
            if self._savepoints is not None:
                raise e.ProgrammingError(
                    "Explicit rollback() forbidden within a Transaction "
                    "context. (Either raise Transaction.Rollback() or allow "
                    "an exception to propagate out of the context.)"
                )
            if self.pgconn.transaction_status == TransactionStatus.IDLE:
                return
            self._exec_command(b"rollback")

    def _exec_command(self, command: bytes) -> None:
        # Caller must hold self.lock
        logger.debug(f"{self}: {command!r}")
        self.pgconn.send_query(command)
        results = self.wait(execute(self.pgconn))
        if results[-1].status != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                f"error on {command.decode('utf8')}:"
                f" {pq.error_message(results[-1], encoding=self.client_encoding)}"
            )

    def transaction(
        self,
        savepoint_name: Optional[str] = None,
        force_rollback: bool = False,
    ) -> Transaction:
        return Transaction(self, savepoint_name, force_rollback)

    @classmethod
    def wait(cls, gen: PQGen[RV], timeout: Optional[float] = 0.1) -> RV:
        return wait(gen, timeout=timeout)

    def _set_client_encoding(self, name: str) -> None:
        with self.lock:
            self.pgconn.send_query_params(
                b"select set_config('client_encoding', $1, false)",
                [encodings.py2pg(name)],
            )
            gen = execute(self.pgconn)
            (result,) = self.wait(gen)
            if result.status != ExecStatus.TUPLES_OK:
                raise e.error_from_result(
                    result, encoding=self.client_encoding
                )

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

    def _set_autocommit(self, value: bool) -> None:
        with self.lock:
            super()._set_autocommit(value)


class AsyncConnection(BaseConnection):
    """
    Asynchronous wrapper for a connection to the database.
    """

    cursor_factory: Type[cursor.AsyncCursor]

    def __init__(self, pgconn: "PGconn"):
        super().__init__(pgconn)
        self.lock = asyncio.Lock()
        self.cursor_factory = cursor.AsyncCursor

    @classmethod
    async def connect(
        cls, conninfo: str = "", *, autocommit: bool = False, **kwargs: Any
    ) -> "AsyncConnection":
        conninfo = make_conninfo(conninfo, **kwargs)
        gen = connect(conninfo)
        pgconn = await cls.wait(gen)
        conn = cls(pgconn)
        conn._autocommit = autocommit
        return conn

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_type:
            await self.rollback()
        else:
            await self.commit()

        await self.close()

    async def close(self) -> None:
        self.pgconn.finish()

    async def cursor(
        self, name: str = "", format: pq.Format = pq.Format.TEXT
    ) -> cursor.AsyncCursor:
        """
        Return a new `AsyncCursor` to send commands and queries to the connection.
        """
        if name:
            raise NotImplementedError

        return self.cursor_factory(self, format=format)

    async def _start_query(self) -> None:
        # the function is meant to be called by a cursor once the lock is taken
        if self._autocommit:
            return

        if self.pgconn.transaction_status != TransactionStatus.IDLE:
            return

        self.pgconn.send_query(b"begin")
        (pgres,) = await self.wait(execute(self.pgconn))
        if pgres.status != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                "error on begin:"
                f" {pq.error_message(pgres, encoding=self.client_encoding)}"
            )

    async def commit(self) -> None:
        async with self.lock:
            if self.pgconn.transaction_status == TransactionStatus.IDLE:
                return
            await self._exec(b"commit")

    async def rollback(self) -> None:
        async with self.lock:
            if self.pgconn.transaction_status == TransactionStatus.IDLE:
                return
            await self._exec(b"rollback")

    async def _exec(self, command: bytes) -> None:
        # Caller must hold self.lock
        self.pgconn.send_query(command)
        (pgres,) = await self.wait(execute(self.pgconn))
        if pgres.status != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                f"error on {command.decode('utf8')}:"
                f" {pq.error_message(pgres, encoding=self.client_encoding)}"
            )

    @classmethod
    async def wait(cls, gen: PQGen[RV]) -> RV:
        return await wait_async(gen)

    def _set_client_encoding(self, name: str) -> None:
        raise AttributeError(
            "'client_encoding' is read-only on async connections:"
            " please use await .set_client_encoding() instead."
        )

    async def set_client_encoding(self, name: str) -> None:
        """Async version of the `client_encoding` setter."""
        async with self.lock:
            self.pgconn.send_query_params(
                b"select set_config('client_encoding', $1, false)",
                [name.encode("utf-8")],
            )
            gen = execute(self.pgconn)
            (result,) = await self.wait(gen)
            if result.status != ExecStatus.TUPLES_OK:
                raise e.error_from_result(
                    result, encoding=self.client_encoding
                )

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

    def _set_autocommit(self, value: bool) -> None:
        raise AttributeError(
            "autocommit is read-only on async connections:"
            " please use await connection.set_autocommit() instead."
            " Note that you can pass an 'autocommit' value to 'connect()'"
            " if it doesn't need to change during the connection's lifetime."
        )

    async def set_autocommit(self, value: bool) -> None:
        """Async version of the `autocommit` setter."""
        async with self.lock:
            super()._set_autocommit(value)
