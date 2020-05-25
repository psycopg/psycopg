"""
psycopg3 connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import logging
import asyncio
import threading
from typing import Any, AsyncGenerator, Callable, Generator, List, NamedTuple
from typing import Optional, Type, cast
from weakref import ref, ReferenceType
from functools import partial

from . import pq
from . import errors as e
from . import cursor
from . import proto
from .pq import TransactionStatus, ExecStatus
from .conninfo import make_conninfo
from .waiting import wait, wait_async
from .generators import notifies

logger = logging.getLogger(__name__)
package_logger = logging.getLogger("psycopg3")

connect: Callable[[str], proto.PQGen[pq.proto.PGconn]]
execute: Callable[[pq.proto.PGconn], proto.PQGen[List[pq.proto.PGresult]]]

if pq.__impl__ == "c":
    from . import _psycopg3

    connect = _psycopg3.connect
    execute = _psycopg3.execute

else:
    from . import generators

    connect = generators.connect
    execute = generators.execute


class Notify(NamedTuple):
    channel: str
    payload: str
    pid: int


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

    def __init__(self, pgconn: pq.proto.PGconn):
        self.pgconn = pgconn
        self.cursor_factory = cursor.BaseCursor
        self._autocommit = False
        self.dumpers: proto.DumpersMap = {}
        self.loaders: proto.LoadersMap = {}
        self._notice_handlers: List[NoticeHandler] = []
        self._notify_handlers: List[NotifyHandler] = []
        # name of the postgres encoding (in bytes)
        self._pgenc = b""

        wself = ref(self)

        pgconn.notice_handler = partial(BaseConnection._notice_handler, wself)
        pgconn.notify_handler = partial(BaseConnection._notify_handler, wself)

    @property
    def closed(self) -> bool:
        return self.status == self.ConnStatus.BAD

    @property
    def status(self) -> pq.ConnStatus:
        return self.pgconn.status

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        status = self.pgconn.transaction_status
        if status != TransactionStatus.IDLE:
            raise e.ProgrammingError(
                "can't change autocommit state: connection in"
                f" transaction status {TransactionStatus(status).name}"
            )
        self._autocommit = value

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.BaseCursor:
        if name is not None:
            raise NotImplementedError
        return self.cursor_factory(self, binary=binary)

    @property
    def codec(self) -> codecs.CodecInfo:
        # TODO: utf8 fastpath?
        pgenc = self.pgconn.parameter_status(b"client_encoding") or b""
        if self._pgenc != pgenc:
            if pgenc:
                try:
                    pyenc = pq.py_codecs[pgenc.decode("ascii")]
                except KeyError:
                    raise e.NotSupportedError(
                        f"encoding {pgenc.decode('ascii')} not available in Python"
                    )
                self._codec = codecs.lookup(pyenc)
            else:
                # fallback for a connection closed whose codec was never asked
                if not hasattr(self, "_codec"):
                    self._codec = codecs.lookup("utf8")

            self._pgenc = pgenc
        return self._codec

    @property
    def encoding(self) -> str:
        rv = self.pgconn.parameter_status(b"client_encoding")
        if rv is not None:
            return rv.decode("ascii")
        else:
            return "UTF8"

    def add_notice_handler(self, callback: NoticeHandler) -> None:
        self._notice_handlers.append(callback)

    def remove_notice_handler(self, callback: NoticeHandler) -> None:
        self._notice_handlers.remove(callback)

    @staticmethod
    def _notice_handler(
        wself: "ReferenceType[BaseConnection]", res: pq.proto.PGresult
    ) -> None:
        self = wself()
        if self is None or not self._notice_handler:
            return

        diag = e.Diagnostic(res, self.codec.name)
        for cb in self._notice_handlers:
            try:
                cb(diag)
            except Exception as ex:
                package_logger.exception(
                    "error processing notice callback '%s': %s", cb, ex
                )

    def add_notify_handler(self, callback: NotifyHandler) -> None:
        self._notify_handlers.append(callback)

    def remove_notify_handler(self, callback: NotifyHandler) -> None:
        self._notify_handlers.remove(callback)

    @staticmethod
    def _notify_handler(
        wself: "ReferenceType[BaseConnection]", pgn: pq.PGnotify
    ) -> None:
        self = wself()
        if self is None or not self._notify_handlers:
            return

        decode = self.codec.decode
        n = Notify(decode(pgn.relname)[0], decode(pgn.extra)[0], pgn.be_pid)
        for cb in self._notify_handlers:
            cb(n)


class Connection(BaseConnection):
    """
    Wrap a connection to the database.

    This class implements a DBAPI-compliant interface.
    """

    cursor_factory: Type[cursor.Cursor]

    def __init__(self, pgconn: pq.proto.PGconn):
        super().__init__(pgconn)
        self.lock = threading.Lock()
        self.cursor_factory = cursor.Cursor

    @classmethod
    def connect(
        cls,
        conninfo: Optional[str] = None,
        *,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> "Connection":
        if conninfo is None and not kwargs:
            raise TypeError("missing conninfo and not parameters specified")
        conninfo = make_conninfo(conninfo or "", **kwargs)
        gen = connect(conninfo)
        pgconn = cls.wait(gen)
        conn = cls(pgconn)
        conn._autocommit = autocommit
        return conn

    def close(self) -> None:
        self.pgconn.finish()

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.Cursor:
        cur = super().cursor(name, binary)
        return cast(cursor.Cursor, cur)

    def _start_query(self) -> None:
        # the function is meant to be called by a cursor once the lock is taken
        if self._autocommit:
            return

        if self.pgconn.transaction_status == TransactionStatus.INTRANS:
            return

        self.pgconn.send_query(b"begin")
        (pgres,) = self.wait(execute(self.pgconn))
        if pgres.status != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                f"error on begin: {pq.error_message(pgres)}"
            )

    def commit(self) -> None:
        self._exec_commit_rollback(b"commit")

    def rollback(self) -> None:
        self._exec_commit_rollback(b"rollback")

    def _exec_commit_rollback(self, command: bytes) -> None:
        with self.lock:
            status = self.pgconn.transaction_status
            if status == TransactionStatus.IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = self.wait(execute(self.pgconn))
            if pgres.status != ExecStatus.COMMAND_OK:
                raise e.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )

    @classmethod
    def wait(
        cls, gen: proto.PQGen[proto.RV], timeout: Optional[float] = 0.1
    ) -> proto.RV:
        return wait(gen, timeout=timeout)

    def set_client_encoding(self, value: str) -> None:
        with self.lock:
            self.pgconn.send_query_params(
                b"select set_config('client_encoding', $1, false)",
                [value.encode("ascii")],
            )
            gen = execute(self.pgconn)
            (result,) = self.wait(gen)
            if result.status != ExecStatus.TUPLES_OK:
                raise e.error_from_result(result)

    def notifies(self) -> Generator[Optional[Notify], bool, None]:
        decode = self.codec.decode
        while 1:
            with self.lock:
                ns = self.wait(notifies(self.pgconn))
            for pgn in ns:
                n = Notify(
                    decode(pgn.relname)[0], decode(pgn.extra)[0], pgn.be_pid
                )
                if (yield n):
                    yield None  # for the send who stopped us
                    return


class AsyncConnection(BaseConnection):
    """
    Wrap an asynchronous connection to the database.

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines.
    """

    cursor_factory: Type[cursor.AsyncCursor]

    def __init__(self, pgconn: pq.proto.PGconn):
        super().__init__(pgconn)
        self.lock = asyncio.Lock()
        self.cursor_factory = cursor.AsyncCursor

    @classmethod
    async def connect(
        cls,
        conninfo: Optional[str] = None,
        *,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> "AsyncConnection":
        if conninfo is None and not kwargs:
            raise TypeError("missing conninfo and not parameters specified")
        conninfo = make_conninfo(conninfo or "", **kwargs)
        gen = connect(conninfo)
        pgconn = await cls.wait(gen)
        conn = cls(pgconn)
        conn._autocommit = autocommit
        return conn

    async def close(self) -> None:
        self.pgconn.finish()

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.AsyncCursor:
        cur = super().cursor(name, binary)
        return cast(cursor.AsyncCursor, cur)

    async def _start_query(self) -> None:
        # the function is meant to be called by a cursor once the lock is taken
        if self._autocommit:
            return

        if self.pgconn.transaction_status == TransactionStatus.INTRANS:
            return

        self.pgconn.send_query(b"begin")
        (pgres,) = await self.wait(execute(self.pgconn))
        if pgres.status != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                f"error on begin: {pq.error_message(pgres)}"
            )

    async def commit(self) -> None:
        await self._exec_commit_rollback(b"commit")

    async def rollback(self) -> None:
        await self._exec_commit_rollback(b"rollback")

    async def _exec_commit_rollback(self, command: bytes) -> None:
        async with self.lock:
            status = self.pgconn.transaction_status
            if status == TransactionStatus.IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = await self.wait(execute(self.pgconn))
            if pgres.status != ExecStatus.COMMAND_OK:
                raise e.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )

    @classmethod
    async def wait(cls, gen: proto.PQGen[proto.RV]) -> proto.RV:
        return await wait_async(gen)

    async def set_client_encoding(self, value: str) -> None:
        async with self.lock:
            self.pgconn.send_query_params(
                b"select set_config('client_encoding', $1, false)",
                [value.encode("ascii")],
            )
            gen = execute(self.pgconn)
            (result,) = await self.wait(gen)
            if result.status != ExecStatus.TUPLES_OK:
                raise e.error_from_result(result)

    async def notifies(self) -> AsyncGenerator[Optional[Notify], bool]:
        decode = self.codec.decode
        while 1:
            async with self.lock:
                ns = await self.wait(notifies(self.pgconn))
            for pgn in ns:
                n = Notify(
                    decode(pgn.relname)[0], decode(pgn.extra)[0], pgn.be_pid
                )
                if (yield n):
                    yield None
                    return
