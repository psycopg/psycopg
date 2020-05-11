"""
psycopg3 connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import logging
import asyncio
import threading
from typing import Any, Optional, Type
from typing import cast, TYPE_CHECKING

from . import pq
from . import errors as e
from . import cursor
from . import generators
from . import proto
from .conninfo import make_conninfo
from .waiting import wait, wait_async

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .generators import PQGen, RV


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
        self.dumpers: proto.DumpersMap = {}
        self.loaders: proto.LoadersMap = {}
        # name of the postgres encoding (in bytes)
        self._pgenc = b""

    @property
    def closed(self) -> bool:
        return self.status == self.ConnStatus.BAD

    @property
    def status(self) -> pq.ConnStatus:
        return self.pgconn.status

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

    def encode(self, s: str) -> bytes:
        return self.codec.encode(s)[0]

    def decode(self, b: bytes) -> str:
        return self.codec.decode(b)[0]

    @property
    def encoding(self) -> str:
        rv = self.pgconn.parameter_status(b"client_encoding")
        if rv is not None:
            return rv.decode("ascii")
        else:
            return "UTF8"


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
        cls, conninfo: Optional[str] = None, **kwargs: Any,
    ) -> "Connection":
        if conninfo is None and not kwargs:
            raise TypeError("missing conninfo and not parameters specified")
        conninfo = make_conninfo(conninfo or "", **kwargs)
        gen = generators.connect(conninfo)
        pgconn = cls.wait(gen)
        return cls(pgconn)

    def close(self) -> None:
        self.pgconn.finish()

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.Cursor:
        cur = super().cursor(name, binary)
        return cast(cursor.Cursor, cur)

    def commit(self) -> None:
        self._exec_commit_rollback(b"commit")

    def rollback(self) -> None:
        self._exec_commit_rollback(b"rollback")

    def _exec_commit_rollback(self, command: bytes) -> None:
        with self.lock:
            status = self.pgconn.transaction_status
            if status == pq.TransactionStatus.IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = self.wait(generators.execute(self.pgconn))
            if pgres.status != pq.ExecStatus.COMMAND_OK:
                raise e.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )

    @classmethod
    def wait(cls, gen: "PQGen[RV]", timeout: Optional[float] = 0.1) -> "RV":
        return wait(gen, timeout=timeout)

    def set_client_encoding(self, value: str) -> None:
        with self.lock:
            self.pgconn.send_query_params(
                b"select set_config('client_encoding', $1, false)",
                [value.encode("ascii")],
            )
            gen = generators.execute(self.pgconn)
            (result,) = self.wait(gen)
            if result.status != pq.ExecStatus.TUPLES_OK:
                raise e.error_from_result(result)


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
        cls, conninfo: Optional[str] = None, **kwargs: Any
    ) -> "AsyncConnection":
        if conninfo is None and not kwargs:
            raise TypeError("missing conninfo and not parameters specified")
        conninfo = make_conninfo(conninfo or "", **kwargs)
        gen = generators.connect(conninfo)
        pgconn = await cls.wait(gen)
        return cls(pgconn)

    async def close(self) -> None:
        self.pgconn.finish()

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.AsyncCursor:
        cur = super().cursor(name, binary)
        return cast(cursor.AsyncCursor, cur)

    async def commit(self) -> None:
        await self._exec_commit_rollback(b"commit")

    async def rollback(self) -> None:
        await self._exec_commit_rollback(b"rollback")

    async def _exec_commit_rollback(self, command: bytes) -> None:
        async with self.lock:
            status = self.pgconn.transaction_status
            if status == pq.TransactionStatus.IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = await self.wait(generators.execute(self.pgconn))
            if pgres.status != pq.ExecStatus.COMMAND_OK:
                raise e.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )

    @classmethod
    async def wait(cls, gen: "PQGen[RV]") -> "RV":
        return await wait_async(gen)

    async def set_client_encoding(self, value: str) -> None:
        async with self.lock:
            self.pgconn.send_query_params(
                b"select set_config('client_encoding', $1, false)",
                [value.encode("ascii")],
            )
            gen = generators.execute(self.pgconn)
            (result,) = await self.wait(gen)
            if result.status != pq.ExecStatus.TUPLES_OK:
                raise e.error_from_result(result)
