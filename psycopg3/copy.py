"""
psycopg3 copy support
"""

# Copyright (C) 2020 The Psycopg Team

import re
import codecs
from typing import TYPE_CHECKING, AsyncGenerator, Generator
from typing import Dict, Match, Optional, Type, Union
from types import TracebackType

from . import pq
from .proto import AdaptContext
from .generators import copy_from, copy_to, copy_end

if TYPE_CHECKING:
    from .connection import BaseConnection, Connection, AsyncConnection


class BaseCopy:
    def __init__(
        self,
        context: AdaptContext,
        result: Optional[pq.proto.PGresult],
        format: pq.Format = pq.Format.TEXT,
    ):
        from .adapt import Transformer

        self._connection: Optional["BaseConnection"] = None
        self._transformer = Transformer(context)
        self.format = format
        self.pgresult = result
        self._finished = False
        self._codec: Optional[codecs.CodecInfo] = None

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def connection(self) -> "BaseConnection":
        if self._connection is not None:
            return self._connection

        self._connection = conn = self._transformer.connection
        if conn is not None:
            return conn

        raise ValueError("no connection available")

    @property
    def pgresult(self) -> Optional[pq.proto.PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[pq.proto.PGresult]) -> None:
        self._pgresult = result
        self._transformer.pgresult = result

    def _ensure_bytes(self, data: Union[bytes, str]) -> bytes:
        if isinstance(data, bytes):
            return data

        elif isinstance(data, str):
            if self._codec is not None:
                return self._codec.encode(data)[0]

            if (
                self.pgresult is None
                or self.pgresult.binary_tuples == pq.Format.BINARY
            ):
                raise TypeError(
                    "cannot copy str data in binary mode: use bytes instead"
                )
            self._codec = self.connection.codec
            return self._codec.encode(data)[0]


def _bsrepl_sub(
    m: Match[bytes],
    __map: Dict[bytes, bytes] = {
        b"b": b"\b",
        b"t": b"\t",
        b"n": b"\n",
        b"v": b"\v",
        b"f": b"\f",
        b"r": b"\r",
    },
) -> bytes:
    g = m.group(0)
    return __map.get(g, g)


_bsrepl_re = re.compile(rb"\\(.)")


class Copy(BaseCopy):
    _connection: Optional["Connection"]

    @property
    def connection(self) -> "Connection":
        # TODO: mypy error: "Callable[[BaseCopy], BaseConnection]" has no
        # attribute "fget"
        return BaseCopy.connection.fget(self)  # type: ignore

    def read(self) -> Optional[bytes]:
        if self._finished:
            return None

        conn = self.connection
        rv = conn.wait(copy_from(conn.pgconn))
        if rv is None:
            self._finished = True

        return rv

    def write(self, buffer: Union[str, bytes]) -> None:
        conn = self.connection
        conn.wait(copy_to(conn.pgconn, self._ensure_bytes(buffer)))

    def finish(self, error: Optional[str] = None) -> None:
        conn = self.connection
        berr = (
            conn.codec.encode(error, "replace")[0]
            if error is not None
            else None
        )
        conn.wait(copy_end(conn.pgconn, berr))
        self._finished = True

    def __enter__(self) -> "Copy":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_val is None:
            self.finish()
        else:
            self.finish(str(exc_val))

    def __iter__(self) -> Generator[bytes, None, None]:
        while 1:
            data = self.read()
            if data is None:
                break
            yield data


class AsyncCopy(BaseCopy):
    _connection: Optional["AsyncConnection"]

    @property
    def connection(self) -> "AsyncConnection":
        return BaseCopy.connection.fget(self)  # type: ignore

    async def read(self) -> Optional[bytes]:
        if self._finished:
            return None

        conn = self.connection
        rv = await conn.wait(copy_from(conn.pgconn))
        if rv is None:
            self._finished = True

        return rv

    async def write(self, buffer: Union[str, bytes]) -> None:
        conn = self.connection
        await conn.wait(copy_to(conn.pgconn, self._ensure_bytes(buffer)))

    async def finish(self, error: Optional[str] = None) -> None:
        conn = self.connection
        berr = (
            conn.codec.encode(error, "replace")[0]
            if error is not None
            else None
        )
        await conn.wait(copy_end(conn.pgconn, berr))
        self._finished = True

    async def __aenter__(self) -> "AsyncCopy":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_val is None:
            await self.finish()
        else:
            await self.finish(str(exc_val))

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        while 1:
            data = await self.read()
            if data is None:
                break
            yield data
