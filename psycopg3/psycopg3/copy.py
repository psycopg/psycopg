"""
psycopg3 copy support
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from typing import TYPE_CHECKING, AsyncIterator, Iterator, Generic
from typing import Any, Dict, List, Match, Optional, Sequence, Type, Union
from types import TracebackType

from .pq import Format
from .proto import ConnectionType, Transformer
from .generators import copy_from, copy_to, copy_end

if TYPE_CHECKING:
    from .pq.proto import PGresult
    from .connection import Connection, AsyncConnection  # noqa: F401


class BaseCopy(Generic[ConnectionType]):
    def __init__(self, connection: ConnectionType, transformer: Transformer):
        self.connection = connection
        self.transformer = transformer

        self.format = self.pgresult.binary_tuples
        self._first_row = True
        self._finished = False
        self._encoding: str = ""

        if self.format == Format.TEXT:
            self._format_row = self._format_row_text
        else:
            self._format_row = self._format_row_binary

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def pgresult(self) -> "PGresult":
        pgresult = self.transformer.pgresult
        assert pgresult, "The Transformer doesn't have a PGresult set"
        return pgresult

    def _ensure_bytes(self, data: Union[bytes, str]) -> bytes:
        if isinstance(data, bytes):
            return data

        elif isinstance(data, str):
            if self._encoding:
                return data.encode(self._encoding)

            if (
                self.pgresult is None
                or self.pgresult.binary_tuples == Format.BINARY
            ):
                raise TypeError(
                    "cannot copy str data in binary mode: use bytes instead"
                )
            self._encoding = self.connection.client_encoding
            return data.encode(self._encoding)

        else:
            raise TypeError(f"can't write {type(data).__name__}")

    def format_row(self, row: Sequence[Any]) -> bytes:
        out: List[Optional[bytes]] = []
        for item in row:
            if item is not None:
                dumper = self.transformer.get_dumper(item, self.format)
                out.append(dumper.dump(item))
            else:
                out.append(None)
        return self._format_row(out)

    def _format_row_text(self, row: Sequence[Optional[bytes]]) -> bytes:
        return (
            b"\t".join(
                _bsrepl_re.sub(_bsrepl_sub, item)
                if item is not None
                else br"\N"
                for item in row
            )
            + b"\n"
        )

    def _format_row_binary(
        self,
        row: Sequence[Optional[bytes]],
        __int2_struct: struct.Struct = struct.Struct("!h"),
        __int4_struct: struct.Struct = struct.Struct("!i"),
    ) -> bytes:
        out = []
        if self._first_row:
            out.append(
                # Signature, flags, extra length
                b"PGCOPY\n\xff\r\n\0"
                b"\x00\x00\x00\x00"
                b"\x00\x00\x00\x00"
            )
            self._first_row = False

        out.append(__int2_struct.pack(len(row)))
        for item in row:
            if item is not None:
                out.append(__int4_struct.pack(len(item)))
                out.append(item)
            else:
                out.append(b"\xff\xff\xff\xff")

        return b"".join(out)


def _bsrepl_sub(
    m: Match[bytes],
    __map: Dict[bytes, bytes] = {
        b"\b": b"\\b",
        b"\t": b"\\t",
        b"\n": b"\\n",
        b"\v": b"\\v",
        b"\f": b"\\f",
        b"\r": b"\\r",
        b"\\": b"\\\\",
    },
) -> bytes:
    return __map[m.group(0)]


_bsrepl_re = re.compile(b"[\b\t\n\v\f\r\\\\]")


class Copy(BaseCopy["Connection"]):
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

    def write_row(self, row: Sequence[Any]) -> None:
        data = self.format_row(row)
        self.write(data)

    def finish(self, error: str = "") -> None:
        conn = self.connection
        berr = error.encode(conn.client_encoding, "replace") if error else None
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
            if self.format == Format.BINARY and not self._first_row:
                # send EOF only if we copied binary rows (_first_row is False)
                self.write(b"\xff\xff")
            self.finish()
        else:
            self.finish(str(exc_val) or type(exc_val).__qualname__)

    def __iter__(self) -> Iterator[bytes]:
        while 1:
            data = self.read()
            if data is None:
                break
            yield data


class AsyncCopy(BaseCopy["AsyncConnection"]):
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

    async def write_row(self, row: Sequence[Any]) -> None:
        data = self.format_row(row)
        await self.write(data)

    async def finish(self, error: str = "") -> None:
        conn = self.connection
        berr = error.encode(conn.client_encoding, "replace") if error else None
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
            if self.format == Format.BINARY and not self._first_row:
                # send EOF only if we copied binary rows (_first_row is False)
                await self.write(b"\xff\xff")
            await self.finish()
        else:
            await self.finish(str(exc_val))

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while 1:
            data = await self.read()
            if data is None:
                break
            yield data
