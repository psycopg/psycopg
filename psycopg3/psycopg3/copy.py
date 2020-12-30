"""
psycopg3 copy support
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from typing import TYPE_CHECKING, AsyncIterator, Iterator, Generic
from typing import Any, Dict, List, Match, Optional, Sequence, Type, Union
from types import TracebackType

from . import pq
from .pq import Format, ExecStatus
from .proto import ConnectionType, PQGen
from .generators import copy_from, copy_to, copy_end

if TYPE_CHECKING:
    from .pq.proto import PGresult
    from .cursor import BaseCursor  # noqa: F401
    from .connection import Connection, AsyncConnection  # noqa: F401


class BaseCopy(Generic[ConnectionType]):
    def __init__(self, cursor: "BaseCursor[ConnectionType]"):
        self.cursor = cursor
        self.connection = cursor.connection
        self.transformer = cursor._transformer

        assert (
            self.transformer.pgresult
        ), "The Transformer doesn't have a PGresult set"
        self._pgresult: "PGresult" = self.transformer.pgresult

        self.format = Format(self._pgresult.binary_tuples)
        self._encoding = self.connection.client_encoding
        self._first_row = True
        self._finished = False

        if self.format == Format.TEXT:
            self._format_copy_row = self._format_row_text
        else:
            self._format_copy_row = self._format_row_binary

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self.connection.pgconn)
        return f"<{cls} {info} at 0x{id(self):x}>"

    # High level copy protocol generators (state change of the Copy object)

    def _read_gen(self) -> PQGen[bytes]:
        if self._finished:
            return b""

        conn = self.connection
        res = yield from copy_from(conn.pgconn)
        if isinstance(res, bytes):
            return res

        # res is the final PGresult
        self._finished = True
        nrows = res.command_tuples
        self.cursor._rowcount = nrows if nrows is not None else -1
        return b""

    def _write_gen(self, buffer: Union[str, bytes]) -> PQGen[None]:
        conn = self.connection
        yield from copy_to(conn.pgconn, self._ensure_bytes(buffer))

    def _finish_gen(self, error: str = "") -> PQGen[None]:
        conn = self.connection
        berr = error.encode(conn.client_encoding, "replace") if error else None
        res = yield from copy_end(conn.pgconn, berr)
        nrows = res.command_tuples
        self.cursor._rowcount = nrows if nrows is not None else -1
        self._finished = True

    def _exit_gen(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
    ) -> PQGen[None]:
        # no-op in COPY TO
        if self._pgresult.status == ExecStatus.COPY_OUT:
            return

        if not exc_type:
            if self.format == Format.BINARY and not self._first_row:
                # send EOF only if we copied binary rows (_first_row is False)
                yield from self._write_gen(b"\xff\xff")
            yield from self._finish_gen()
        else:
            yield from self._finish_gen(
                f"error from Python: {exc_type.__qualname__} - {exc_val}"
            )

    # Support methods

    def _format_row(self, row: Sequence[Any]) -> bytes:
        """Convert a Python sequence to the data to send for copy"""
        out: List[Optional[bytes]] = []
        for item in row:
            if item is not None:
                dumper = self.transformer.get_dumper(item, self.format)
                out.append(dumper.dump(item))
            else:
                out.append(None)
        return self._format_copy_row(out)

    def _format_row_text(self, row: Sequence[Optional[bytes]]) -> bytes:
        """Convert a row of adapted data to the data to send for text copy"""
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
        """Convert a row of adapted data to the data to send for binary copy"""
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

    def _ensure_bytes(self, data: Union[bytes, str]) -> bytes:
        if isinstance(data, bytes):
            return data

        elif isinstance(data, str):
            if self._pgresult.binary_tuples == Format.BINARY:
                raise TypeError(
                    "cannot copy str data in binary mode: use bytes instead"
                )
            return data.encode(self._encoding)

        else:
            raise TypeError(f"can't write {type(data).__name__}")

    def _check_reuse(self) -> None:
        if self._finished:
            raise TypeError("copy blocks can be used only once")


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
    """Manage a :sql:`COPY` operation."""

    __module__ = "psycopg3"

    def read(self) -> bytes:
        """Read a row of data after a :sql:`COPY TO` operation.

        Return an empty bytes string when the data is finished.
        """
        return self.connection.wait(self._read_gen())

    def write(self, buffer: Union[str, bytes]) -> None:
        """Write a block of data after a :sql:`COPY FROM` operation.

        If the COPY is in binary format *buffer* must be `!bytes`. In text mode
        it can be either `!bytes` or `!str`.
        """
        self.connection.wait(self._write_gen(buffer))

    def write_row(self, row: Sequence[Any]) -> None:
        """Write a record after a :sql:`COPY FROM` operation."""
        data = self._format_row(row)
        self.connection.wait(self._write_gen(data))

    def _finish(self, error: str = "") -> None:
        """Terminate a :sql:`COPY FROM` operation."""
        self.connection.wait(self._finish_gen(error))

    def __enter__(self) -> "Copy":
        self._check_reuse()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.connection.wait(self._exit_gen(exc_type, exc_val))

    def __iter__(self) -> Iterator[bytes]:
        while True:
            data = self.read()
            if not data:
                break
            yield data


class AsyncCopy(BaseCopy["AsyncConnection"]):
    """Manage an asynchronous :sql:`COPY` operation."""

    __module__ = "psycopg3"

    async def read(self) -> bytes:
        return await self.connection.wait(self._read_gen())

    async def write(self, buffer: Union[str, bytes]) -> None:
        await self.connection.wait(self._write_gen(buffer))

    async def write_row(self, row: Sequence[Any]) -> None:
        data = self._format_row(row)
        await self.connection.wait(self._write_gen(data))

    async def _finish(self, error: str = "") -> None:
        await self.connection.wait(self._finish_gen(error))

    async def __aenter__(self) -> "AsyncCopy":
        self._check_reuse()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.connection.wait(self._exit_gen(exc_type, exc_val))

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            data = await self.read()
            if not data:
                break
            yield data
