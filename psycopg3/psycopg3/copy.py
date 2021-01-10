"""
psycopg3 copy support
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from types import TracebackType
from typing import TYPE_CHECKING, AsyncIterator, Iterator, Generic, Union
from typing import Any, Dict, List, Match, Optional, Sequence, Type, Tuple

from . import pq
from . import errors as e
from .pq import Format, ExecStatus
from .proto import ConnectionType, PQGen, Transformer
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
        self._pgconn = self.connection.pgconn

        assert (
            self.transformer.pgresult
        ), "The Transformer doesn't have a PGresult set"
        self._pgresult: "PGresult" = self.transformer.pgresult

        self.format = Format(self._pgresult.binary_tuples)
        self._encoding = self.connection.client_encoding
        self._signature_sent = False
        self._row_mode = False  # true if the user is using send_row()
        self._write_buffer = bytearray()
        self._write_buffer_size = 32 * 1024
        self._finished = False

        if self.format == Format.TEXT:
            self._format_row = format_row_text
            self._parse_row = parse_row_text
        else:
            self._format_row = format_row_binary
            self._parse_row = parse_row_binary

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self._pgconn)
        return f"<{cls} {info} at 0x{id(self):x}>"

    def set_types(self, types: Sequence[int]) -> None:
        """
        Set the types expected out of a :sql:`COPY TO` operation.

        Without setting the types, the data from :sql:`COPY TO` will be
        returned as unparsed strings or bytes.
        """
        self.transformer.set_row_types(types, [self.format] * len(types))

    # High level copy protocol generators (state change of the Copy object)

    def _read_gen(self) -> PQGen[memoryview]:
        if self._finished:
            return memoryview(b"")

        res = yield from copy_from(self._pgconn)
        if isinstance(res, memoryview):
            return res

        # res is the final PGresult
        self._finished = True
        nrows = res.command_tuples
        self.cursor._rowcount = nrows if nrows is not None else -1
        return memoryview(b"")

    def _read_row_gen(self) -> PQGen[Optional[Tuple[Any, ...]]]:
        data = yield from self._read_gen()
        if not data:
            return None
        if self.format == Format.BINARY:
            if not self._signature_sent:
                if data[: len(_binary_signature)] != _binary_signature:
                    raise e.DataError(
                        "binary copy doesn't start with the expected signature"
                    )
                self._signature_sent = True
                data = data[len(_binary_signature) :]
            elif data == _binary_trailer:
                return None
        return self._parse_row(data, self.transformer)

    def _write_gen(self, buffer: Union[str, bytes]) -> PQGen[None]:
        # if write() was called, assume the header was sent together with the
        # first block of data.
        self._signature_sent = True
        yield from copy_to(self._pgconn, self._ensure_bytes(buffer))

    def _write_row_gen(self, row: Sequence[Any]) -> PQGen[None]:
        # Note down that we are writing in row mode: it means we will have
        # to take care of the end-of-copy marker too
        self._row_mode = True

        if self.format == Format.BINARY and not self._signature_sent:
            self._write_buffer += _binary_signature
            self._signature_sent = True

        self._format_row(row, self.transformer, self._write_buffer)
        if len(self._write_buffer) > self._write_buffer_size:
            yield from copy_to(self._pgconn, self._write_buffer)
            self._write_buffer.clear()

    def _finish_gen(self, error: str = "") -> PQGen[None]:
        if error:
            berr = error.encode(self.connection.client_encoding, "replace")
            res = yield from copy_end(self._pgconn, berr)
        else:
            if self._write_buffer:
                yield from copy_to(self._pgconn, self._write_buffer)
                self._write_buffer.clear()
            res = yield from copy_end(self._pgconn, None)

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

        # In case of error in Python let's quit it here
        if exc_type:
            yield from self._finish_gen(
                f"error from Python: {exc_type.__qualname__} - {exc_val}"
            )
            return

        if self.format == Format.BINARY:
            # If we have sent no data we need to send the signature
            # and the trailer
            if not self._signature_sent:
                self._write_buffer += _binary_signature
                self._write_buffer += _binary_trailer
            elif self._row_mode:
                # if we have sent data already, we have sent the signature too
                # (either with the first row, or we assume that in block mode
                # the signature is included).
                # Write the trailer only if we are sending rows (with the
                # assumption that who is copying binary data is sending the
                # whole format).
                self._write_buffer += _binary_trailer

        yield from self._finish_gen()

    # Support methods

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


class Copy(BaseCopy["Connection"]):
    """Manage a :sql:`COPY` operation."""

    __module__ = "psycopg3"

    def read(self) -> memoryview:
        """
        Read an unparsed row after a :sql:`COPY TO` operation.

        Return an empty string when the data is finished.
        """
        return self.connection.wait(self._read_gen())

    def rows(self) -> Iterator[Tuple[Any, ...]]:
        """
        Iterate on the result of a :sql:`COPY TO` operation record by record.

        Note that the records returned will be tuples of of unparsed strings or
        bytes, unless data types are specified using `set_types()`.
        """
        while True:
            record = self.read_row()
            if record is None:
                break
            yield record

    def read_row(self) -> Optional[Tuple[Any, ...]]:
        """
        Read a parsed row of data from a table after a :sql:`COPY TO` operation.

        Return `!None` when the data is finished.

        Note that the records returned will be tuples of unparsed strings or
        bytes, unless data types are specified using `set_types()`.
        """
        return self.connection.wait(self._read_row_gen())

    def write(self, buffer: Union[str, bytes]) -> None:
        """
        Write a block of data to a table after a :sql:`COPY FROM` operation.

        If the :sql:`COPY` is in binary format *buffer* must be `!bytes`. In
        text mode it can be either `!bytes` or `!str`.
        """
        self.connection.wait(self._write_gen(buffer))

    def write_row(self, row: Sequence[Any]) -> None:
        """Write a record to a table after a :sql:`COPY FROM` operation."""
        self.connection.wait(self._write_row_gen(row))

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

    def __iter__(self) -> Iterator[memoryview]:
        while True:
            data = self.read()
            if not data:
                break
            yield data


class AsyncCopy(BaseCopy["AsyncConnection"]):
    """Manage an asynchronous :sql:`COPY` operation."""

    __module__ = "psycopg3"

    async def read(self) -> memoryview:
        return await self.connection.wait(self._read_gen())

    async def rows(self) -> AsyncIterator[Tuple[Any, ...]]:
        while True:
            record = await self.read_row()
            if record is None:
                break
            yield record

    async def read_row(self) -> Optional[Tuple[Any, ...]]:
        return await self.connection.wait(self._read_row_gen())

    async def write(self, buffer: Union[str, bytes]) -> None:
        await self.connection.wait(self._write_gen(buffer))

    async def write_row(self, row: Sequence[Any]) -> None:
        await self.connection.wait(self._write_row_gen(row))

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

    async def __aiter__(self) -> AsyncIterator[memoryview]:
        while True:
            data = await self.read()
            if not data:
                break
            yield data


def _format_row_text(
    row: Sequence[Any], tx: Transformer, out: Optional[bytearray] = None
) -> bytearray:
    """Convert a row of objects to the data to send for copy."""
    if out is None:
        out = bytearray()

    if not row:
        out += b"\n"
        return out

    for item in row:
        if item is not None:
            dumper = tx.get_dumper(item, Format.TEXT)
            b = dumper.dump(item)
            out += _dump_re.sub(_dump_sub, b)
        else:
            out += br"\N"
        out += b"\t"

    out[-1:] = b"\n"
    return out


def _format_row_binary(
    row: Sequence[Any], tx: Transformer, out: Optional[bytearray] = None
) -> bytearray:
    """Convert a row of objects to the data to send for binary copy."""
    if out is None:
        out = bytearray()

    out += _pack_int2(len(row))
    for item in row:
        if item is not None:
            dumper = tx.get_dumper(item, Format.BINARY)
            b = dumper.dump(item)
            out += _pack_int4(len(b))
            out += b
        else:
            out += _binary_null

    return out


def _parse_row_text(data: bytes, tx: Transformer) -> Tuple[Any, ...]:
    if not isinstance(data, bytes):
        data = bytes(data)
    fields = data.split(b"\t")
    fields[-1] = fields[-1][:-1]  # drop \n
    row = [None if f == b"\\N" else _load_re.sub(_load_sub, f) for f in fields]
    return tx.load_sequence(row)


def _parse_row_binary(data: bytes, tx: Transformer) -> Tuple[Any, ...]:
    row: List[Optional[bytes]] = []
    nfields = _unpack_int2(data, 0)[0]
    pos = 2
    for i in range(nfields):
        length = _unpack_int4(data, pos)[0]
        pos += 4
        if length >= 0:
            row.append(data[pos : pos + length])
            pos += length
        else:
            row.append(None)

    return tx.load_sequence(row)


_pack_int2 = struct.Struct("!h").pack
_pack_int4 = struct.Struct("!i").pack
_unpack_int2 = struct.Struct("!h").unpack_from
_unpack_int4 = struct.Struct("!i").unpack_from

_binary_signature = (
    # Signature, flags, extra length
    b"PGCOPY\n\xff\r\n\0"
    b"\x00\x00\x00\x00"
    b"\x00\x00\x00\x00"
)
_binary_trailer = b"\xff\xff"
_binary_null = b"\xff\xff\xff\xff"

_dump_re = re.compile(b"[\b\t\n\v\f\r\\\\]")
_dump_repl = {
    b"\b": b"\\b",
    b"\t": b"\\t",
    b"\n": b"\\n",
    b"\v": b"\\v",
    b"\f": b"\\f",
    b"\r": b"\\r",
    b"\\": b"\\\\",
}


def _dump_sub(
    m: Match[bytes], __map: Dict[bytes, bytes] = _dump_repl
) -> bytes:
    return __map[m.group(0)]


_load_re = re.compile(b"\\\\[btnvfr\\\\]")
_load_repl = {v: k for k, v in _dump_repl.items()}


def _load_sub(
    m: Match[bytes], __map: Dict[bytes, bytes] = _load_repl
) -> bytes:
    return __map[m.group(0)]


# Override functions with fast versions if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    format_row_text = _psycopg3.format_row_text
    format_row_binary = _psycopg3.format_row_binary
    parse_row_text = _psycopg3.parse_row_text
    parse_row_binary = _psycopg3.parse_row_binary

else:
    format_row_text = _format_row_text
    format_row_binary = _format_row_binary
    parse_row_text = _parse_row_text
    parse_row_binary = _parse_row_binary
