"""
psycopg copy support
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import queue
import struct
import asyncio
import threading
from abc import ABC, abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, AsyncIterator, Iterator, Generic, Union
from typing import Any, Dict, List, Match, Optional, Sequence, Type, Tuple

from . import pq
from . import errors as e
from .pq import ExecStatus
from .abc import ConnectionType, PQGen, Transformer
from .adapt import PyFormat
from ._compat import create_task
from ._cmodule import _psycopg
from ._encodings import pgconn_encoding
from .generators import copy_from, copy_to, copy_end

if TYPE_CHECKING:
    from .pq.abc import PGresult
    from .cursor import BaseCursor, Cursor
    from .cursor_async import AsyncCursor
    from .connection import Connection  # noqa: F401
    from .connection_async import AsyncConnection  # noqa: F401

TEXT = pq.Format.TEXT
BINARY = pq.Format.BINARY


class BaseCopy(Generic[ConnectionType]):
    """
    Base implementation for copy user interface

    Two subclasses expose real methods with the sync/async differences.

    The difference between the text and binary format is managed by two
    different `Formatter` subclasses.

    While the interface doesn't dictate it, both subclasses are implemented
    with a worker to perform I/O related work, consuming the data provided in
    the correct format from a queue, while the main thread is concerned with
    formatting the data in copy format and adding it to the queue.
    """

    # Max size of the write queue of buffers. More than that copy will block
    # Each buffer around Formatter.BUFFER_SIZE size
    QUEUE_SIZE = 1024

    formatter: "Formatter"

    def __init__(self, cursor: "BaseCursor[ConnectionType, Any]"):
        self.cursor = cursor
        self.connection = cursor.connection
        self._pgconn = self.connection.pgconn

        tx = cursor._tx
        assert tx.pgresult, "The Transformer doesn't have a PGresult set"
        self._pgresult: "PGresult" = tx.pgresult

        if self._pgresult.binary_tuples == pq.Format.TEXT:
            self.formatter = TextFormatter(
                tx, encoding=pgconn_encoding(self._pgconn)
            )
        else:
            self.formatter = BinaryFormatter(tx)

        self._finished = False

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self._pgconn)
        return f"<{cls} {info} at 0x{id(self):x}>"

    def _enter(self) -> None:
        if self._finished:
            raise TypeError("copy blocks can be used only once")

    def set_types(self, types: Sequence[Union[int, str]]) -> None:
        """
        Set the types expected in a COPY operation.

        The types must be specified as a sequence of oid or PostgreSQL type
        names (e.g. ``int4``, ``timestamptz[]``).

        This operation overcomes the lack of metadata returned by PostgreSQL
        when a COPY operation begins:

        - On :sql:`COPY TO`, `!set_types()` allows to specify what types the
          operation returns. If `!set_types()` is not used, the data will be
          reurned as unparsed strings or bytes instead of Python objects.

        - On :sql:`COPY FROM`, `!set_types()` allows to choose what type the
          database expects. This is especially useful in binary copy, because
          PostgreSQL will apply no cast rule.

        """
        registry = self.cursor.adapters.types
        oids = [
            t if isinstance(t, int) else registry.get_oid(t) for t in types
        ]

        if self._pgresult.status == ExecStatus.COPY_IN:
            self.formatter.transformer.set_dumper_types(
                oids, self.formatter.format
            )
        else:
            self.formatter.transformer.set_loader_types(
                oids, self.formatter.format
            )

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

        row = self.formatter.parse_row(data)
        if row is None:
            # Get the final result to finish the copy operation
            yield from self._read_gen()
            self._finished = True
            return None

        return row

    def _end_copy_gen(self, exc: Optional[BaseException]) -> PQGen[None]:
        bmsg: Optional[bytes]
        if exc:
            msg = f"error from Python: {type(exc).__qualname__} - {exc}"
            bmsg = msg.encode(pgconn_encoding(self._pgconn), "replace")
        else:
            bmsg = None

        res = yield from copy_end(self._pgconn, bmsg)

        nrows = res.command_tuples
        self.cursor._rowcount = nrows if nrows is not None else -1
        self._finished = True


class Copy(BaseCopy["Connection[Any]"]):
    """Manage a :sql:`COPY` operation."""

    __module__ = "psycopg"

    def __init__(self, cursor: "Cursor[Any]"):
        super().__init__(cursor)
        self._queue: queue.Queue[Optional[bytes]] = queue.Queue(
            maxsize=self.QUEUE_SIZE
        )
        self._worker: Optional[threading.Thread] = None

    def __enter__(self) -> "Copy":
        self._enter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.finish(exc_val)

    # End user sync interface

    def __iter__(self) -> Iterator[memoryview]:
        """Implement block-by-block iteration on :sql:`COPY TO`."""
        while True:
            data = self.read()
            if not data:
                break
            yield data

    def read(self) -> memoryview:
        """
        Read an unparsed row after a :sql:`COPY TO` operation.

        Return an empty string when the data is finished.
        """
        return self.connection.wait(self._read_gen())

    def rows(self) -> Iterator[Tuple[Any, ...]]:
        """
        Iterate on the result of a :sql:`COPY TO` operation record by record.

        Note that the records returned will be tuples of unparsed strings or
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
        data = self.formatter.write(buffer)
        self._write(data)

    def write_row(self, row: Sequence[Any]) -> None:
        """Write a record to a table after a :sql:`COPY FROM` operation."""
        data = self.formatter.write_row(row)
        self._write(data)

    def finish(self, exc: Optional[BaseException]) -> None:
        """Terminate the copy operation and free the resources allocated.

        You shouldn't need to call this function yourself: it is usually called
        by exit. It is available if, despite what is documented, you end up
        using the `Copy` object outside a block.
        """
        # no-op in COPY TO
        if self._pgresult.status == ExecStatus.COPY_OUT:
            return

        self._write_end()
        self.connection.wait(self._end_copy_gen(exc))

    # Concurrent copy support

    def worker(self) -> None:
        """Push data to the server when available from the copy queue.

        Terminate reading when the queue receives a None.

        The function is designed to be run in a separate thread.
        """
        while 1:
            data = self._queue.get(block=True, timeout=24 * 60 * 60)
            if not data:
                break
            self.connection.wait(copy_to(self._pgconn, data))

    def _write(self, data: bytes) -> None:
        if not data:
            return

        if not self._worker:
            # warning: reference loop, broken by _write_end
            self._worker = threading.Thread(target=self.worker)
            self._worker.daemon = True
            self._worker.start()

        self._queue.put(data)

    def _write_end(self) -> None:
        data = self.formatter.end()
        self._write(data)
        self._queue.put(None)

        if self._worker:
            self._worker.join()
            self._worker = None  # break the loop


class AsyncCopy(BaseCopy["AsyncConnection[Any]"]):
    """Manage an asynchronous :sql:`COPY` operation."""

    __module__ = "psycopg"

    def __init__(self, cursor: "AsyncCursor[Any]"):
        super().__init__(cursor)
        self._queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(
            maxsize=self.QUEUE_SIZE
        )
        self._worker: Optional[asyncio.Future[None]] = None

    async def __aenter__(self) -> "AsyncCopy":
        self._enter()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.finish(exc_val)

    async def __aiter__(self) -> AsyncIterator[memoryview]:
        while True:
            data = await self.read()
            if not data:
                break
            yield data

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
        data = self.formatter.write(buffer)
        await self._write(data)

    async def write_row(self, row: Sequence[Any]) -> None:
        data = self.formatter.write_row(row)
        await self._write(data)

    async def finish(self, exc: Optional[BaseException]) -> None:
        # no-op in COPY TO
        if self._pgresult.status == ExecStatus.COPY_OUT:
            return

        await self._write_end()
        await self.connection.wait(self._end_copy_gen(exc))

    # Concurrent copy support

    async def worker(self) -> None:
        """Push data to the server when available from the copy queue.

        Terminate reading when the queue receives a None.

        The function is designed to be run in a separate thread.
        """
        while 1:
            data = await self._queue.get()
            if not data:
                break
            await self.connection.wait(copy_to(self._pgconn, data))

    async def _write(self, data: bytes) -> None:
        if not data:
            return

        if not self._worker:
            self._worker = create_task(self.worker())

        await self._queue.put(data)

    async def _write_end(self) -> None:
        data = self.formatter.end()
        await self._write(data)
        await self._queue.put(None)

        if self._worker:
            await asyncio.gather(self._worker)
            self._worker = None  # break reference loops if any


class Formatter(ABC):
    """
    A class which understand a copy format (text, binary).
    """

    format: pq.Format

    # Size of data to accumulate before sending it down the network
    BUFFER_SIZE = 32 * 1024

    def __init__(self, transformer: Transformer):
        self.transformer = transformer
        self._write_buffer = bytearray()
        self._row_mode = False  # true if the user is using write_row()

    @abstractmethod
    def parse_row(self, data: bytes) -> Optional[Tuple[Any, ...]]:
        ...

    @abstractmethod
    def write(self, buffer: Union[str, bytes]) -> bytes:
        ...

    @abstractmethod
    def write_row(self, row: Sequence[Any]) -> bytes:
        ...

    @abstractmethod
    def end(self) -> bytes:
        ...


class TextFormatter(Formatter):

    format = pq.Format.TEXT

    def __init__(self, transformer: Transformer, encoding: str = "utf-8"):
        super().__init__(transformer)
        self._encoding = encoding

    def parse_row(self, data: bytes) -> Optional[Tuple[Any, ...]]:
        if data:
            return parse_row_text(data, self.transformer)
        else:
            return None

    def write(self, buffer: Union[str, bytes]) -> bytes:
        data = self._ensure_bytes(buffer)
        self._signature_sent = True
        return data

    def write_row(self, row: Sequence[Any]) -> bytes:
        # Note down that we are writing in row mode: it means we will have
        # to take care of the end-of-copy marker too
        self._row_mode = True

        format_row_text(row, self.transformer, self._write_buffer)
        if len(self._write_buffer) > self.BUFFER_SIZE:
            buffer, self._write_buffer = self._write_buffer, bytearray()
            return buffer
        else:
            return b""

    def end(self) -> bytes:
        buffer, self._write_buffer = self._write_buffer, bytearray()
        return buffer

    def _ensure_bytes(self, data: Union[bytes, str]) -> bytes:
        if isinstance(data, bytes):
            return data

        elif isinstance(data, str):
            return data.encode(self._encoding)

        else:
            raise TypeError(f"can't write {type(data).__name__}")


class BinaryFormatter(Formatter):

    format = pq.Format.BINARY

    def __init__(self, transformer: Transformer):
        super().__init__(transformer)
        self._signature_sent = False

    def parse_row(self, data: bytes) -> Optional[Tuple[Any, ...]]:
        if not self._signature_sent:
            if data[: len(_binary_signature)] != _binary_signature:
                raise e.DataError(
                    "binary copy doesn't start with the expected signature"
                )
            self._signature_sent = True
            data = data[len(_binary_signature) :]

        elif data == _binary_trailer:
            return None

        return parse_row_binary(data, self.transformer)

    def write(self, buffer: Union[str, bytes]) -> bytes:
        data = self._ensure_bytes(buffer)
        self._signature_sent = True
        return data

    def write_row(self, row: Sequence[Any]) -> bytes:
        # Note down that we are writing in row mode: it means we will have
        # to take care of the end-of-copy marker too
        self._row_mode = True

        if not self._signature_sent:
            self._write_buffer += _binary_signature
            self._signature_sent = True

        format_row_binary(row, self.transformer, self._write_buffer)
        if len(self._write_buffer) > self.BUFFER_SIZE:
            buffer, self._write_buffer = self._write_buffer, bytearray()
            return buffer
        else:
            return b""

    def end(self) -> bytes:
        # If we have sent no data we need to send the signature
        # and the trailer
        if not self._signature_sent:
            self._write_buffer += _binary_signature
            self._write_buffer += _binary_trailer

        elif self._row_mode:
            # if we have sent data already, we have sent the signature
            # too (either with the first row, or we assume that in
            # block mode the signature is included).
            # Write the trailer only if we are sending rows (with the
            # assumption that who is copying binary data is sending the
            # whole format).
            self._write_buffer += _binary_trailer

        buffer, self._write_buffer = self._write_buffer, bytearray()
        return buffer

    def _ensure_bytes(self, data: Union[bytes, str]) -> bytes:
        if isinstance(data, bytes):
            return data

        elif isinstance(data, str):
            raise TypeError(
                "cannot copy str data in binary mode: use bytes instead"
            )

        else:
            raise TypeError(f"can't write {type(data).__name__}")


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
            dumper = tx.get_dumper(item, PyFormat.TEXT)
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
    adapted = tx.dump_sequence(row, [PyFormat.BINARY] * len(row))
    for b in adapted:
        if b is not None:
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
if _psycopg:
    format_row_text = _psycopg.format_row_text
    format_row_binary = _psycopg.format_row_binary
    parse_row_text = _psycopg.parse_row_text
    parse_row_binary = _psycopg.parse_row_binary

else:
    format_row_text = _format_row_text
    format_row_binary = _format_row_binary
    parse_row_text = _parse_row_text
    parse_row_binary = _parse_row_binary
