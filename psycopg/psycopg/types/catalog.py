"""
Adapters for PostgreSQL catalog types.

Covers: cid, xid, xid8, pg_lsn, tid, int2vector, oidvector.
"""

# Copyright (C) 2024 The Psycopg Team

from __future__ import annotations

import struct

from .. import postgres
from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Loader
from .._struct import unpack_uint4


class CidLoader(Loader):
    """Load cid text values as Python int."""

    def load(self, data: Buffer) -> int:
        return int(data)


class CidBinaryLoader(Loader):
    """Load cid binary values as Python int."""

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_uint4(data)[0]


class XidLoader(Loader):
    """Load xid text values as Python int."""

    def load(self, data: Buffer) -> int:
        return int(data)


class XidBinaryLoader(Loader):
    """Load xid binary values as Python int."""

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_uint4(data)[0]


_unpack_uint8 = struct.Struct("!Q").unpack


class Xid8Loader(Loader):
    """Load xid8 text values as Python int."""

    def load(self, data: Buffer) -> int:
        return int(data)


class Xid8BinaryLoader(Loader):
    """Load xid8 binary values as Python int."""

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return int(_unpack_uint8(data)[0])


class PgLsnLoader(Loader):
    """Load pg_lsn text values as a 64-bit integer offset."""

    def load(self, data: Buffer) -> int:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        hi, lo = text.split("/")
        return (int(hi, 16) << 32) | int(lo, 16)


class PgLsnBinaryLoader(Loader):
    """Load pg_lsn binary values as Python int."""

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return int(_unpack_uint8(data)[0])


_unpack_tid = struct.Struct("!IH").unpack


class TidLoader(Loader):
    """Load tid text values as ``(block, offset)`` tuple."""

    def load(self, data: Buffer) -> tuple[int, int]:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        text = text.strip("()")
        block_str, offset_str = text.split(",")
        return (int(block_str), int(offset_str))


class TidBinaryLoader(Loader):
    """Load tid binary values as ``(block, offset)`` tuple."""

    format = Format.BINARY

    def load(self, data: Buffer) -> tuple[int, int]:
        block, offset = _unpack_tid(data)
        return (block, offset)


_unpack_int2_s = struct.Struct("!h").unpack


class Int2VectorLoader(Loader):
    """Load int2vector text values as ``list[int]``."""

    def load(self, data: Buffer) -> list[int]:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        if not text.strip():
            return []
        return [int(v) for v in text.split()]


class Int2VectorBinaryLoader(Loader):
    """Load int2vector binary values as ``list[int]``."""

    format = Format.BINARY

    def load(self, data: Buffer) -> list[int]:
        buf = bytes(data) if isinstance(data, memoryview) else data
        if len(buf) < 20:
            return []

        offset = 0
        ndim = struct.unpack_from("!i", buf, offset)[0]
        offset += 12
        if ndim == 0:
            return []

        dim = struct.unpack_from("!i", buf, offset)[0]
        offset += 8

        result: list[int] = []
        for _ in range(dim):
            elem_len = struct.unpack_from("!i", buf, offset)[0]
            offset += 4
            if elem_len == -1:
                result.append(0)
            else:
                result.append(_unpack_int2_s(buf[offset : offset + elem_len])[0])
                offset += elem_len

        return result


_unpack_uint4_s = struct.Struct("!I").unpack


class OidVectorLoader(Loader):
    """Load oidvector text values as ``list[int]``."""

    def load(self, data: Buffer) -> list[int]:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        if not text.strip():
            return []
        return [int(v) for v in text.split()]


class OidVectorBinaryLoader(Loader):
    """Load oidvector binary values as ``list[int]``."""

    format = Format.BINARY

    def load(self, data: Buffer) -> list[int]:
        buf = bytes(data) if isinstance(data, memoryview) else data
        if len(buf) < 20:
            return []

        offset = 12
        ndim = struct.unpack_from("!i", buf, 0)[0]
        if ndim == 0:
            return []

        dim = struct.unpack_from("!i", buf, offset)[0]
        offset += 8

        result: list[int] = []
        for _ in range(dim):
            elem_len = struct.unpack_from("!i", buf, offset)[0]
            offset += 4
            if elem_len == -1:
                result.append(0)
            else:
                result.append(_unpack_uint4_s(buf[offset : offset + elem_len])[0])
                offset += elem_len

        return result


def register_catalog(context: AdaptContext | None = None) -> None:
    """Register catalog-type loaders in the given context or globally."""

    adapters = context.adapters if context else postgres.adapters

    adapters.register_loader("cid", CidLoader)
    adapters.register_loader("cid", CidBinaryLoader)

    adapters.register_loader("xid", XidLoader)
    adapters.register_loader("xid", XidBinaryLoader)

    adapters.register_loader("xid8", Xid8Loader)
    adapters.register_loader("xid8", Xid8BinaryLoader)

    adapters.register_loader("pg_lsn", PgLsnLoader)
    adapters.register_loader("pg_lsn", PgLsnBinaryLoader)

    adapters.register_loader("tid", TidLoader)
    adapters.register_loader("tid", TidBinaryLoader)

    adapters.register_loader("int2vector", Int2VectorLoader)
    adapters.register_loader("int2vector", Int2VectorBinaryLoader)

    adapters.register_loader("oidvector", OidVectorLoader)
    adapters.register_loader("oidvector", OidVectorBinaryLoader)


def register_default_adapters(context: AdaptContext) -> None:
    register_catalog(context)
