"""
Adapters for PostgreSQL catalog types.

Covers: cid, xid, xid8, pg_lsn, tid, int2vector, oidvector.
"""

# Copyright (C) 2024 The Psycopg Team

from __future__ import annotations

import struct

from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Loader
from .._struct import unpack_uint4

# ---------------------------------------------------------------------------
# uint32 types: cid, xid
# ---------------------------------------------------------------------------
#
# Both are unsigned 32-bit transaction/command IDs.  They are returned as
# plain Python ints.  Dumping is intentionally NOT registered for plain int
# so that round-trips require an explicit cast — consistent with how oid is
# handled (requires the Oid wrapper).


class CidLoader(Loader):
    def load(self, data: Buffer) -> int:
        return int(data)


class CidBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_uint4(data)[0]


class XidLoader(Loader):
    def load(self, data: Buffer) -> int:
        return int(data)


class XidBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_uint4(data)[0]


# ---------------------------------------------------------------------------
# uint64 types: xid8, pg_lsn
# ---------------------------------------------------------------------------
#
# xid8 is a full 64-bit transaction ID (PostgreSQL 13+).
# pg_lsn is a Write-Ahead Log Sequence Number, displayed as X/Y in text.


_unpack_uint8 = struct.Struct("!Q").unpack


class Xid8Loader(Loader):
    def load(self, data: Buffer) -> int:
        return int(data)


class Xid8BinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return int(_unpack_uint8(data)[0])


class PgLsnLoader(Loader):
    """
    Load pg_lsn as an integer (bytes offset from WAL origin).

    The text representation is 'X/YYYYYYYY' where X and Y are hex values.
    We convert to a single 64-bit integer: (X << 32) | Y.
    """

    def load(self, data: Buffer) -> int:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        hi, lo = text.split("/")
        return (int(hi, 16) << 32) | int(lo, 16)


class PgLsnBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return int(_unpack_uint8(data)[0])


# ---------------------------------------------------------------------------
# tid — tuple identifier (block, offset)
# ---------------------------------------------------------------------------
#
# Binary format: 4 bytes unsigned block + 2 bytes unsigned offset = 6 bytes.
# Text format:   '(block,offset)' (parenthesised, comma-separated).
# Returned as a plain (int, int) tuple.

_unpack_tid = struct.Struct("!IH").unpack


class TidLoader(Loader):
    def load(self, data: Buffer) -> tuple[int, int]:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        # strip parens and split: '(3,1)' → ('3', '1')
        text = text.strip("()")
        block_str, offset_str = text.split(",")
        return (int(block_str), int(offset_str))


class TidBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> tuple[int, int]:
        block, offset = _unpack_tid(data)
        return (block, offset)


# ---------------------------------------------------------------------------
# int2vector — space-separated list of int2 values
# ---------------------------------------------------------------------------
#
# Used internally by PostgreSQL for index key attribute lists.
# Text format:   '1 2 3'
# Binary format: same as a 1-D int2[] array (PostgreSQL array header).
# Returned as list[int].

_unpack_int2_s = struct.Struct("!h").unpack


class Int2VectorLoader(Loader):
    def load(self, data: Buffer) -> list[int]:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        if not text.strip():
            return []
        return [int(v) for v in text.split()]


class Int2VectorBinaryLoader(Loader):
    """
    Parse a PostgreSQL 1-D int2[] binary representation.

    Binary layout (big-endian):
        ndim       int32   (must be 1)
        flags      int32   (0 = no NULLs)
        element_oid int32  (21 = int2)
        dim        int32   (number of elements)
        lbound     int32   (lower bound, usually 0)
        elements:  for each → int32 len + int16 value
    """

    format = Format.BINARY

    def load(self, data: Buffer) -> list[int]:
        buf = bytes(data) if isinstance(data, memoryview) else data
        if len(buf) < 20:
            return []

        offset = 0
        # ndim, flags, element_oid
        ndim = struct.unpack_from("!i", buf, offset)[0]
        offset += 12  # skip ndim(4) + flags(4) + element_oid(4)
        if ndim == 0:
            return []

        dim = struct.unpack_from("!i", buf, offset)[0]
        offset += 8  # skip dim(4) + lbound(4)

        result: list[int] = []
        for _ in range(dim):
            elem_len = struct.unpack_from("!i", buf, offset)[0]
            offset += 4
            if elem_len == -1:
                result.append(0)  # NULL → 0
            else:
                result.append(_unpack_int2_s(buf[offset : offset + elem_len])[0])
                offset += elem_len

        return result


# ---------------------------------------------------------------------------
# oidvector — space-separated list of oid values
# ---------------------------------------------------------------------------
#
# Used internally for function argument type lists.
# Text format:   '23 25 1043'
# Binary format: same as a 1-D oid[] array.
# Returned as list[int].

_unpack_uint4_s = struct.Struct("!I").unpack


class OidVectorLoader(Loader):
    def load(self, data: Buffer) -> list[int]:
        text = bytes(data).decode() if isinstance(data, memoryview) else data.decode()
        if not text.strip():
            return []
        return [int(v) for v in text.split()]


class OidVectorBinaryLoader(Loader):
    """
    Parse a PostgreSQL 1-D oid[] binary representation.

    Same layout as Int2VectorBinaryLoader but elements are uint32.
    """

    format = Format.BINARY

    def load(self, data: Buffer) -> list[int]:
        buf = bytes(data) if isinstance(data, memoryview) else data
        if len(buf) < 20:
            return []

        offset = 12  # skip ndim + flags + element_oid
        ndim = struct.unpack_from("!i", buf, 0)[0]
        if ndim == 0:
            return []

        dim = struct.unpack_from("!i", buf, offset)[0]
        offset += 8  # skip dim + lbound

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


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

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
