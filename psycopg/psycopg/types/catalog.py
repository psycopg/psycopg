"""
Adapters for catalog types.
"""

# Copyright (C) 2026 The Psycopg Team

from __future__ import annotations

from .. import _oids
from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Dumper, Loader
from .._struct import pack_uint8, unpack_uint8


class Lsn(str):
    """PostgreSQL LSN (Log Sequence Number).

    Subclass of ``str`` so existing code that receives ``pg_lsn`` as a plain
    string continues to work. Ordering operators use integer comparison, which
    is correct for LSNs (string ordering is wrong when the hex segments differ
    in digit count, e.g. ``"F/0"`` vs ``"10/0"``).
    """

    __slots__ = ("_value",)

    _value: int

    def __new__(cls, value: int | str) -> Lsn:
        if isinstance(value, str):
            shigh, slow = value.split("/")
            value = (int(shigh, 16) << 32) | int(slow, 16)

        if not 0 <= value < 2**64:
            raise OverflowError("Lsn value must be in the unsigned 64 bits range")

        high = value >> 32
        low = value & 0xFFFFFFFF
        s = f"{high:X}/{low:X}"

        lsn = super().__new__(cls, s)
        lsn._value = value
        return lsn

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __int__(self) -> int:
        return self._value

    def __bool__(self) -> bool:
        return bool(self._value)

    def __add__(self, other: int) -> Lsn:  # type: ignore[override]
        return Lsn(int(self) + other)

    def __sub__(self, other: Lsn) -> int:
        return int(self) - int(other)

    def __lt__(self, other: object) -> bool:
        if isinstance(other, str):
            return int(self) < int(Lsn(other))
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, str):
            return int(self) <= int(Lsn(other))
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, str):
            return int(self) > int(Lsn(other))
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, str):
            return int(self) >= int(Lsn(other))
        return NotImplemented


class LsnLoader(Loader):
    def load(self, data: Buffer) -> Lsn:
        return Lsn(bytes(data).decode())


class LsnBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> Lsn:
        return Lsn(unpack_uint8(data)[0])


class LsnDumper(Dumper):
    oid = _oids.PG_LSN_OID

    def dump(self, obj: Lsn) -> Buffer | None:
        return str(obj).encode()


class LsnBinaryDumper(Dumper):
    format = Format.BINARY
    oid = _oids.PG_LSN_OID

    def dump(self, obj: Lsn) -> Buffer | None:
        return pack_uint8(int(obj))


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters
    adapters.register_loader("pg_lsn", LsnLoader)
    adapters.register_loader("pg_lsn", LsnBinaryLoader)
    adapters.register_dumper(Lsn, LsnDumper)
    adapters.register_dumper(Lsn, LsnBinaryDumper)
