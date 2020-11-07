"""
Adapers for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import struct
from typing import Any, Callable, Dict, Tuple, cast
from decimal import Decimal

from ..oids import builtins
from ..adapt import Dumper, Loader

_PackInt = Callable[[int], bytes]
_UnpackInt = Callable[[bytes], Tuple[int]]
_UnpackFloat = Callable[[bytes], Tuple[float]]

_pack_int2 = cast(_PackInt, struct.Struct("!h").pack)
_pack_int4 = cast(_PackInt, struct.Struct("!i").pack)
_pack_uint4 = cast(_PackInt, struct.Struct("!I").pack)
_pack_int8 = cast(_PackInt, struct.Struct("!q").pack)
_unpack_int2 = cast(_UnpackInt, struct.Struct("!h").unpack)
_unpack_int4 = cast(_UnpackInt, struct.Struct("!i").unpack)
_unpack_uint4 = cast(_UnpackInt, struct.Struct("!I").unpack)
_unpack_int8 = cast(_UnpackInt, struct.Struct("!q").unpack)
_unpack_float4 = cast(_UnpackFloat, struct.Struct("!f").unpack)
_unpack_float8 = cast(_UnpackFloat, struct.Struct("!d").unpack)


# Wrappers to force numbers to be cast as specific PostgreSQL types


class Int2(int):
    def __new__(cls, arg: int) -> "Int2":
        rv: Int2 = super().__new__(cls, arg)  # type: ignore[call-arg]
        return rv


class Int4(int):
    def __new__(cls, arg: int) -> "Int4":
        rv: Int4 = super().__new__(cls, arg)  # type: ignore[call-arg]
        return rv


class Int8(int):
    def __new__(cls, arg: int) -> "Int8":
        rv: Int8 = super().__new__(cls, arg)  # type: ignore[call-arg]
        return rv


class Oid(int):
    def __new__(cls, arg: int) -> "Oid":
        rv: Oid = super().__new__(cls, arg)  # type: ignore[call-arg]
        return rv


class NumberDumper(Dumper):
    _special: Dict[bytes, bytes] = {}

    def dump(self, obj: Any) -> bytes:
        return str(obj).encode("utf8")

    def quote(self, obj: Any) -> bytes:
        value = self.dump(obj)

        if value in self._special:
            return self._special[value]

        return b" " + value if value.startswith(b"-") else value


@Dumper.text(int)
class IntDumper(NumberDumper):
    # We don't know the size of it, so we have to return a type big enough
    oid = builtins["numeric"].oid


@Dumper.text(float)
class FloatDumper(NumberDumper):

    oid = builtins["float8"].oid

    _special = {
        b"inf": b"'Infinity'::float8",
        b"-inf": b"'-Infinity'::float8",
        b"nan": b"'NaN'::float8",
    }


@Dumper.text(Decimal)
class DecimalDumper(NumberDumper):

    oid = builtins["numeric"].oid

    _special = {
        b"Infinity": b"'Infinity'::numeric",
        b"-Infinity": b"'-Infinity'::numeric",
        b"NaN": b"'NaN'::numeric",
    }


@Dumper.text(Int2)
class Int2Dumper(NumberDumper):
    oid = builtins["int2"].oid


@Dumper.text(Int4)
class Int4Dumper(NumberDumper):
    oid = builtins["int4"].oid


@Dumper.text(Int8)
class Int8Dumper(NumberDumper):
    oid = builtins["int8"].oid


@Dumper.text(Oid)
class OidDumper(NumberDumper):
    oid = builtins["oid"].oid


@Dumper.binary(Int2)
class Int2BinaryDumper(Int2Dumper):
    def dump(self, obj: int) -> bytes:
        return _pack_int2(obj)


@Dumper.binary(Int4)
class Int4BinaryDumper(Int4Dumper):
    def dump(self, obj: int) -> bytes:
        return _pack_int4(obj)


@Dumper.binary(Int8)
class Int8BinaryDumper(Int8Dumper):
    def dump(self, obj: int) -> bytes:
        return _pack_int8(obj)


@Dumper.binary(Oid)
class OidBinaryDumper(OidDumper):
    def dump(self, obj: int) -> bytes:
        return _pack_uint4(obj)


@Loader.text(builtins["int2"].oid)
@Loader.text(builtins["int4"].oid)
@Loader.text(builtins["int8"].oid)
@Loader.text(builtins["oid"].oid)
class IntLoader(Loader):
    def load(self, data: bytes) -> int:
        return int(data.decode("utf8"))


@Loader.binary(builtins["int2"].oid)
class Int2BinaryLoader(Loader):
    def load(self, data: bytes) -> int:
        return _unpack_int2(data)[0]


@Loader.binary(builtins["int4"].oid)
class Int4BinaryLoader(Loader):
    def load(self, data: bytes) -> int:
        return _unpack_int4(data)[0]


@Loader.binary(builtins["int8"].oid)
class Int8BinaryLoader(Loader):
    def load(self, data: bytes) -> int:
        return _unpack_int8(data)[0]


@Loader.binary(builtins["oid"].oid)
class OidBinaryLoader(Loader):
    def load(self, data: bytes) -> int:
        return _unpack_uint4(data)[0]


@Loader.text(builtins["float4"].oid)
@Loader.text(builtins["float8"].oid)
class FloatLoader(Loader):
    def load(self, data: bytes) -> float:
        # it supports bytes directly
        return float(data)


@Loader.binary(builtins["float4"].oid)
class Float4BinaryLoader(Loader):
    def load(self, data: bytes) -> float:
        return _unpack_float4(data)[0]


@Loader.binary(builtins["float8"].oid)
class Float8BinaryLoader(Loader):
    def load(self, data: bytes) -> float:
        return _unpack_float8(data)[0]


@Loader.text(builtins["numeric"].oid)
class NumericLoader(Loader):
    def load(self, data: bytes) -> Decimal:
        return Decimal(data.decode("utf8"))
