"""
Adapters of numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import struct
from decimal import Decimal
from typing import Tuple

from ..adapt import Adapter, TypeCaster
from .oids import builtins
from .array import ArrayCaster

FLOAT8_OID = builtins["float8"].oid
NUMERIC_OID = builtins["numeric"].oid

_encode = codecs.lookup("ascii").encode
_decode = codecs.lookup("ascii").decode

_int2_struct = struct.Struct("!h")
_int4_struct = struct.Struct("!i")
_int8_struct = struct.Struct("!q")
_oid_struct = struct.Struct("!I")
_float4_struct = struct.Struct("!f")
_float8_struct = struct.Struct("!d")


@Adapter.text(int)
def adapt_int(obj: int) -> Tuple[bytes, int]:
    # We don't know the size of it, so we have to return a type big enough
    return _encode(str(obj))[0], NUMERIC_OID


@Adapter.text(float)
def adapt_float(obj: float) -> Tuple[bytes, int]:
    # Float can't be bigger than this instead
    return _encode(str(obj))[0], FLOAT8_OID


@Adapter.text(Decimal)
def adapt_decimal(obj: Decimal) -> Tuple[bytes, int]:
    # Float can't be bigger than this instead
    return _encode(str(obj))[0], NUMERIC_OID


_bool_adapt = {
    True: (b"t", builtins["bool"].oid),
    False: (b"f", builtins["bool"].oid),
}
_bool_binary_adapt = {
    True: (b"\x01", builtins["bool"].oid),
    False: (b"\x00", builtins["bool"].oid),
}


@Adapter.text(bool)
def adapt_bool(obj: bool) -> Tuple[bytes, int]:
    return _bool_adapt[obj]


@Adapter.binary(bool)
def adapt_binary_bool(obj: bool) -> Tuple[bytes, int]:
    return _bool_binary_adapt[obj]


@TypeCaster.text(builtins["int2"].oid)
@TypeCaster.text(builtins["int4"].oid)
@TypeCaster.text(builtins["int8"].oid)
@TypeCaster.text(builtins["oid"].oid)
@ArrayCaster.text(builtins["int2"].array_oid)
@ArrayCaster.text(builtins["int4"].array_oid)
@ArrayCaster.text(builtins["int8"].array_oid)
@ArrayCaster.text(builtins["oid"].array_oid)
def cast_int(data: bytes) -> int:
    return int(_decode(data)[0])


@TypeCaster.binary(builtins["int2"].oid)
@ArrayCaster.binary(builtins["int2"].array_oid)
def cast_binary_int2(data: bytes) -> int:
    rv: int = _int2_struct.unpack(data)[0]
    return rv


@TypeCaster.binary(builtins["int4"].oid)
@ArrayCaster.binary(builtins["int4"].array_oid)
def cast_binary_int4(data: bytes) -> int:
    rv: int = _int4_struct.unpack(data)[0]
    return rv


@TypeCaster.binary(builtins["int8"].oid)
@ArrayCaster.binary(builtins["int8"].array_oid)
def cast_binary_int8(data: bytes) -> int:
    rv: int = _int8_struct.unpack(data)[0]
    return rv


@TypeCaster.binary(builtins["oid"].oid)
@ArrayCaster.binary(builtins["oid"].array_oid)
def cast_binary_oid(data: bytes) -> int:
    rv: int = _oid_struct.unpack(data)[0]
    return rv


@TypeCaster.text(builtins["float4"].oid)
@TypeCaster.text(builtins["float8"].oid)
def cast_float(data: bytes) -> float:
    # it supports bytes directly
    return float(data)


@TypeCaster.binary(builtins["float4"].oid)
def cast_binary_float4(data: bytes) -> float:
    rv: float = _float4_struct.unpack(data)[0]
    return rv


@TypeCaster.binary(builtins["float8"].oid)
def cast_binary_float8(data: bytes) -> float:
    rv: float = _float8_struct.unpack(data)[0]
    return rv


@TypeCaster.text(builtins["numeric"].oid)
def cast_numeric(data: bytes) -> Decimal:
    return Decimal(_decode(data)[0])


_bool_casts = {b"t": True, b"f": False}
_bool_binary_casts = {b"\x01": True, b"\x00": False}


@TypeCaster.text(builtins["bool"].oid)
def cast_bool(data: bytes) -> bool:
    return _bool_casts[data]


@TypeCaster.binary(builtins["bool"].oid)
def cast_binary_bool(data: bytes) -> bool:
    return _bool_binary_casts[data]
