"""
Adapters of numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import struct
from decimal import Decimal
from typing import Tuple

from ..adapt import Adapter, Typecaster
from .oids import builtins

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


@Adapter.text(bool)
def adapt_bool(obj: bool) -> Tuple[bytes, int]:
    return _bool_adapt[obj]


@Typecaster.text(builtins["int2"].oid)
@Typecaster.text(builtins["int4"].oid)
@Typecaster.text(builtins["int8"].oid)
@Typecaster.text(builtins["oid"].oid)
def cast_int(data: bytes) -> int:
    return int(_decode(data)[0])


@Typecaster.binary(builtins["int2"].oid)
def cast_binary_int2(data: bytes) -> int:
    rv: int = _int2_struct.unpack(data)[0]
    return rv


@Typecaster.binary(builtins["int4"].oid)
def cast_binary_int4(data: bytes) -> int:
    rv: int = _int4_struct.unpack(data)[0]
    return rv


@Typecaster.binary(builtins["int8"].oid)
def cast_binary_int8(data: bytes) -> int:
    rv: int = _int8_struct.unpack(data)[0]
    return rv


@Typecaster.binary(builtins["oid"].oid)
def cast_binary_oid(data: bytes) -> int:
    rv: int = _oid_struct.unpack(data)[0]
    return rv


@Typecaster.text(builtins["float4"].oid)
@Typecaster.text(builtins["float8"].oid)
def cast_float(data: bytes) -> float:
    # it supports bytes directly
    return float(data)


@Typecaster.binary(builtins["float4"].oid)
def cast_binary_float4(data: bytes) -> float:
    rv: float = _float4_struct.unpack(data)[0]
    return rv


@Typecaster.binary(builtins["float8"].oid)
def cast_binary_float8(data: bytes) -> float:
    rv: float = _float8_struct.unpack(data)[0]
    return rv


@Typecaster.text(builtins["numeric"].oid)
def cast_numeric(data: bytes) -> Decimal:
    return Decimal(_decode(data)[0])


_bool_casts = {b"t": True, b"f": False}
_bool_binary_casts = {b"\x01": True, b"\x00": False}


@Typecaster.text(builtins["bool"].oid)
def cast_bool(data: bytes) -> bool:
    return _bool_casts[data]


@Typecaster.binary(builtins["bool"].oid)
def cast_binary_bool(data: bytes) -> bool:
    return _bool_binary_casts[data]
