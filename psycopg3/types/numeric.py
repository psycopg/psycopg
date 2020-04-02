"""
Adapters of numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import struct
from decimal import Decimal
from typing import Tuple

from ..adapt import Adapter, Typecaster
from ..utils.typing import Oid
from .oids import type_oid

_encode = codecs.lookup("ascii").encode
_decode = codecs.lookup("ascii").decode

_int2_struct = struct.Struct("!h")
_int4_struct = struct.Struct("!i")
_int8_struct = struct.Struct("!q")
_oid_struct = struct.Struct("!I")
_float4_struct = struct.Struct("!f")
_float8_struct = struct.Struct("!d")


@Adapter.text(int)
def adapt_int(obj: int) -> Tuple[bytes, Oid]:
    # We don't know the size of it, so we have to return a type big enough
    return _encode(str(obj))[0], type_oid["numeric"]


@Adapter.text(float)
def adapt_float(obj: float) -> Tuple[bytes, Oid]:
    # Float can't be bigger than this instead
    return _encode(str(obj))[0], type_oid["float8"]


@Adapter.text(Decimal)
def adapt_decimal(obj: Decimal) -> Tuple[bytes, Oid]:
    # Float can't be bigger than this instead
    return _encode(str(obj))[0], type_oid["numeric"]


_bool_adapt = {
    True: (b"t", type_oid["bool"]),
    False: (b"f", type_oid["bool"]),
}


@Adapter.text(bool)
def adapt_bool(obj: bool) -> Tuple[bytes, Oid]:
    return _bool_adapt[obj]


@Typecaster.text(type_oid["int2"])
@Typecaster.text(type_oid["int4"])
@Typecaster.text(type_oid["int8"])
@Typecaster.text(type_oid["oid"])
def cast_int(data: bytes) -> int:
    return int(_decode(data)[0])


@Typecaster.binary(type_oid["int2"])
def cast_binary_int2(data: bytes) -> int:
    rv: int = _int2_struct.unpack(data)[0]
    return rv


@Typecaster.binary(type_oid["int4"])
def cast_binary_int4(data: bytes) -> int:
    rv: int = _int4_struct.unpack(data)[0]
    return rv


@Typecaster.binary(type_oid["int8"])
def cast_binary_int8(data: bytes) -> int:
    rv: int = _int8_struct.unpack(data)[0]
    return rv


@Typecaster.binary(type_oid["oid"])
def cast_binary_oid(data: bytes) -> int:
    rv: int = _oid_struct.unpack(data)[0]
    return rv


@Typecaster.text(type_oid["float4"])
@Typecaster.text(type_oid["float8"])
def cast_float(data: bytes) -> float:
    # it supports bytes directly
    return float(data)


@Typecaster.binary(type_oid["float4"])
def cast_binary_float4(data: bytes) -> float:
    rv: float = _float4_struct.unpack(data)[0]
    return rv


@Typecaster.binary(type_oid["float8"])
def cast_binary_float8(data: bytes) -> float:
    rv: float = _float8_struct.unpack(data)[0]
    return rv


@Typecaster.text(type_oid["numeric"])
def cast_numeric(data: bytes) -> Decimal:
    return Decimal(_decode(data)[0])


_bool_casts = {b"t": True, b"f": False}
_bool_binary_casts = {b"\x01": True, b"\x00": False}


@Typecaster.text(type_oid["bool"])
def cast_bool(data: bytes) -> bool:
    return _bool_casts[data]


@Typecaster.binary(type_oid["bool"])
def cast_binary_bool(data: bytes) -> bool:
    return _bool_binary_casts[data]
