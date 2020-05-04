"""
Adapers for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import struct
from decimal import Decimal
from typing import Tuple

from ..adapt import Dumper, Loader
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


@Dumper.text(int)
def dump_int(obj: int) -> Tuple[bytes, int]:
    # We don't know the size of it, so we have to return a type big enough
    return _encode(str(obj))[0], NUMERIC_OID


@Dumper.text(float)
def dump_float(obj: float) -> Tuple[bytes, int]:
    # Float can't be bigger than this instead
    return _encode(str(obj))[0], FLOAT8_OID


@Dumper.text(Decimal)
def dump_decimal(obj: Decimal) -> Tuple[bytes, int]:
    return _encode(str(obj))[0], NUMERIC_OID


_bool_dump = {
    True: (b"t", builtins["bool"].oid),
    False: (b"f", builtins["bool"].oid),
}
_bool_binary_dump = {
    True: (b"\x01", builtins["bool"].oid),
    False: (b"\x00", builtins["bool"].oid),
}


@Dumper.text(bool)
def dump_bool(obj: bool) -> Tuple[bytes, int]:
    return _bool_dump[obj]


@Dumper.binary(bool)
def dump_bool_binary(obj: bool) -> Tuple[bytes, int]:
    return _bool_binary_dump[obj]


@Loader.text(builtins["int2"].oid)
@Loader.text(builtins["int4"].oid)
@Loader.text(builtins["int8"].oid)
@Loader.text(builtins["oid"].oid)
def load_int(data: bytes) -> int:
    return int(_decode(data)[0])


@Loader.binary(builtins["int2"].oid)
def load_int2_binary(data: bytes) -> int:
    rv: int = _int2_struct.unpack(data)[0]
    return rv


@Loader.binary(builtins["int4"].oid)
def load_int4_binary(data: bytes) -> int:
    rv: int = _int4_struct.unpack(data)[0]
    return rv


@Loader.binary(builtins["int8"].oid)
def load_int8_binary(data: bytes) -> int:
    rv: int = _int8_struct.unpack(data)[0]
    return rv


@Loader.binary(builtins["oid"].oid)
def load_oid_binary(data: bytes) -> int:
    rv: int = _oid_struct.unpack(data)[0]
    return rv


@Loader.text(builtins["float4"].oid)
@Loader.text(builtins["float8"].oid)
def load_float(data: bytes) -> float:
    # it supports bytes directly
    return float(data)


@Loader.binary(builtins["float4"].oid)
def load_float4_binary(data: bytes) -> float:
    rv: float = _float4_struct.unpack(data)[0]
    return rv


@Loader.binary(builtins["float8"].oid)
def load_float8_binary(data: bytes) -> float:
    rv: float = _float8_struct.unpack(data)[0]
    return rv


@Loader.text(builtins["numeric"].oid)
def load_numeric(data: bytes) -> Decimal:
    return Decimal(_decode(data)[0])


_bool_loads = {b"t": True, b"f": False}
_bool_binary_loads = {b"\x01": True, b"\x00": False}


@Loader.text(builtins["bool"].oid)
def load_bool(data: bytes) -> bool:
    return _bool_loads[data]


@Loader.binary(builtins["bool"].oid)
def load_bool_binary(data: bytes) -> bool:
    return _bool_binary_loads[data]
