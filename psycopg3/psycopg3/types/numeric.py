"""
Adapers for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import struct
from typing import Any, Callable, Dict, Tuple, cast
from decimal import Decimal

from ..oids import builtins
from ..adapt import Dumper, Loader
from ..utils.codecs import EncodeFunc, DecodeFunc, encode_ascii, decode_ascii

UnpackInt = Callable[[bytes], Tuple[int]]
UnpackFloat = Callable[[bytes], Tuple[float]]


class NumberDumper(Dumper):
    _special: Dict[bytes, bytes] = {}

    def dump(self, obj: Any, __encode: EncodeFunc = encode_ascii) -> bytes:
        return __encode(str(obj))[0]

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


@Loader.text(builtins["int2"].oid)
@Loader.text(builtins["int4"].oid)
@Loader.text(builtins["int8"].oid)
@Loader.text(builtins["oid"].oid)
class IntLoader(Loader):
    def load(self, data: bytes, __decode: DecodeFunc = decode_ascii) -> int:
        return int(__decode(data)[0])


@Loader.binary(builtins["int2"].oid)
class Int2BinaryLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!h").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["int4"].oid)
class Int4BinaryLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!i").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["int8"].oid)
class Int8BinaryLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!q").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["oid"].oid)
class OidBinaryLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!I").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.text(builtins["float4"].oid)
@Loader.text(builtins["float8"].oid)
class FloatLoader(Loader):
    def load(self, data: bytes) -> float:
        # it supports bytes directly
        return float(data)


@Loader.binary(builtins["float4"].oid)
class Float4BinaryLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!f").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["float8"].oid)
class Float8BinaryLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!d").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.text(builtins["numeric"].oid)
class NumericLoader(Loader):
    def load(
        self, data: bytes, __decode: DecodeFunc = decode_ascii
    ) -> Decimal:
        return Decimal(__decode(data)[0])
