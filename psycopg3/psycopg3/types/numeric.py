"""
Adapers for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import struct
from typing import Any, Callable, Dict, Tuple, cast
from decimal import Decimal

from ..adapt import Dumper, Loader
from ..proto import EncodeFunc, DecodeFunc
from .oids import builtins

UnpackInt = Callable[[bytes], Tuple[int]]
UnpackFloat = Callable[[bytes], Tuple[float]]

FLOAT8_OID = builtins["float8"].oid
NUMERIC_OID = builtins["numeric"].oid

_encode_ascii = codecs.lookup("ascii").encode
_decode_ascii = codecs.lookup("ascii").decode


class NumberDumper(Dumper):
    _special: Dict[bytes, bytes] = {}

    def dump(self, obj: Any, __encode: EncodeFunc = _encode_ascii) -> bytes:
        return __encode(str(obj))[0]

    def quote(self, obj: Any) -> bytes:
        value = self.dump(obj)

        if value in self._special:
            return self._special[value]

        return b" " + value if value.startswith(b"-") else value


@Dumper.text(int)
class TextIntDumper(NumberDumper):
    @property
    def oid(self) -> int:
        # We don't know the size of it, so we have to return a type big enough
        return NUMERIC_OID


@Dumper.text(float)
class TextFloatDumper(NumberDumper):
    _special = {
        b"inf": b"'Infinity'::float8",
        b"-inf": b"'-Infinity'::float8",
        b"nan": b"'NaN'::float8",
    }

    @property
    def oid(self) -> int:
        # Float can't be bigger than this instead
        return FLOAT8_OID


@Dumper.text(Decimal)
class TextDecimalDumper(NumberDumper):
    _special = {
        b"Infinity": b"'Infinity'::numeric",
        b"-Infinity": b"'-Infinity'::numeric",
        b"NaN": b"'NaN'::numeric",
    }

    @property
    def oid(self) -> int:
        return NUMERIC_OID


@Loader.text(builtins["int2"].oid)
@Loader.text(builtins["int4"].oid)
@Loader.text(builtins["int8"].oid)
@Loader.text(builtins["oid"].oid)
class TextIntLoader(Loader):
    def load(self, data: bytes, __decode: DecodeFunc = _decode_ascii) -> int:
        return int(__decode(data)[0])


@Loader.binary(builtins["int2"].oid)
class BinaryInt2Loader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!h").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["int4"].oid)
class BinaryInt4Loader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!i").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["int8"].oid)
class BinaryInt8Loader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!q").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["oid"].oid)
class BinaryOidLoader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!I").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.text(builtins["float4"].oid)
@Loader.text(builtins["float8"].oid)
class TextFloatLoader(Loader):
    def load(self, data: bytes) -> float:
        # it supports bytes directly
        return float(data)


@Loader.binary(builtins["float4"].oid)
class BinaryFloat4Loader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!f").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.binary(builtins["float8"].oid)
class BinaryFloat8Loader(Loader):
    def load(
        self,
        data: bytes,
        __unpack: UnpackInt = cast(UnpackInt, struct.Struct("!d").unpack),
    ) -> int:
        return __unpack(data)[0]


@Loader.text(builtins["numeric"].oid)
class TextNumericLoader(Loader):
    def load(
        self, data: bytes, __decode: DecodeFunc = _decode_ascii
    ) -> Decimal:
        return Decimal(__decode(data)[0])
