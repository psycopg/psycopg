"""
Adapers for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import struct
from typing import Callable, Dict, Tuple, cast
from decimal import Decimal

from ..adapt import Dumper, Loader
from ..proto import EncodeFunc, DecodeFunc
from .oids import builtins

UnpackInt = Callable[[bytes], Tuple[int]]
UnpackFloat = Callable[[bytes], Tuple[float]]

FLOAT8_OID = builtins["float8"].oid
NUMERIC_OID = builtins["numeric"].oid
BOOL_OID = builtins["bool"].oid


@Dumper.text(int)
class TextIntDumper(Dumper):
    def dump(
        self, obj: int, __encode: EncodeFunc = codecs.lookup("ascii").encode
    ) -> bytes:
        return __encode(str(obj))[0]

    @property
    def oid(self) -> int:
        # We don't know the size of it, so we have to return a type big enough
        return NUMERIC_OID


@Dumper.text(float)
class TextFloatDumper(Dumper):
    def dump(
        self, obj: float, __encode: EncodeFunc = codecs.lookup("ascii").encode
    ) -> bytes:
        return __encode(str(obj))[0]

    @property
    def oid(self) -> int:
        # Float can't be bigger than this instead
        return FLOAT8_OID


@Dumper.text(Decimal)
class TextDecimalDumper(Dumper):
    def dump(
        self,
        obj: Decimal,
        __encode: EncodeFunc = codecs.lookup("ascii").encode,
    ) -> bytes:
        return __encode(str(obj))[0]

    @property
    def oid(self) -> int:
        return NUMERIC_OID


@Dumper.text(bool)
class TextBoolDumper(Dumper):
    def dump(self, obj: bool) -> bytes:
        return b"t" if obj else b"f"

    @property
    def oid(self) -> int:
        return BOOL_OID


@Dumper.binary(bool)
class BinaryBoolDumper(Dumper):
    def dump(self, obj: bool) -> bytes:
        return b"\x01" if obj else b"\x00"

    @property
    def oid(self) -> int:
        return BOOL_OID


@Loader.text(builtins["int2"].oid)
@Loader.text(builtins["int4"].oid)
@Loader.text(builtins["int8"].oid)
@Loader.text(builtins["oid"].oid)
class TextIntLoader(Loader):
    def load(
        self, data: bytes, __decode: DecodeFunc = codecs.lookup("ascii").decode
    ) -> int:
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
        self, data: bytes, __decode: DecodeFunc = codecs.lookup("ascii").decode
    ) -> Decimal:
        return Decimal(__decode(data)[0])


@Loader.text(builtins["bool"].oid)
class TextBoolLoader(Loader):
    def load(
        self,
        data: bytes,
        __values: Dict[bytes, bool] = {b"t": True, b"f": False},
    ) -> bool:
        return __values[data]


@Loader.binary(builtins["bool"].oid)
class BinaryBoolLoader(Loader):
    def load(
        self,
        data: bytes,
        __values: Dict[bytes, bool] = {b"\x01": True, b"\x00": False},
    ) -> bool:
        return __values[data]
