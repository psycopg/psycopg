"""
Adapters for numpy types.
"""

# Copyright (C) 2022 The Psycopg Team

from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

from .. import postgres
from .._struct import pack_int2, pack_int4, pack_int8
from ..abc import AdaptContext, Buffer
from ..adapt import Dumper
from ..pq import Format
from .. import _struct

from .numeric import dump_int_to_numeric_binary, _SpecialValuesDumper

if TYPE_CHECKING:
    import numpy as np

PackNumpyFloat4 = Callable[[Union["np.float16", "np.float32"]], bytes]
PackNumpyFloat8 = Callable[["np.float64"], bytes]

pack_float8 = cast(PackNumpyFloat8, _struct.pack_float8)
pack_float4 = cast(PackNumpyFloat4, _struct.pack_float4)


class _NPIntDumper(Dumper):
    def dump(self, obj: Any) -> Buffer:
        return str(obj).encode()

    def quote(self, obj: Any) -> Buffer:
        value = self.dump(obj)
        return value if obj >= 0 else b" " + value


class NPInt8Dumper(_NPIntDumper):
    oid = postgres.types["int2"].oid


NPInt16Dumper = NPInt8Dumper
NPUInt8Dumper = NPInt8Dumper


class NPInt32Dumper(_NPIntDumper):
    oid = postgres.types["int4"].oid


NPUInt16Dumper = NPInt32Dumper


class NPInt64Dumper(_NPIntDumper):
    oid = postgres.types["int8"].oid


NPUInt32Dumper = NPInt64Dumper


class NPBooleanDumper(_NPIntDumper):
    oid = postgres.types["bool"].oid

    def dump(self, obj: "np.bool_") -> bytes:
        return "t".encode() if bool(obj) is True else "f".encode()


class NPUInt64Dumper(_NPIntDumper):
    oid = postgres.types["numeric"].oid


NPULongLongDumper = NPUInt64Dumper


class _NPFloatDumper(_SpecialValuesDumper):

    _special = {
        b"inf": b"'Infinity'::float8",
        b"-inf": b"'-Infinity'::float8",
        b"nan": b"'NaN'::float8",
    }


class NPFloat16Dumper(_NPFloatDumper):
    oid = postgres.types["float4"].oid


NPFloat32Dumper = NPFloat16Dumper


class NPFloat64Dumper(_NPFloatDumper):

    oid = postgres.types["float8"].oid


# Binary Dumpers


class NPFloat16BinaryDumper(Dumper):
    format = Format.BINARY
    oid = postgres.types["float4"].oid

    def dump(self, obj: "np.float16") -> bytes:
        return pack_float4(obj)


class NPFloat32BinaryDumper(Dumper):
    format = Format.BINARY
    oid = postgres.types["float4"].oid

    def dump(self, obj: "np.float32") -> bytes:
        return pack_float4(obj)


class NPFloat64BinaryDumper(Dumper):
    format = Format.BINARY
    oid = postgres.types["float8"].oid

    def dump(self, obj: "np.float64") -> bytes:
        return pack_float8(obj)


class NPInt8BinaryDumper(NPInt8Dumper):

    format = Format.BINARY

    def dump(self, obj: "np.int8") -> bytes:
        return pack_int2(int(obj))


NPInt16BinaryDumper = NPInt8BinaryDumper
NPUInt8BinaryDumper = NPInt8BinaryDumper


class NPInt32BinaryDumper(NPInt32Dumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> bytes:
        return pack_int4(int(obj))


NPUInt16BinaryDumper = NPInt32BinaryDumper


class NPInt64BinaryDumper(NPInt64Dumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> bytes:
        return pack_int8(int(obj))


NPUInt32BinaryDumper = NPInt64BinaryDumper


class NPBooleanBinaryDumper(NPBooleanDumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> bytes:
        return b"\x01" if obj else b"\x00"


class NPUInt64BinaryDumper(NPUInt64Dumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> Buffer:

        return dump_int_to_numeric_binary(int(obj))


NPUlongLongBinaryDumper = NPUInt64BinaryDumper


def register_default_adapters(context: Optional[AdaptContext] = None) -> None:
    adapters = context.adapters if context else postgres.adapters

    adapters.register_dumper("numpy.int8", NPInt8Dumper)
    adapters.register_dumper("numpy.int16", NPInt16Dumper)
    adapters.register_dumper("numpy.int32", NPInt32Dumper)
    adapters.register_dumper("numpy.int64", NPInt64Dumper)
    adapters.register_dumper("numpy.bool_", NPBooleanDumper)
    adapters.register_dumper("numpy.uint8", NPUInt8Dumper)
    adapters.register_dumper("numpy.uint16", NPUInt16Dumper)
    adapters.register_dumper("numpy.uint32", NPUInt32Dumper)
    adapters.register_dumper("numpy.uint64", NPUInt64Dumper)
    adapters.register_dumper("numpy.ulonglong", NPULongLongDumper)
    adapters.register_dumper("numpy.float16", NPFloat16Dumper)
    adapters.register_dumper("numpy.float32", NPFloat32Dumper)
    adapters.register_dumper("numpy.float64", NPFloat64Dumper)

    adapters.register_dumper("numpy.int8", NPInt8BinaryDumper)
    adapters.register_dumper("numpy.int16", NPInt16BinaryDumper)
    adapters.register_dumper("numpy.int32", NPInt32BinaryDumper)
    adapters.register_dumper("numpy.int64", NPInt64BinaryDumper)
    adapters.register_dumper("numpy.bool_", NPBooleanBinaryDumper)
    adapters.register_dumper("numpy.uint8", NPUInt8BinaryDumper)
    adapters.register_dumper("numpy.uint16", NPUInt16BinaryDumper)
    adapters.register_dumper("numpy.uint32", NPUInt32BinaryDumper)
    adapters.register_dumper("numpy.uint64", NPUInt64BinaryDumper)
    adapters.register_dumper("numpy.ulonglong", NPUlongLongBinaryDumper)
    adapters.register_dumper("numpy.float16", NPFloat16BinaryDumper)
    adapters.register_dumper("numpy.float32", NPFloat32BinaryDumper)
    adapters.register_dumper("numpy.float64", NPFloat64BinaryDumper)
