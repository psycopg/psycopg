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

from .bool import BoolDumper, BoolBinaryDumper
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


class NPInt16Dumper(_NPIntDumper):
    oid = postgres.types["int2"].oid


class NPInt32Dumper(_NPIntDumper):
    oid = postgres.types["int4"].oid


class NPInt64Dumper(_NPIntDumper):
    oid = postgres.types["int8"].oid


class NPNumericDumper(_NPIntDumper):
    oid = postgres.types["numeric"].oid


class _NPFloatDumper(_SpecialValuesDumper):

    _special = {
        b"inf": b"'Infinity'::float8",
        b"-inf": b"'-Infinity'::float8",
        b"nan": b"'NaN'::float8",
    }


class NPFloat32Dumper(_NPFloatDumper):
    oid = postgres.types["float4"].oid


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


class NPInt16BinaryDumper(NPInt16Dumper):

    format = Format.BINARY

    def dump(self, obj: "np.int8") -> bytes:
        return pack_int2(int(obj))


class NPInt32BinaryDumper(NPInt32Dumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> bytes:
        return pack_int4(int(obj))


class NPInt64BinaryDumper(NPInt64Dumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> bytes:
        return pack_int8(int(obj))


class NPNumericBinaryDumper(NPNumericDumper):

    format = Format.BINARY

    def dump(self, obj: Any) -> Buffer:

        return dump_int_to_numeric_binary(int(obj))


def register_default_adapters(context: Optional[AdaptContext] = None) -> None:
    adapters = context.adapters if context else postgres.adapters

    adapters.register_dumper("numpy.int8", NPInt16Dumper)
    adapters.register_dumper("numpy.int16", NPInt16Dumper)
    adapters.register_dumper("numpy.int32", NPInt32Dumper)
    adapters.register_dumper("numpy.int64", NPInt64Dumper)
    adapters.register_dumper("numpy.bool_", BoolDumper)
    adapters.register_dumper("numpy.uint8", NPInt16Dumper)
    adapters.register_dumper("numpy.uint16", NPInt32Dumper)
    adapters.register_dumper("numpy.uint32", NPInt64Dumper)
    adapters.register_dumper("numpy.uint64", NPNumericDumper)
    adapters.register_dumper("numpy.ulonglong", NPNumericDumper)
    adapters.register_dumper("numpy.float16", NPFloat32Dumper)
    adapters.register_dumper("numpy.float32", NPFloat32Dumper)
    adapters.register_dumper("numpy.float64", NPFloat64Dumper)

    adapters.register_dumper("numpy.int8", NPInt16BinaryDumper)
    adapters.register_dumper("numpy.int16", NPInt16BinaryDumper)
    adapters.register_dumper("numpy.int32", NPInt32BinaryDumper)
    adapters.register_dumper("numpy.int64", NPInt64BinaryDumper)
    adapters.register_dumper("numpy.bool_", BoolBinaryDumper)
    adapters.register_dumper("numpy.uint8", NPInt16BinaryDumper)
    adapters.register_dumper("numpy.uint16", NPInt32BinaryDumper)
    adapters.register_dumper("numpy.uint32", NPInt64BinaryDumper)
    adapters.register_dumper("numpy.uint64", NPNumericBinaryDumper)
    adapters.register_dumper("numpy.ulonglong", NPNumericBinaryDumper)
    adapters.register_dumper("numpy.float16", NPFloat16BinaryDumper)
    adapters.register_dumper("numpy.float32", NPFloat32BinaryDumper)
    adapters.register_dumper("numpy.float64", NPFloat64BinaryDumper)
