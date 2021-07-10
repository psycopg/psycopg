"""
Adapers for numeric types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import struct
from math import log
from typing import Any, Callable, DefaultDict, Dict, Tuple, Union, cast
from decimal import Decimal, DefaultContext, Context

from .. import errors as e
from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader, PyFormat
from ..proto import AdaptContext
from .._struct import pack_int2, pack_uint2, unpack_int2
from .._struct import pack_int4, pack_uint4, unpack_int4, unpack_uint4
from .._struct import pack_int8, unpack_int8
from .._struct import pack_float8, unpack_float4, unpack_float8

# Exposed here
from .._wrappers import (
    Int2 as Int2,
    Int4 as Int4,
    Int8 as Int8,
    IntNumeric as IntNumeric,
    Oid as Oid,
)


class _NumberDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj: Any) -> bytes:
        return str(obj).encode("utf8")

    def quote(self, obj: Any) -> bytes:
        value = self.dump(obj)
        return value if obj >= 0 else b" " + value


class _SpecialValuesDumper(_NumberDumper):

    _special: Dict[bytes, bytes] = {}

    def quote(self, obj: Any) -> bytes:
        value = self.dump(obj)

        if value in self._special:
            return self._special[value]

        return value if obj >= 0 else b" " + value


class FloatDumper(_SpecialValuesDumper):

    format = Format.TEXT
    _oid = builtins["float8"].oid

    _special = {
        b"inf": b"'Infinity'::float8",
        b"-inf": b"'-Infinity'::float8",
        b"nan": b"'NaN'::float8",
    }


class FloatBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = builtins["float8"].oid

    def dump(self, obj: float) -> bytes:
        return pack_float8(obj)


class DecimalDumper(_SpecialValuesDumper):

    _oid = builtins["numeric"].oid

    def dump(self, obj: Decimal) -> bytes:
        if obj.is_nan():
            # cover NaN and sNaN
            return b"NaN"
        else:
            return str(obj).encode("utf8")

    _special = {
        b"Infinity": b"'Infinity'::numeric",
        b"-Infinity": b"'-Infinity'::numeric",
        b"NaN": b"'NaN'::numeric",
    }


class Int2Dumper(_NumberDumper):
    _oid = builtins["int2"].oid


class Int4Dumper(_NumberDumper):
    _oid = builtins["int4"].oid


class Int8Dumper(_NumberDumper):
    _oid = builtins["int8"].oid


class IntNumericDumper(_NumberDumper):
    _oid = builtins["numeric"].oid


class OidDumper(_NumberDumper):
    _oid = builtins["oid"].oid


class IntDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj: Any) -> bytes:
        raise TypeError(
            f"{type(self).__name__} is a dispatcher to other dumpers:"
            " dump() is not supposed to be called"
        )

    def get_key(self, obj: int, format: PyFormat) -> type:
        return self.upgrade(obj, format).cls

    _int2_dumper = Int2Dumper(Int2)
    _int4_dumper = Int4Dumper(Int4)
    _int8_dumper = Int8Dumper(Int8)
    _int_numeric_dumper = IntNumericDumper(IntNumeric)

    def upgrade(self, obj: int, format: PyFormat) -> Dumper:
        if -(2 ** 31) <= obj < 2 ** 31:
            if -(2 ** 15) <= obj < 2 ** 15:
                return self._int2_dumper
            else:
                return self._int4_dumper
        else:
            if -(2 ** 63) <= obj < 2 ** 63:
                return self._int8_dumper
            else:
                return self._int_numeric_dumper


class Int2BinaryDumper(Int2Dumper):

    format = Format.BINARY

    def dump(self, obj: int) -> bytes:
        return pack_int2(obj)


class Int4BinaryDumper(Int4Dumper):

    format = Format.BINARY

    def dump(self, obj: int) -> bytes:
        return pack_int4(obj)


class Int8BinaryDumper(Int8Dumper):

    format = Format.BINARY

    def dump(self, obj: int) -> bytes:
        return pack_int8(obj)


# Ratio between number of bits required to store a number and number of pg
# decimal digits required.
BIT_PER_PGDIGIT = log(2) / log(10_000)


class IntNumericBinaryDumper(IntNumericDumper):

    format = Format.BINARY

    def dump(self, obj: int) -> bytearray:
        ndigits = int(obj.bit_length() * BIT_PER_PGDIGIT) + 1
        out = bytearray(b"\x00\x00" * (ndigits + 4))
        if obj < 0:
            sign = NUMERIC_NEG
            obj = -obj
        else:
            sign = NUMERIC_POS

        out[:8] = _pack_numeric_head(ndigits, ndigits - 1, sign, 0)
        i = 8 + (ndigits - 1) * 2
        while obj:
            rem = obj % 10_000
            obj //= 10_000
            out[i : i + 2] = pack_uint2(rem)
            i -= 2

        return out


class OidBinaryDumper(OidDumper):

    format = Format.BINARY

    def dump(self, obj: int) -> bytes:
        return pack_uint4(obj)


class IntBinaryDumper(IntDumper):

    format = Format.BINARY

    _int2_dumper = Int2BinaryDumper(Int2)
    _int4_dumper = Int4BinaryDumper(Int4)
    _int8_dumper = Int8BinaryDumper(Int8)
    _int_numeric_dumper = IntNumericBinaryDumper(IntNumeric)


class IntLoader(Loader):

    format = Format.TEXT

    def load(self, data: Buffer) -> int:
        # it supports bytes directly
        return int(data)


class Int2BinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_int2(data)[0]


class Int4BinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_int4(data)[0]


class Int8BinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_int8(data)[0]


class OidBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> int:
        return unpack_uint4(data)[0]


class FloatLoader(Loader):

    format = Format.TEXT

    def load(self, data: Buffer) -> float:
        # it supports bytes directly
        return float(data)


class Float4BinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> float:
        return unpack_float4(data)[0]


class Float8BinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> float:
        return unpack_float8(data)[0]


class NumericLoader(Loader):

    format = Format.TEXT

    def load(self, data: Buffer) -> Decimal:
        if isinstance(data, memoryview):
            data = bytes(data)
        return Decimal(data.decode("utf8"))


DEC_DIGITS = 4  # decimal digits per Postgres "digit"
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_NAN = 0xC000
NUMERIC_PINF = 0xD000
NUMERIC_NINF = 0xF000

_decimal_special = {
    NUMERIC_NAN: Decimal("NaN"),
    NUMERIC_PINF: Decimal("Infinity"),
    NUMERIC_NINF: Decimal("-Infinity"),
}


class _ContextMap(DefaultDict[int, Context]):
    """
    Cache for decimal contexts to use when the precision requires it.

    Note: if the default context is used (prec=28) you can get an invalid
    operation or a rounding to 0:

    - Decimal(1000).shift(24) = Decimal('1000000000000000000000000000')
    - Decimal(1000).shift(25) = Decimal('0')
    - Decimal(1000).shift(30) raises InvalidOperation
    """

    def __missing__(self, key: int) -> Context:
        val = Context(prec=key)
        self[key] = val
        return val


_contexts = _ContextMap()
for i in range(DefaultContext.prec):
    _contexts[i] = DefaultContext

_unpack_numeric_head = cast(
    Callable[[bytes], Tuple[int, int, int, int]],
    struct.Struct("!HhHH").unpack_from,
)
_pack_numeric_head = cast(
    Callable[[int, int, int, int], bytes],
    struct.Struct("!HhHH").pack,
)


class NumericBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> Decimal:
        ndigits, weight, sign, dscale = _unpack_numeric_head(data)
        if sign == NUMERIC_POS or sign == NUMERIC_NEG:
            val = 0
            for i in range(8, len(data), 2):
                val = val * 10_000 + data[i] * 0x100 + data[i + 1]

            shift = dscale - (ndigits - weight - 1) * DEC_DIGITS
            ctx = _contexts[(weight + 2) * DEC_DIGITS + dscale]
            return (
                Decimal(val if sign == NUMERIC_POS else -val)
                .scaleb(-dscale, ctx)
                .shift(shift, ctx)
            )
        else:
            try:
                return _decimal_special[sign]
            except KeyError:
                raise e.DataError(
                    f"bad value for numeric sign: 0x{sign:X}"
                ) from None


NUMERIC_NAN_BIN = _pack_numeric_head(0, 0, NUMERIC_NAN, 0)
NUMERIC_PINF_BIN = _pack_numeric_head(0, 0, NUMERIC_PINF, 0)
NUMERIC_NINF_BIN = _pack_numeric_head(0, 0, NUMERIC_NINF, 0)


class DecimalBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = builtins["numeric"].oid

    def dump(self, obj: Decimal) -> Union[bytearray, bytes]:
        sign, digits, exp = obj.as_tuple()
        if exp == "n" or exp == "N":  # type: ignore[comparison-overlap]
            return NUMERIC_NAN_BIN
        elif exp == "F":  # type: ignore[comparison-overlap]
            return NUMERIC_NINF_BIN if sign else NUMERIC_PINF_BIN

        # Weights of py digits into a pg digit according to their positions.
        # Starting with an index wi != 0 is equivalent to prepending 0's to
        # the digits tuple, but without really changing it.
        weights = (1000, 100, 10, 1)
        wi = 0

        ndigits = nzdigits = len(digits)

        # Find the last nonzero digit
        while nzdigits > 0 and digits[nzdigits - 1] == 0:
            nzdigits -= 1

        if exp <= 0:
            dscale = -exp
        else:
            dscale = 0
            # align the py digits to the pg digits if there's some py exponent
            ndigits += exp % DEC_DIGITS

        if not nzdigits:
            return _pack_numeric_head(0, 0, NUMERIC_POS, dscale)

        # Equivalent of 0-padding left to align the py digits to the pg digits
        # but without changing the digits tuple.
        mod = (ndigits - dscale) % DEC_DIGITS
        if mod:
            wi = DEC_DIGITS - mod
            ndigits += wi

        tmp = nzdigits + wi
        out = bytearray(
            _pack_numeric_head(
                tmp // DEC_DIGITS + (tmp % DEC_DIGITS and 1),  # ndigits
                (ndigits + exp) // DEC_DIGITS - 1,  # weight
                NUMERIC_NEG if sign else NUMERIC_POS,  # sign
                dscale,
            )
        )

        pgdigit = 0
        for i in range(nzdigits):
            pgdigit += weights[wi] * digits[i]
            wi += 1
            if wi >= DEC_DIGITS:
                out += pack_uint2(pgdigit)
                pgdigit = wi = 0

        if pgdigit:
            out += pack_uint2(pgdigit)

        return out


def register_default_globals(ctx: AdaptContext) -> None:
    IntDumper.register(int, ctx)
    IntBinaryDumper.register(int, ctx)
    FloatDumper.register(float, ctx)
    FloatBinaryDumper.register(float, ctx)
    # The binary dumper is currently some 30% slower, so default to text
    # (see tests/scripts/testdec.py for a rough benchmark)
    DecimalBinaryDumper.register("decimal.Decimal", ctx)
    DecimalDumper.register("decimal.Decimal", ctx)
    Int2Dumper.register(Int2, ctx)
    Int4Dumper.register(Int4, ctx)
    Int8Dumper.register(Int8, ctx)
    IntNumericDumper.register(IntNumeric, ctx)
    OidDumper.register(Oid, ctx)
    Int2BinaryDumper.register(Int2, ctx)
    Int4BinaryDumper.register(Int4, ctx)
    Int8BinaryDumper.register(Int8, ctx)
    OidBinaryDumper.register(Oid, ctx)
    IntLoader.register("int2", ctx)
    IntLoader.register("int4", ctx)
    IntLoader.register("int8", ctx)
    IntLoader.register("oid", ctx)
    Int2BinaryLoader.register("int2", ctx)
    Int4BinaryLoader.register("int4", ctx)
    Int8BinaryLoader.register("int8", ctx)
    OidBinaryLoader.register("oid", ctx)
    FloatLoader.register("float4", ctx)
    FloatLoader.register("float8", ctx)
    Float4BinaryLoader.register("float4", ctx)
    Float8BinaryLoader.register("float8", ctx)
    NumericLoader.register("numeric", ctx)
    NumericBinaryLoader.register("numeric", ctx)
