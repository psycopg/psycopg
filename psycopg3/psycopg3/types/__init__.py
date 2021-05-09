"""
psycopg3 types package
"""

# Copyright (C) 2020-2021 The Psycopg Team

from ..oids import INVALID_OID
from ..proto import AdaptContext

# Register default adapters
from . import array, composite, range

# Wrapper objects
from ..wrappers.numeric import (
    Int2 as Int2,
    Int4 as Int4,
    Int8 as Int8,
    IntNumeric as IntNumeric,
    Oid as Oid,
)
from .json import Json as Json, Jsonb as Jsonb
from .range import Range as Range

# Database types descriptors
from .._typeinfo import (
    TypeInfo as TypeInfo,
    RangeInfo as RangeInfo,
    CompositeInfo as CompositeInfo,
)

# Json global registrations
from .json import (
    set_json_dumps as set_json_dumps,
    set_json_loads as set_json_loads,
)

# Adapter objects
from .text import (
    StringDumper as StringDumper,
    StringBinaryDumper as StringBinaryDumper,
    TextLoader as TextLoader,
    TextBinaryLoader as TextBinaryLoader,
    BytesDumper as BytesDumper,
    BytesBinaryDumper as BytesBinaryDumper,
    ByteaLoader as ByteaLoader,
    ByteaBinaryLoader as ByteaBinaryLoader,
)
from .numeric import (
    IntDumper as IntDumper,
    IntBinaryDumper as IntBinaryDumper,
    FloatDumper as FloatDumper,
    FloatBinaryDumper as FloatBinaryDumper,
    DecimalDumper as DecimalDumper,
    DecimalBinaryDumper as DecimalBinaryDumper,
    Int2Dumper as Int2Dumper,
    Int4Dumper as Int4Dumper,
    Int8Dumper as Int8Dumper,
    IntNumericDumper as IntNumericDumper,
    OidDumper as OidDumper,
    Int2BinaryDumper as Int2BinaryDumper,
    Int4BinaryDumper as Int4BinaryDumper,
    Int8BinaryDumper as Int8BinaryDumper,
    OidBinaryDumper as OidBinaryDumper,
    IntLoader as IntLoader,
    Int2BinaryLoader as Int2BinaryLoader,
    Int4BinaryLoader as Int4BinaryLoader,
    Int8BinaryLoader as Int8BinaryLoader,
    OidBinaryLoader as OidBinaryLoader,
    FloatLoader as FloatLoader,
    Float4BinaryLoader as Float4BinaryLoader,
    Float8BinaryLoader as Float8BinaryLoader,
    NumericLoader as NumericLoader,
    NumericBinaryLoader as NumericBinaryLoader,
)
from .singletons import (
    BoolDumper as BoolDumper,
    BoolBinaryDumper as BoolBinaryDumper,
    NoneDumper as NoneDumper,
    BoolLoader as BoolLoader,
    BoolBinaryLoader as BoolBinaryLoader,
)
from .date import (
    DateDumper as DateDumper,
    TimeDumper as TimeDumper,
    TimeTzDumper as TimeTzDumper,
    DateTimeTzDumper as DateTimeTzDumper,
    DateTimeDumper as DateTimeDumper,
    TimeDeltaDumper as TimeDeltaDumper,
    DateLoader as DateLoader,
    TimeLoader as TimeLoader,
    TimeTzLoader as TimeTzLoader,
    TimestampLoader as TimestampLoader,
    TimestamptzLoader as TimestamptzLoader,
    IntervalLoader as IntervalLoader,
)
from .json import (
    JsonDumper as JsonDumper,
    JsonBinaryDumper as JsonBinaryDumper,
    JsonbDumper as JsonbDumper,
    JsonbBinaryDumper as JsonbBinaryDumper,
    JsonLoader as JsonLoader,
    JsonBinaryLoader as JsonBinaryLoader,
    JsonbLoader as JsonbLoader,
    JsonbBinaryLoader as JsonbBinaryLoader,
)
from .uuid import (
    UUIDDumper as UUIDDumper,
    UUIDBinaryDumper as UUIDBinaryDumper,
    UUIDLoader as UUIDLoader,
    UUIDBinaryLoader as UUIDBinaryLoader,
)
from .network import (
    InterfaceDumper as InterfaceDumper,
    NetworkDumper as NetworkDumper,
    InetLoader as InetLoader,
    CidrLoader as CidrLoader,
)
from .range import (
    RangeDumper as RangeDumper,
    RangeLoader as RangeLoader,
    Int4RangeLoader as Int4RangeLoader,
    Int8RangeLoader as Int8RangeLoader,
    NumericRangeLoader as NumericRangeLoader,
    DateRangeLoader as DateRangeLoader,
    TimestampRangeLoader as TimestampRangeLoader,
    TimestampTZRangeLoader as TimestampTZRangeLoader,
)
from .array import (
    ListDumper as ListDumper,
    ListBinaryDumper as ListBinaryDumper,
)
from .composite import (
    TupleDumper as TupleDumper,
    RecordLoader as RecordLoader,
    RecordBinaryLoader as RecordBinaryLoader,
    CompositeLoader as CompositeLoader,
    CompositeBinaryLoader as CompositeBinaryLoader,
)


def register_default_globals(ctx: AdaptContext) -> None:
    # NOTE: the order the dumpers are registered is relevant.
    # The last one registered becomes the default for each type.
    # Normally, binary is the default dumper, except for text (which plays
    # the role of unknown, so it can be cast automatically to other types).
    StringBinaryDumper.register(str, ctx)
    StringDumper.register(str, ctx)
    TextLoader.register(INVALID_OID, ctx)
    TextLoader.register("bpchar", ctx)
    TextLoader.register("name", ctx)
    TextLoader.register("text", ctx)
    TextLoader.register("varchar", ctx)
    TextBinaryLoader.register("bpchar", ctx)
    TextBinaryLoader.register("name", ctx)
    TextBinaryLoader.register("text", ctx)
    TextBinaryLoader.register("varchar", ctx)

    BytesDumper.register(bytes, ctx)
    BytesDumper.register(bytearray, ctx)
    BytesDumper.register(memoryview, ctx)
    BytesBinaryDumper.register(bytes, ctx)
    BytesBinaryDumper.register(bytearray, ctx)
    BytesBinaryDumper.register(memoryview, ctx)
    ByteaLoader.register("bytea", ctx)
    ByteaBinaryLoader.register(INVALID_OID, ctx)
    ByteaBinaryLoader.register("bytea", ctx)

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

    BoolDumper.register(bool, ctx)
    BoolBinaryDumper.register(bool, ctx)
    NoneDumper.register(type(None), ctx)
    BoolLoader.register("bool", ctx)
    BoolBinaryLoader.register("bool", ctx)

    DateDumper.register("datetime.date", ctx)
    TimeDumper.register("datetime.time", ctx)
    DateTimeTzDumper.register("datetime.datetime", ctx)
    TimeDeltaDumper.register("datetime.timedelta", ctx)
    DateLoader.register("date", ctx)
    TimeLoader.register("time", ctx)
    TimeTzLoader.register("timetz", ctx)
    TimestampLoader.register("timestamp", ctx)
    TimestamptzLoader.register("timestamptz", ctx)
    IntervalLoader.register("interval", ctx)

    # Currently json binary format is nothing different than text, maybe with
    # an extra memcopy we can avoid.
    JsonBinaryDumper.register(Json, ctx)
    JsonDumper.register(Json, ctx)
    JsonbBinaryDumper.register(Jsonb, ctx)
    JsonbDumper.register(Jsonb, ctx)
    JsonLoader.register("json", ctx)
    JsonbLoader.register("jsonb", ctx)
    JsonBinaryLoader.register("json", ctx)
    JsonbBinaryLoader.register("jsonb", ctx)

    UUIDDumper.register("uuid.UUID", ctx)
    UUIDBinaryDumper.register("uuid.UUID", ctx)
    UUIDLoader.register("uuid", ctx)
    UUIDBinaryLoader.register("uuid", ctx)

    InterfaceDumper.register("ipaddress.IPv4Address", ctx)
    InterfaceDumper.register("ipaddress.IPv6Address", ctx)
    InterfaceDumper.register("ipaddress.IPv4Interface", ctx)
    InterfaceDumper.register("ipaddress.IPv6Interface", ctx)
    NetworkDumper.register("ipaddress.IPv4Network", ctx)
    NetworkDumper.register("ipaddress.IPv6Network", ctx)
    InetLoader.register("inet", ctx)
    CidrLoader.register("cidr", ctx)

    RangeDumper.register(Range, ctx)
    Int4RangeLoader.register("int4range", ctx)
    Int8RangeLoader.register("int8range", ctx)
    NumericRangeLoader.register("numrange", ctx)
    DateRangeLoader.register("daterange", ctx)
    TimestampRangeLoader.register("tsrange", ctx)
    TimestampTZRangeLoader.register("tstzrange", ctx)

    ListDumper.register(list, ctx)
    ListBinaryDumper.register(list, ctx)

    TupleDumper.register(tuple, ctx)
    RecordLoader.register("record", ctx)
    RecordBinaryLoader.register("record", ctx)

    array.register_all_arrays(ctx)
