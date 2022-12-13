"""
Adapters for the enum type.
"""
from enum import Enum
from typing import Any, Dict, Generic, Optional, Mapping, Sequence
from typing import Tuple, Type, TypeVar, Union, cast
from typing_extensions import TypeAlias

from .. import postgres
from .. import errors as e
from ..pq import Format
from ..abc import AdaptContext
from ..adapt import Buffer, Dumper, Loader
from .._encodings import conn_encoding
from .._typeinfo import EnumInfo as EnumInfo  # exported here

E = TypeVar("E", bound=Enum)

EnumDumpMap: TypeAlias = Dict[E, bytes]
EnumLoadMap: TypeAlias = Dict[bytes, E]
EnumMapping: TypeAlias = Union[Mapping[E, str], Sequence[Tuple[E, str]], None]


class _BaseEnumLoader(Loader, Generic[E]):
    """
    Loader for a specific Enum class
    """

    enum: Type[E]
    _load_map: EnumLoadMap[E]

    def load(self, data: Buffer) -> E:
        if not isinstance(data, bytes):
            data = bytes(data)

        try:
            return self._load_map[data]
        except KeyError:
            enc = conn_encoding(self.connection)
            label = data.decode(enc, "replace")
            raise e.DataError(
                f"bad member for enum {self.enum.__qualname__}: {label!r}"
            )


class _BaseEnumDumper(Dumper, Generic[E]):
    """
    Dumper for a specific Enum class
    """

    enum: Type[E]
    _dump_map: EnumDumpMap[E]

    def dump(self, value: E) -> Buffer:
        return self._dump_map[value]


class EnumDumper(Dumper):
    """
    Dumper for a generic Enum class
    """

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._encoding = conn_encoding(self.connection)

    def dump(self, value: E) -> Buffer:
        return value.name.encode(self._encoding)


class EnumBinaryDumper(EnumDumper):
    format = Format.BINARY


def register_enum(
    info: EnumInfo,
    context: Optional[AdaptContext] = None,
    enum: Optional[Type[E]] = None,
    *,
    mapping: EnumMapping[E] = None,
) -> None:
    """Register the adapters to load and dump a enum type.

    :param info: The object with the information about the enum to register.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.
    :param enum: Python enum type matching to the PostgreSQL one. If `!None`,
        a new enum will be generated and exposed as `EnumInfo.enum`.
    :param mapping: Override the mapping between `!enum` members and `!info`
        labels.
    """

    if not info:
        raise TypeError("no info passed. Is the requested enum available?")

    if enum is None:
        enum = cast(Type[E], Enum(info.name.title(), info.labels, module=__name__))

    info.enum = enum
    adapters = context.adapters if context else postgres.adapters
    info.register(context)

    load_map = _make_load_map(info, enum, mapping, context)
    attribs: Dict[str, Any] = {"enum": info.enum, "_load_map": load_map}

    name = f"{info.name.title()}Loader"
    loader = type(name, (_BaseEnumLoader,), attribs)
    adapters.register_loader(info.oid, loader)

    name = f"{info.name.title()}BinaryLoader"
    loader = type(name, (_BaseEnumLoader,), {**attribs, "format": Format.BINARY})
    adapters.register_loader(info.oid, loader)

    dump_map = _make_dump_map(info, enum, mapping, context)
    attribs = {"oid": info.oid, "enum": info.enum, "_dump_map": dump_map}

    name = f"{enum.__name__}Dumper"
    dumper = type(name, (_BaseEnumDumper,), attribs)
    adapters.register_dumper(info.enum, dumper)

    name = f"{enum.__name__}BinaryDumper"
    dumper = type(name, (_BaseEnumDumper,), {**attribs, "format": Format.BINARY})
    adapters.register_dumper(info.enum, dumper)


def _make_load_map(
    info: EnumInfo,
    enum: Type[E],
    mapping: EnumMapping[E],
    context: Optional[AdaptContext],
) -> EnumLoadMap[E]:
    enc = conn_encoding(context.connection if context else None)
    rv: EnumLoadMap[E] = {}
    for label in info.labels:
        try:
            member = enum[label]
        except KeyError:
            # tolerate a missing enum, assuming it won't be used. If it is we
            # will get a DataError on fetch.
            pass
        else:
            rv[label.encode(enc)] = member

    if mapping:
        if isinstance(mapping, Mapping):
            mapping = list(mapping.items())

        for member, label in mapping:
            rv[label.encode(enc)] = member

    return rv


def _make_dump_map(
    info: EnumInfo,
    enum: Type[E],
    mapping: EnumMapping[E],
    context: Optional[AdaptContext],
) -> EnumDumpMap[E]:
    enc = conn_encoding(context.connection if context else None)
    rv: EnumDumpMap[E] = {}
    for member in enum:
        rv[member] = member.name.encode(enc)

    if mapping:
        if isinstance(mapping, Mapping):
            mapping = list(mapping.items())

        for member, label in mapping:
            rv[member] = label.encode(enc)

    return rv


def register_default_adapters(context: AdaptContext) -> None:
    context.adapters.register_dumper(Enum, EnumBinaryDumper)
    context.adapters.register_dumper(Enum, EnumDumper)
