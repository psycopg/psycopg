"""
Adapters for the enum type.
"""
from enum import Enum
from typing import Type, Any, Dict, Generic, Optional, TypeVar, cast

from .. import postgres
from .. import errors as e
from ..pq import Format
from ..abc import AdaptContext
from ..adapt import Buffer, Dumper, Loader
from .._typeinfo import EnumInfo as EnumInfo  # exported here
from .._encodings import pgconn_encoding


E = TypeVar("E", bound=Enum)


class EnumLoader(Loader, Generic[E]):
    _encoding = "utf-8"
    enum: Type[E]

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        conn = self.connection
        if conn:
            self._encoding = pgconn_encoding(conn.pgconn)

    def load(self, data: Buffer) -> E:
        if isinstance(data, memoryview):
            label = bytes(data).decode(self._encoding)
        else:
            label = data.decode(self._encoding)

        try:
            return self.enum[label]
        except KeyError:
            raise e.DataError(
                f"bad memeber for enum {self.enum.__qualname__}: {label!r}"
            )


class EnumBinaryLoader(EnumLoader[E]):
    format = Format.BINARY


class EnumDumper(Dumper):
    _encoding = "utf-8"

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)

        conn = self.connection
        if conn:
            self._encoding = pgconn_encoding(conn.pgconn)

    def dump(self, value: E) -> Buffer:
        return value.name.encode(self._encoding)


class EnumBinaryDumper(EnumDumper):
    format = Format.BINARY


def register_enum(
    info: EnumInfo,
    context: Optional[AdaptContext] = None,
    enum: Optional[Type[E]] = None,
) -> None:
    """Register the adapters to load and dump a enum type.

    :param info: The object with the information about the enum to register.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.
    :param enum: Python enum type matching to the PostgreSQL one. If `!None`,
        a new enum will be generated and exposed as `EnumInfo.enum`.
    """

    if not info:
        raise TypeError("no info passed. Is the requested enum available?")

    if enum is None:
        enum = cast(Type[E], Enum(info.name.title(), info.labels, module=__name__))

    info.enum = enum
    adapters = context.adapters if context else postgres.adapters
    info.register(context)

    attribs: Dict[str, Any] = {"enum": info.enum}

    loader_base = EnumLoader
    name = f"{info.name.title()}Loader"
    loader = type(name, (loader_base,), attribs)
    adapters.register_loader(info.oid, loader)

    loader_base = EnumBinaryLoader
    name = f"{info.name.title()}BinaryLoader"
    loader = type(name, (loader_base,), attribs)
    adapters.register_loader(info.oid, loader)

    attribs = {"oid": info.oid}

    name = f"{enum.__name__}BinaryDumper"
    dumper = type(name, (EnumBinaryDumper,), attribs)
    adapters.register_dumper(info.enum, dumper)

    name = f"{enum.__name__}Dumper"
    dumper = type(name, (EnumDumper,), attribs)
    adapters.register_dumper(info.enum, dumper)


def register_default_adapters(context: AdaptContext) -> None:
    context.adapters.register_dumper(Enum, EnumBinaryDumper)
    context.adapters.register_dumper(Enum, EnumDumper)
