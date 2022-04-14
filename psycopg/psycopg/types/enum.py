"""
Adapters for the enum type.
"""
from enum import Enum
from typing import Optional, TypeVar, Generic, Type, Dict, Any

from ..adapt import Dumper, Loader
from .string import StrBinaryDumper, StrDumper
from .. import postgres
from .._encodings import pgconn_encoding
from .._typeinfo import EnumInfo as EnumInfo  # exported here
from ..abc import AdaptContext
from ..adapt import Buffer
from ..pq import Format


E = TypeVar("E", bound=Enum)


class EnumLoader(Loader, Generic[E]):
    format = Format.TEXT
    _encoding = "utf-8"
    python_type: Type[E]

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

        return self.python_type(label)


class EnumBinaryLoader(EnumLoader[E]):
    format = Format.BINARY


class EnumDumper(StrDumper):
    pass


class EnumBinaryDumper(StrBinaryDumper):
    pass


def register_enum(
    info: EnumInfo,
    python_type: Optional[Type[E]] = None,
    context: Optional[AdaptContext] = None,
) -> None:
    """Register the adapters to load and dump a enum type.

    :param info: The object with the information about the enum to register.
    :param python_type: Python enum type matching to the Postgres one. If `!None`,
        the type will be generated and put into info.python_type.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.

    .. note::
        Only string enums are supported.

        Use binary format if you use enum array and enum labels contains comma:
            connection.execute(..., binary=True)
    """

    if not info:
        raise TypeError("no info passed. Is the requested enum available?")

    if python_type is not None:
        if {type(item.value) for item in python_type} != {str}:
            raise TypeError("invalid enum value type (string is the only supported)")

        info.python_type = python_type
    else:
        info.python_type = Enum(  # type: ignore
            info.name.title(),
            {label: label for label in info.enum_labels},
        )

    adapters = context.adapters if context else postgres.adapters

    info.register(context)

    attribs: Dict[str, Any] = {"python_type": info.python_type}

    loader_base = EnumLoader
    name = f"{info.name.title()}{loader_base.__name__}"
    loader = type(name, (loader_base,), attribs)
    adapters.register_loader(info.oid, loader)

    loader_base = EnumBinaryLoader
    name = f"{info.name.title()}{loader_base.__name__}"
    loader = type(name, (loader_base,), attribs)
    adapters.register_loader(info.oid, loader)

    attribs = {"oid": info.oid}

    dumper_base: Type[Dumper] = EnumBinaryDumper
    name = f"{info.name.title()}{dumper_base.__name__}"
    dumper = type(name, (dumper_base,), attribs)
    adapters.register_dumper(info.python_type, dumper)

    dumper_base = EnumDumper
    name = f"{info.name.title()}{dumper_base.__name__}"
    dumper = type(name, (dumper_base,), attribs)
    adapters.register_dumper(info.python_type, dumper)


def register_default_adapters(context: AdaptContext) -> None:
    context.adapters.register_dumper(Enum, EnumBinaryDumper)
    context.adapters.register_dumper(Enum, EnumDumper)
