"""
Support for composite types adaptation.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import struct
from collections import namedtuple
from typing import Any, Callable, cast, Iterator, List, Optional
from typing import Sequence, Tuple, Type

from .. import pq
from .. import postgres
from ..abc import AdaptContext, Buffer
from ..adapt import PyFormat, RecursiveDumper, RecursiveLoader
from .._struct import pack_len, unpack_len
from ..postgres import TEXT_OID
from .._typeinfo import CompositeInfo as CompositeInfo  # exported here

_struct_oidlen = struct.Struct("!Ii")
_pack_oidlen = cast(Callable[[int, int], bytes], _struct_oidlen.pack)
_unpack_oidlen = cast(
    Callable[[bytes, int], Tuple[int, int]], _struct_oidlen.unpack_from
)


class SequenceDumper(RecursiveDumper):
    def _dump_sequence(
        self, obj: Sequence[Any], start: bytes, end: bytes, sep: bytes
    ) -> bytes:
        if not obj:
            return start + end

        parts = [start]

        for item in obj:
            if item is None:
                parts.append(sep)
                continue

            dumper = self._tx.get_dumper(item, PyFormat.from_pq(self.format))
            ad = dumper.dump(item)
            if not ad:
                ad = b'""'
            elif self._re_needs_quotes.search(ad):
                ad = b'"' + self._re_esc.sub(br"\1\1", ad) + b'"'

            parts.append(ad)
            parts.append(sep)

        parts[-1] = end

        return b"".join(parts)

    _re_needs_quotes = re.compile(br'[",\\\s()]')
    _re_esc = re.compile(br"([\\\"])")


class TupleDumper(SequenceDumper):

    # Should be this, but it doesn't work
    # oid = postgres_types["record"].oid

    def dump(self, obj: Tuple[Any, ...]) -> bytes:
        return self._dump_sequence(obj, b"(", b")", b",")


class TupleBinaryDumper(RecursiveDumper):

    format = pq.Format.BINARY

    # Subclasses must set an info
    info: CompositeInfo

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        nfields = len(self.info.field_types)
        self._tx.set_dumper_types(self.info.field_types, self.format)
        self._formats = (PyFormat.from_pq(self.format),) * nfields

    def dump(self, obj: Tuple[Any, ...]) -> bytearray:
        out = bytearray(pack_len(len(obj)))
        adapted = self._tx.dump_sequence(obj, self._formats)
        for i in range(len(obj)):
            b = adapted[i]
            oid = self.info.field_types[i]
            if b is not None:
                out += _pack_oidlen(oid, len(b))
                out += b
            else:
                out += _pack_oidlen(oid, -1)

        return out


class BaseCompositeLoader(RecursiveLoader):
    def _parse_record(self, data: bytes) -> Iterator[Optional[bytes]]:
        """
        Split a non-empty representation of a composite type into components.

        Terminators shouldn't be used in *data* (so that both record and range
        representations can be parsed).
        """
        for m in self._re_tokenize.finditer(data):
            if m.group(1):
                yield None
            elif m.group(2) is not None:
                yield self._re_undouble.sub(br"\1", m.group(2))
            else:
                yield m.group(3)

        # If the final group ended in `,` there is a final NULL in the record
        # that the regexp couldn't parse.
        if m and m.group().endswith(b","):
            yield None

    _re_tokenize = re.compile(
        br"""(?x)
          (,)                       # an empty token, representing NULL
        | " ((?: [^"] | "")*) " ,?  # or a quoted string
        | ([^",)]+) ,?              # or an unquoted string
        """
    )

    _re_undouble = re.compile(br'(["\\])\1')


class RecordLoader(BaseCompositeLoader):
    def load(self, data: Buffer) -> Tuple[Any, ...]:
        if data == b"()":
            return ()

        cast = self._tx.get_loader(TEXT_OID, self.format).load
        return tuple(
            cast(token) if token is not None else None
            for token in self._parse_record(data[1:-1])
        )


class RecordBinaryLoader(RecursiveLoader):

    format = pq.Format.BINARY
    _types_set = False

    def load(self, data: Buffer) -> Tuple[Any, ...]:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        return self._tx.load_sequence(
            tuple(
                data[offset : offset + length] if length != -1 else None
                for _, offset, length in self._walk_record(data)
            )
        )

    def _walk_record(self, data: bytes) -> Iterator[Tuple[int, int, int]]:
        """
        Yield a sequence of (oid, offset, length) for the content of the record
        """
        nfields = unpack_len(data, 0)[0]
        i = 4
        for _ in range(nfields):
            oid, length = _unpack_oidlen(data, i)
            yield oid, i + 8, length
            i += (8 + length) if length > 0 else 8

    def _config_types(self, data: bytes) -> None:
        oids = [r[0] for r in self._walk_record(data)]
        self._tx.set_loader_types(oids, self.format)


class CompositeLoader(RecordLoader):

    factory: Callable[..., Any]
    fields_types: List[int]
    _types_set = False

    def load(self, data: Buffer) -> Any:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        if data == b"()":
            return type(self).factory()

        return type(self).factory(
            *self._tx.load_sequence(tuple(self._parse_record(data[1:-1])))
        )

    def _config_types(self, data: bytes) -> None:
        self._tx.set_loader_types(self.fields_types, self.format)


class CompositeBinaryLoader(RecordBinaryLoader):

    format = pq.Format.BINARY
    factory: Callable[..., Any]

    def load(self, data: Buffer) -> Any:
        r = super().load(data)
        return type(self).factory(*r)


def register_composite(
    info: CompositeInfo,
    context: Optional[AdaptContext] = None,
    factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Register the adapters to load and dump a composite type.

    :param info: The object with the information about the composite to register.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.
    :param factory: Callable to convert the sequence of attributes read from
        the composite into a Python object.

    .. note::

        Registering the adapters doesn't affect objects already created, even
        if they are children of the registered context. For instance,
        registering the adapter globally doesn't affect already existing
        connections.
    """

    # A friendly error warning instead of an AttributeError in case fetch()
    # failed and it wasn't noticed.
    if not info:
        raise TypeError(
            "no info passed. Is the requested composite available?"
        )

    # Register arrays and type info
    info.register(context)

    if not factory:
        factory = namedtuple(info.name, info.field_names)  # type: ignore

    adapters = context.adapters if context else postgres.adapters

    # generate and register a customized text loader
    loader: Type[BaseCompositeLoader] = type(
        f"{info.name.title()}Loader",
        (CompositeLoader,),
        {
            "factory": factory,
            "fields_types": info.field_types,
        },
    )
    adapters.register_loader(info.oid, loader)

    # generate and register a customized binary loader
    loader = type(
        f"{info.name.title()}BinaryLoader",
        (CompositeBinaryLoader,),
        {"factory": factory},
    )
    adapters.register_loader(info.oid, loader)

    # If the factory is a type, create and register dumpers for it
    if isinstance(factory, type):
        dumper = type(
            f"{info.name.title()}BinaryDumper",
            (TupleBinaryDumper,),
            {"oid": info.oid, "info": info},
        )
        adapters.register_dumper(factory, dumper)

        # Default to the text dumper because it is more flexible
        dumper = type(
            f"{info.name.title()}Dumper", (TupleDumper,), {"oid": info.oid}
        )
        adapters.register_dumper(factory, dumper)

        info.python_type = factory


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters
    adapters.register_dumper(tuple, TupleDumper)
    adapters.register_loader("record", RecordLoader)
    adapters.register_loader("record", RecordBinaryLoader)
