"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Callable, Optional, Type

from . import pq
from . import proto
from .pq import Format as Format
from .types import builtins
from .proto import AdaptContext, DumpersMap, DumperType, LoadersMap, LoaderType
from .cursor import BaseCursor
from .connection import BaseConnection

TEXT_OID = builtins["text"].oid


class Dumper:
    globals: DumpersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, src: type, context: AdaptContext = None):
        self.src = src
        self.context = context
        self.connection = _connection_from_context(context)

    def dump(self, obj: Any) -> bytes:
        raise NotImplementedError()

    def quote(self, obj: Any) -> bytes:
        value = self.dump(obj)

        if self.connection:
            esc = pq.Escaping(self.connection.pgconn)
            return esc.escape_literal(value)
        else:
            esc = pq.Escaping()
            return b"'%s'" % esc.escape_string(value)

    @property
    def oid(self) -> int:
        return TEXT_OID

    @classmethod
    def register(
        cls,
        src: type,
        dumper: DumperType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> DumperType:
        if not isinstance(src, type):
            raise TypeError(
                f"dumpers should be registered on classes, got {src} instead"
            )

        if not (isinstance(dumper, type)):
            raise TypeError(f"dumpers should be classes, got {dumper} instead")

        where = context.dumpers if context is not None else Dumper.globals
        where[src, format] = dumper
        return dumper

    @classmethod
    def register_binary(
        cls, src: type, dumper: DumperType, context: AdaptContext = None
    ) -> DumperType:
        return cls.register(src, dumper, context, format=Format.BINARY)

    @classmethod
    def text(cls, src: type) -> Callable[[DumperType], DumperType]:
        def text_(dumper: DumperType) -> DumperType:
            cls.register(src, dumper)
            return dumper

        return text_

    @classmethod
    def binary(cls, src: type) -> Callable[[DumperType], DumperType]:
        def binary_(dumper: DumperType) -> DumperType:
            cls.register_binary(src, dumper)
            return dumper

        return binary_


class Loader:
    globals: LoadersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, oid: int, context: AdaptContext = None):
        self.oid = oid
        self.context = context
        self.connection = _connection_from_context(context)

    def load(self, data: bytes) -> Any:
        raise NotImplementedError()

    @classmethod
    def register(
        cls,
        oid: int,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> None:
        if not isinstance(oid, int):
            raise TypeError(
                f"loaders should be registered on oid, got {oid} instead"
            )

        where = context.loaders if context is not None else Loader.globals
        where[oid, format] = cls

    @classmethod
    def register_binary(cls, oid: int, context: AdaptContext = None) -> None:
        cls.register(oid, context, format=Format.BINARY)

    @classmethod
    def text(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def text_(loader: LoaderType) -> LoaderType:
            loader.register(oid)
            return loader

        return text_

    @classmethod
    def binary(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def binary_(loader: LoaderType) -> LoaderType:
            loader.register_binary(oid)
            return loader

        return binary_


def _connection_from_context(
    context: AdaptContext,
) -> Optional[BaseConnection]:
    if context is None:
        return None
    elif isinstance(context, BaseConnection):
        return context
    elif isinstance(context, BaseCursor):
        return context.connection
    elif isinstance(context, Transformer):
        return context.connection
    else:
        raise TypeError(f"can't get a connection from {type(context)}")


Transformer: Type[proto.Transformer]

# Override it with fast object if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    Transformer = _psycopg3.Transformer
else:
    from . import _transform

    Transformer = _transform.Transformer
