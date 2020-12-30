"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Type, TYPE_CHECKING, Union

from . import pq
from . import proto
from .pq import Format as Format
from .oids import TEXT_OID
from .proto import DumpersMap, DumperType, LoadersMap, LoaderType, AdaptContext

if TYPE_CHECKING:
    from .connection import BaseConnection


class Dumper(ABC):
    """
    Convert Python object of the type *src* to PostgreSQL representation.
    """

    format: Format
    connection: Optional["BaseConnection"] = None

    # A class-wide oid, which will be used by default by instances unless
    # the subclass overrides it in init.
    _oid: int = 0

    def __init__(self, src: type, context: Optional[AdaptContext] = None):
        self.src = src
        self.connection = context.connection if context else None

        self.oid = self._oid
        """The oid to pass to the server, if known."""

        # Postgres 9.6 doesn't deal well with unknown oids
        if (
            not self.oid
            and self.connection
            and self.connection.pgconn.server_version < 100000
        ):
            self.oid = TEXT_OID

    @abstractmethod
    def dump(self, obj: Any) -> bytes:
        """Convert the object *obj* to PostgreSQL representation."""
        ...

    # TODO: the protocol signature should probably return a Buffer like object
    # (the C implementation may return bytearray)
    def quote(self, obj: Any) -> bytes:
        """Convert the object *obj* to escaped representation."""
        value = self.dump(obj)

        if self.connection:
            esc = pq.Escaping(self.connection.pgconn)
            return esc.escape_literal(value)
        else:
            esc = pq.Escaping()
            return b"'%s'" % esc.escape_string(value)

    @classmethod
    def register(
        cls, src: Union[type, str], context: Optional[AdaptContext] = None
    ) -> None:
        """
        Configure *context* to use this dumper to convert object of type *src*.
        """
        adapters = context.adapters if context else global_adapters
        adapters.register_dumper(src, cls)

    @classmethod
    def text(cls, src: Union[type, str]) -> Callable[[DumperType], DumperType]:
        def text_(dumper: DumperType) -> DumperType:
            assert dumper.format == Format.TEXT
            dumper.register(src)
            return dumper

        return text_

    @classmethod
    def binary(
        cls, src: Union[type, str]
    ) -> Callable[[DumperType], DumperType]:
        def binary_(dumper: DumperType) -> DumperType:
            assert dumper.format == Format.BINARY
            dumper.register(src)
            return dumper

        return binary_


class Loader(ABC):
    """
    Convert PostgreSQL objects with OID *oid* to Python objects.
    """

    format: Format
    connection: Optional["BaseConnection"]

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        self.oid = oid
        self.connection = context.connection if context else None

    @abstractmethod
    def load(self, data: bytes) -> Any:
        """Convert a PostgreSQL value to a Python object."""
        ...

    @classmethod
    def register(
        cls, oid: int, context: Optional[AdaptContext] = None
    ) -> None:
        """
        Configure *context* to use this loader to convert values with OID *oid*.
        """
        adapters = context.adapters if context else global_adapters
        adapters.register_loader(oid, cls)

    @classmethod
    def text(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def text_(loader: LoaderType) -> LoaderType:
            assert loader.format == Format.TEXT
            loader.register(oid)
            return loader

        return text_

    @classmethod
    def binary(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def binary_(loader: LoaderType) -> LoaderType:
            assert loader.format == Format.BINARY
            loader.register(oid)
            return loader

        return binary_


class AdaptersMap:
    """
    Map oids to Loaders and types to Dumpers.

    The object can start empty or copy from another object of the same class.
    Copies are copy-on-write: if the maps are updated make a copy. This way
    extending e.g. global map by a connection or a connection map from a cursor
    is cheap: a copy is made only on customisation.
    """

    _dumpers: DumpersMap
    _loaders: LoadersMap

    def __init__(self, extend: Optional["AdaptersMap"] = None):
        if extend:
            self._dumpers = extend._dumpers
            self._own_dumpers = False
            self._loaders = extend._loaders
            self._own_loaders = False
        else:
            self._dumpers = {}
            self._own_dumpers = True
            self._loaders = {}
            self._own_loaders = True

    def register_dumper(
        self, src: Union[type, str], dumper: Type[Dumper]
    ) -> None:
        """
        Configure the context to use *dumper* to convert object of type *src*.
        """
        if not isinstance(src, (str, type)):
            raise TypeError(
                f"dumpers should be registered on classes, got {src} instead"
            )

        if not self._own_dumpers:
            self._dumpers = self._dumpers.copy()
            self._own_dumpers = True

        self._dumpers[src, dumper.format] = dumper

    def register_loader(self, oid: int, loader: Type[Loader]) -> None:
        """
        Configure the context to use *loader* to convert data of oid *oid*.
        """
        if not isinstance(oid, int):
            raise TypeError(
                f"loaders should be registered on oid, got {oid} instead"
            )

        if not self._own_loaders:
            self._loaders = self._loaders.copy()
            self._own_loaders = True

        self._loaders[oid, loader.format] = loader


global_adapters = AdaptersMap()


Transformer: Type[proto.Transformer]

# Override it with fast object if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    Transformer = _psycopg3.Transformer
else:
    from . import _transform

    Transformer = _transform.Transformer
