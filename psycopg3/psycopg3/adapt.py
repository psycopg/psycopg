"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Tuple, Union
from typing import cast, TYPE_CHECKING, TypeVar

from . import pq
from . import proto
from . import errors as e
from ._enums import Format as Format
from .oids import postgres_types
from .proto import AdaptContext, Buffer as Buffer
from ._typeinfo import TypesRegistry

if TYPE_CHECKING:
    from .connection import BaseConnection

RV = TypeVar("RV")


class Dumper(ABC):
    """
    Convert Python object of the type *cls* to PostgreSQL representation.
    """

    format: pq.Format

    # A class-wide oid, which will be used by default by instances unless
    # the subclass overrides it in init.
    _oid: int = 0

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        self.cls = cls
        self.connection: Optional["BaseConnection"] = (
            context.connection if context else None
        )

        self.oid = self._oid
        """The oid to pass to the server, if known."""

    @abstractmethod
    def dump(self, obj: Any) -> Buffer:
        """Convert the object *obj* to PostgreSQL representation."""
        ...

    def quote(self, obj: Any) -> Buffer:
        """Convert the object *obj* to escaped representation."""
        value = self.dump(obj)

        if self.connection:
            esc = pq.Escaping(self.connection.pgconn)
            return esc.escape_literal(value)
        else:
            esc = pq.Escaping()
            return b"'%s'" % esc.escape_string(value)

    def get_key(
        self, obj: Any, format: Format
    ) -> Union[type, Tuple[type, ...]]:
        """Return an alternative key to upgrade the dumper to represent *obj*

        Normally the type of the object is all it takes to define how to dump
        the object to the database. In a few cases this is not enough. Example

        - Python int could be several Postgres types: int2, int4, int8, numeric
        - Python lists should be dumped according to the type they contain
          to convert them to e.g. array of strings, array of ints (which?...)

        In these cases a Dumper can implement `get_key()` and return a new
        class, or sequence of classes, that can be used to indentify the same
        dumper again.

        If a Dumper implements `get_key()` it should also implmement
        `upgrade()`.
        """
        return self.cls

    def upgrade(self, obj: Any, format: Format) -> "Dumper":
        """Return a new dumper to manage *obj*.

        Once `Transformer.get_dumper()` has been notified that this Dumper
        class cannot handle *obj* itself it will invoke `upgrade()`, which
        should return a new `Dumper` instance, and will be reused for every
        objects for which `get_key()` returns the same result.
        """
        return self

    @classmethod
    def register(
        this_cls, cls: Union[type, str], context: Optional[AdaptContext] = None
    ) -> None:
        """
        Configure *context* to use this dumper to convert object of type *cls*.
        """
        adapters = context.adapters if context else global_adapters
        adapters.register_dumper(cls, this_cls)


class Loader(ABC):
    """
    Convert PostgreSQL objects with OID *oid* to Python objects.
    """

    format: pq.Format

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        self.oid = oid
        self.connection: Optional["BaseConnection"] = (
            context.connection if context else None
        )

    @abstractmethod
    def load(self, data: Buffer) -> Any:
        """Convert a PostgreSQL value to a Python object."""
        ...

    @classmethod
    def register(
        cls, oid: Union[int, str], context: Optional[AdaptContext] = None
    ) -> None:
        """
        Configure *context* to use this loader to convert values with OID *oid*.
        """
        adapters = context.adapters if context else global_adapters
        adapters.register_loader(oid, cls)


class AdaptersMap(AdaptContext):
    """
    Map oids to Loaders and types to Dumpers.

    The object can start empty or copy from another object of the same class.
    Copies are copy-on-write: if the maps are updated make a copy. This way
    extending e.g. global map by a connection or a connection map from a cursor
    is cheap: a copy is made only on customisation.
    """

    _dumpers: List[Dict[Union[type, str], Type["Dumper"]]]
    _loaders: List[Dict[int, Type["Loader"]]]
    types: TypesRegistry

    # Record if a dumper or loader has an optimised version.
    _optimised: Dict[type, type] = {}

    def __init__(
        self,
        template: Optional["AdaptersMap"] = None,
        types: Optional[TypesRegistry] = None,
    ):
        if template:
            self._dumpers = template._dumpers[:]
            self._own_dumpers = [False, False]
            self._loaders = template._loaders[:]
            self._own_loaders = [False, False]
            self.types = TypesRegistry(template.types)
        else:
            self._dumpers = [{}, {}]
            self._own_dumpers = [True, True]
            self._loaders = [{}, {}]
            self._own_loaders = [True, True]
            self.types = types or TypesRegistry()

    # implement the AdaptContext protocol too
    @property
    def adapters(self) -> "AdaptersMap":
        return self

    @property
    def connection(self) -> Optional["BaseConnection"]:
        return None

    def register_dumper(
        self, cls: Union[type, str], dumper: Type[Dumper]
    ) -> None:
        """
        Configure the context to use *dumper* to convert object of type *cls*.
        """
        if not isinstance(cls, (str, type)):
            raise TypeError(
                f"dumpers should be registered on classes, got {cls} instead"
            )

        dumper = self._get_optimised(dumper)
        fmt = dumper.format
        if not self._own_dumpers[fmt]:
            self._dumpers[fmt] = self._dumpers[fmt].copy()
            self._own_dumpers[fmt] = True

        self._dumpers[fmt][cls] = dumper

    def register_loader(
        self, oid: Union[int, str], loader: Type[Loader]
    ) -> None:
        """
        Configure the context to use *loader* to convert data of oid *oid*.
        """
        if isinstance(oid, str):
            oid = self.types[oid].oid
        if not isinstance(oid, int):
            raise TypeError(
                f"loaders should be registered on oid, got {oid} instead"
            )

        loader = self._get_optimised(loader)
        fmt = loader.format
        if not self._own_loaders[fmt]:
            self._loaders[fmt] = self._loaders[fmt].copy()
            self._own_loaders[fmt] = True

        self._loaders[fmt][oid] = loader

    def get_dumper(self, cls: type, format: Format) -> Type[Dumper]:
        """
        Return the dumper class for the given type and format.

        Raise ProgrammingError if a class is not available.
        """
        if format == Format.AUTO:
            # When dumping a string with %s we may refer to any type actually,
            # but the user surely passed a text format
            if cls is str:
                dmaps = [self._dumpers[pq.Format.TEXT]]
            else:
                dmaps = [
                    self._dumpers[pq.Format.BINARY],
                    self._dumpers[pq.Format.TEXT],
                ]
        elif format == Format.BINARY:
            dmaps = [self._dumpers[pq.Format.BINARY]]
        elif format == Format.TEXT:
            dmaps = [self._dumpers[pq.Format.TEXT]]
        else:
            raise ValueError(f"bad dumper format: {format}")

        # Look for the right class, including looking at superclasses
        for scls in cls.__mro__:
            for dmap in dmaps:
                if scls in dmap:
                    return dmap[scls]

            # If the adapter is not found, look for its name as a string
            fqn = scls.__module__ + "." + scls.__qualname__
            for dmap in dmaps:
                if fqn in dmap:
                    # Replace the class name with the class itself
                    d = dmap[scls] = dmap.pop(fqn)
                    return d

        raise e.ProgrammingError(
            f"cannot adapt type {cls.__name__}"
            f" to format {Format(format).name}"
        )

    def get_loader(
        self, oid: int, format: pq.Format
    ) -> Optional[Type[Loader]]:
        """
        Return the loader class for the given oid and format.

        Return None if not found.
        """
        return self._loaders[format].get(oid)

    @classmethod
    def _get_optimised(self, cls: Type[RV]) -> Type[RV]:
        """Return the optimised version of a Dumper or Loader class.

        Return the input class itself if there is no optimised version.
        """
        try:
            return self._optimised[cls]
        except KeyError:
            pass

        # Check if the class comes from psycopg3.types and there is a class
        # with the same name in psycopg3_c._psycopg3.
        if pq.__impl__ == "c":
            from psycopg3 import types
            from psycopg3_c import _psycopg3

            if cls.__module__.startswith(types.__name__):
                new = cast(Type[RV], getattr(_psycopg3, cls.__name__, None))
                if new:
                    self._optimised[cls] = new
                    return new

        self._optimised[cls] = cls
        return cls


global_adapters = AdaptersMap(types=postgres_types)


Transformer: Type[proto.Transformer]

# Override it with fast object if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    Transformer = _psycopg3.Transformer
else:
    from . import _transform

    Transformer = _transform.Transformer
