"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from abc import ABC, abstractmethod
from typing import Any, Optional, Type, Tuple, Union, TYPE_CHECKING

from . import pq
from . import _adapters_map
from .proto import AdaptContext, Buffer as Buffer
from ._enums import PyFormat as PyFormat
from ._cmodule import _psycopg

if TYPE_CHECKING:
    from . import proto
    from .connection import BaseConnection

AdaptersMap = _adapters_map.AdaptersMap


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
        self.connection: Optional["BaseConnection[Any]"] = (
            context.connection if context else None
        )

        self.oid: int = self._oid
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
        self, obj: Any, format: PyFormat
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

    def upgrade(self, obj: Any, format: PyFormat) -> "Dumper":
        """Return a new dumper to manage *obj*.

        Once `Transformer.get_dumper()` has been notified that this Dumper
        class cannot handle *obj* itself it will invoke `upgrade()`, which
        should return a new `Dumper` instance, and will be reused for every
        objects for which `get_key()` returns the same result.
        """
        return self


class Loader(ABC):
    """
    Convert PostgreSQL objects with OID *oid* to Python objects.
    """

    format: pq.Format

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        self.oid = oid
        self.connection: Optional["BaseConnection[Any]"] = (
            context.connection if context else None
        )

    @abstractmethod
    def load(self, data: Buffer) -> Any:
        """Convert a PostgreSQL value to a Python object."""
        ...


Transformer: Type["proto.Transformer"]

# Override it with fast object if available
if _psycopg:
    Transformer = _psycopg.Transformer
else:
    from . import _transform

    Transformer = _transform.Transformer


class RecursiveDumper(Dumper):
    """Dumper with a transformer to help dumping recursive types."""

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._tx = Transformer(context)


class RecursiveLoader(Loader):
    """Loader with a transformer to help loading recursive types."""

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)
