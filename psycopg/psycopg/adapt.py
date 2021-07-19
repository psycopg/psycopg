"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from abc import ABC, abstractmethod
from typing import Any, Optional, Type, Tuple, Union, TYPE_CHECKING

from . import pq, abc
from . import _adapters_map
from ._enums import PyFormat as PyFormat
from ._cmodule import _psycopg

if TYPE_CHECKING:
    from .connection import BaseConnection

AdaptersMap = _adapters_map.AdaptersMap
Buffer = abc.Buffer


class Dumper(abc.Dumper, ABC):
    """
    Convert Python object of the type *cls* to PostgreSQL representation.
    """

    # A class-wide oid, which will be used by default by instances unless
    # the subclass overrides it in init.
    _oid: int = 0

    oid: int
    """The oid to pass to the server, if known."""

    def __init__(self, cls: type, context: Optional[abc.AdaptContext] = None):
        self.cls = cls
        self.connection: Optional["BaseConnection[Any]"] = (
            context.connection if context else None
        )

        self.oid = self._oid

    def __repr__(self) -> str:
        return (
            f"<{type(self).__module__}.{type(self).__qualname__}"
            f" (oid={self.oid}) at 0x{id(self):x}>"
        )

    @abstractmethod
    def dump(self, obj: Any) -> Buffer:
        ...

    def quote(self, obj: Any) -> Buffer:
        """
        By default return the `dump()` value quoted and sanitised, so
        that the result can be used to build a SQL string. This works well
        for most types and you won't likely have to implement this method in a
        subclass.
        """
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
        """
        Implementation of the `~psycopg.abc.Dumper.get_key()` member of the
        `~psycopg.abc.Dumper` protocol. Look at its definition for details.

        This implementation returns the *cls* passed in the constructor.
        Subclasses needing to specialise the PostgreSQL type according to the
        *value* of the object dumped (not only according to to its type)
        should override this class.

        """
        return self.cls

    def upgrade(self, obj: Any, format: PyFormat) -> "Dumper":
        """
        Implementation of the `~psycopg.abc.Dumper.upgrade()` member of the
        `~psycopg.abc.Dumper` protocol. Look at its definition for details.

        This implementation just returns *self*. If a subclass implements
        `get_key()` it should probably override `!upgrade()` too.
        """
        return self


class Loader(ABC):
    """
    Convert PostgreSQL objects with OID *oid* to Python objects.
    """

    format: pq.Format

    def __init__(self, oid: int, context: Optional[abc.AdaptContext] = None):
        self.oid = oid
        self.connection: Optional["BaseConnection[Any]"] = (
            context.connection if context else None
        )

    @abstractmethod
    def load(self, data: Buffer) -> Any:
        """Convert a PostgreSQL value to a Python object."""
        ...


Transformer: Type["abc.Transformer"]

# Override it with fast object if available
if _psycopg:
    Transformer = _psycopg.Transformer
else:
    from . import _transform

    Transformer = _transform.Transformer


class RecursiveDumper(Dumper):
    """Dumper with a transformer to help dumping recursive types."""

    def __init__(self, cls: type, context: Optional[abc.AdaptContext] = None):
        super().__init__(cls, context)
        self._tx = Transformer(context)


class RecursiveLoader(Loader):
    """Loader with a transformer to help loading recursive types."""

    def __init__(self, oid: int, context: Optional[abc.AdaptContext] = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)
