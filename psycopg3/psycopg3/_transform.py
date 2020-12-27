"""
Helper object to transform values between Python and PostgreSQL
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Dict, List, Optional, Sequence, Tuple
from typing import TYPE_CHECKING

from . import errors as e
from .pq import Format
from .oids import INVALID_OID
from .proto import LoadFunc, AdaptContext

if TYPE_CHECKING:
    from .pq.proto import PGresult
    from .adapt import Dumper, Loader, AdaptersMap
    from .connection import BaseConnection


class Transformer(AdaptContext):
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can be optimised.
    """

    __module__ = "psycopg3.adapt"
    _adapters: "AdaptersMap"
    _pgresult: Optional["PGresult"] = None

    def __init__(self, context: Optional[AdaptContext] = None):
        # WARNING: don't store context, or you'll create a loop with the Cursor
        if context:
            self._adapters = context.adapters
            self._connection = context.connection

        else:
            from .adapt import global_adapters

            self._adapters = global_adapters
            self._connection = None

        # mapping class, fmt -> Dumper instance
        self._dumpers_cache: Dict[Tuple[type, Format], "Dumper"] = {}

        # mapping oid, fmt -> Loader instance
        self._loaders_cache: Dict[Tuple[int, Format], "Loader"] = {}

        # sequence of load functions from value to python
        # the length of the result columns
        self._row_loaders: List[LoadFunc] = []

    @property
    def connection(self) -> Optional["BaseConnection"]:
        return self._connection

    @property
    def adapters(self) -> "AdaptersMap":
        return self._adapters

    @property
    def pgresult(self) -> Optional["PGresult"]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional["PGresult"]) -> None:
        self._pgresult = result
        rc = self._row_loaders = []

        self._ntuples: int
        self._nfields: int
        if not result:
            self._nfields = self._ntuples = 0
            return

        nf = self._nfields = result.nfields
        self._ntuples = result.ntuples

        for i in range(nf):
            oid = result.ftype(i)
            fmt = result.fformat(i)
            rc.append(self.get_loader(oid, fmt).load)

    def set_row_types(
        self, types: Sequence[int], formats: Sequence[Format]
    ) -> None:
        rc: List[LoadFunc] = [None] * len(types)  # type: ignore[list-item]
        for i in range(len(rc)):
            rc[i] = self.get_loader(types[i], formats[i]).load

        self._row_loaders = rc

    def get_dumper(self, obj: Any, format: Format) -> "Dumper":
        # Fast path: return a Dumper class already instantiated from the same type
        cls = type(obj)
        try:
            return self._dumpers_cache[cls, format]
        except KeyError:
            pass

        # We haven't seen this type in this query yet. Look for an adapter
        # in the current context (which was grown from more generic ones).
        # Also look for superclasses: if you can adapt a type you should be
        # able to adapt its subtypes, otherwise Liskov is sad.
        dmap = self._adapters._dumpers
        for scls in cls.__mro__:
            dumper_class = dmap.get((scls, format))
            if not dumper_class:
                continue

            dumper = self._dumpers_cache[cls, format] = dumper_class(cls, self)
            return dumper

        # If the adapter is not found, look for its name as a string
        for scls in cls.__mro__:
            fqn = f"{cls.__module__}.{scls.__qualname__}"
            dumper_class = dmap.get((fqn, format))
            if dumper_class is None:
                continue

            dmap[cls, format] = dumper_class
            dumper = self._dumpers_cache[cls, format] = dumper_class(cls, self)
            return dumper

        raise e.ProgrammingError(
            f"cannot adapt type {type(obj).__name__}"
            f" to format {Format(format).name}"
        )

    def load_rows(self, row0: int, row1: int) -> Sequence[Tuple[Any, ...]]:
        res = self._pgresult
        if not res:
            raise e.InterfaceError("result not set")

        if not (0 <= row0 <= self._ntuples and 0 <= row1 <= self._ntuples):
            raise e.InterfaceError(
                f"rows must be included between 0 and {self._ntuples}"
            )

        records: List[Tuple[Any, ...]]
        records = [None] * (row1 - row0)  # type: ignore[list-item]
        for row in range(row0, row1):
            record: List[Any] = [None] * self._nfields
            for col in range(self._nfields):
                val = res.get_value(row, col)
                if val is not None:
                    record[col] = self._row_loaders[col](val)
            records[row - row0] = tuple(record)

        return records

    def load_row(self, row: int) -> Optional[Tuple[Any, ...]]:
        res = self._pgresult
        if not res:
            return None

        if not 0 <= row < self._ntuples:
            return None

        record: List[Any] = [None] * self._nfields
        for col in range(self._nfields):
            val = res.get_value(row, col)
            if val is not None:
                record[col] = self._row_loaders[col](val)

        return tuple(record)

    def load_sequence(
        self, record: Sequence[Optional[bytes]]
    ) -> Tuple[Any, ...]:
        return tuple(
            (self._row_loaders[i](val) if val is not None else None)
            for i, val in enumerate(record)
        )

    def get_loader(self, oid: int, format: Format) -> "Loader":
        key = (oid, format)
        try:
            return self._loaders_cache[key]
        except KeyError:
            pass

        loader_cls = self._adapters._loaders.get(key)
        if not loader_cls:
            loader_cls = self._adapters._loaders[INVALID_OID, format]
        loader = self._loaders_cache[key] = loader_cls(oid, self)
        return loader
