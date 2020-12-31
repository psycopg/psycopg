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
        self._dumpers_cache: Tuple[Dict[type, "Dumper"], Dict[type, "Dumper"]]
        self._dumpers_cache = ({}, {})

        # mapping oid, fmt -> Loader instance
        self._loaders_cache: Tuple[Dict[int, "Loader"], Dict[int, "Loader"]]
        self._loaders_cache = ({}, {})

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
            rc.append(self.get_loader(oid, fmt).load)  # type: ignore

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
            return self._dumpers_cache[format][cls]
        except KeyError:
            pass

        dumper_class = self._adapters.get_dumper(cls, format)
        if dumper_class:
            d = self._dumpers_cache[format][cls] = dumper_class(cls, self)
            return d

        raise e.ProgrammingError(
            f"cannot adapt type {cls.__name__} to format {Format(format).name}"
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
        try:
            return self._loaders_cache[format][oid]
        except KeyError:
            pass

        loader_cls = self._adapters.get_loader(oid, format)
        if not loader_cls:
            loader_cls = self._adapters.get_loader(INVALID_OID, format)
            if not loader_cls:
                raise e.InterfaceError("unknown oid loader not found")
        loader = self._loaders_cache[format][oid] = loader_cls(oid, self)
        return loader
