"""
Helper object to transform values between Python and PostgreSQL
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any, Dict, List, Optional, Sequence, Tuple
from typing import DefaultDict, Type, TYPE_CHECKING
from collections import defaultdict

from . import pq
from . import postgres
from . import errors as e
from .abc import LoadFunc, AdaptContext, PyFormat, DumperKey
from .rows import Row, RowMaker
from .postgres import INVALID_OID

if TYPE_CHECKING:
    from .abc import Dumper, Loader
    from .adapt import AdaptersMap
    from .pq.abc import PGresult
    from .connection import BaseConnection

NoneType: Type[None] = type(None)
DumperCache = Dict[DumperKey, "Dumper"]
OidDumperCache = Dict[int, "Dumper"]
LoaderCache = Dict[int, "Loader"]


class Transformer(AdaptContext):
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that attributes
    such as the server version or the connection encoding will not change. The
    object have its state so adapting several values of the same type can be
    optimised.

    """

    __module__ = "psycopg.adapt"
    _adapters: "AdaptersMap"
    _pgresult: Optional["PGresult"] = None

    def __init__(self, context: Optional[AdaptContext] = None):

        # WARNING: don't store context, or you'll create a loop with the Cursor
        if context:
            self._adapters = context.adapters
            self._conn = context.connection
        else:
            self._adapters = postgres.adapters
            self._conn = None

        # mapping fmt, class -> Dumper instance
        self._dumpers: DefaultDict[PyFormat, DumperCache]
        self._dumpers = defaultdict(dict)

        # mapping fmt, oid -> Dumper instance
        # Not often used, so create it only if needed.
        self._oid_dumpers: Optional[Tuple[OidDumperCache, OidDumperCache]]
        self._oid_dumpers = None

        # mapping fmt, oid -> Loader instance
        self._loaders: Tuple[LoaderCache, LoaderCache] = ({}, {})

        self._row_dumpers: List[Optional["Dumper"]] = []

        # sequence of load functions from value to python
        # the length of the result columns
        self._row_loaders: List[LoadFunc] = []

    @property
    def connection(self) -> Optional["BaseConnection[Any]"]:
        return self._conn

    @property
    def adapters(self) -> "AdaptersMap":
        return self._adapters

    @property
    def pgresult(self) -> Optional["PGresult"]:
        return self._pgresult

    def set_pgresult(
        self,
        result: Optional["PGresult"],
        *,
        set_loaders: bool = True,
        format: Optional[pq.Format] = None,
    ) -> None:
        self._pgresult = result

        self._ntuples: int
        self._nfields: int
        if not result:
            self._nfields = self._ntuples = 0
            if set_loaders:
                self._row_loaders = []
            return

        nf = self._nfields = result.nfields
        self._ntuples = result.ntuples

        if set_loaders:
            rc = self._row_loaders = []
            for i in range(nf):
                oid = result.ftype(i)
                fmt = result.fformat(i) if format is None else format
                rc.append(self.get_loader(oid, fmt).load)  # type: ignore

    def set_row_types(
        self, types: Sequence[int], formats: Sequence[pq.Format]
    ) -> None:
        rc: List[LoadFunc] = []
        for i in range(len(types)):
            rc.append(self.get_loader(types[i], formats[i]).load)

        self._row_loaders = rc

    def dump_sequence(
        self, params: Sequence[Any], formats: Sequence[PyFormat]
    ) -> Tuple[List[Any], Tuple[int, ...], Sequence[pq.Format]]:
        ps: List[Optional[bytes]] = [None] * len(params)
        ts = [INVALID_OID] * len(params)
        fs: List[pq.Format] = [pq.Format.TEXT] * len(params)

        dumpers = self._row_dumpers
        if not dumpers:
            dumpers = self._row_dumpers = [None] * len(params)

        for i in range(len(params)):
            param = params[i]
            if param is not None:
                dumper = dumpers[i]
                if not dumper:
                    dumper = dumpers[i] = self.get_dumper(param, formats[i])
                ps[i] = dumper.dump(param)
                ts[i] = dumper.oid
                fs[i] = dumper.format

        return ps, tuple(ts), fs

    def get_dumper(self, obj: Any, format: PyFormat) -> "Dumper":
        """
        Return a Dumper instance to dump *obj*.
        """
        # Normally, the type of the object dictates how to dump it
        key = type(obj)

        # Reuse an existing Dumper class for objects of the same type
        cache = self._dumpers[format]
        try:
            dumper = cache[key]
        except KeyError:
            # If it's the first time we see this type, look for a dumper
            # configured for it.
            dcls = self.adapters.get_dumper(key, format)
            cache[key] = dumper = dcls(key, self)

        # Check if the dumper requires an upgrade to handle this specific value
        key1 = dumper.get_key(obj, format)
        if key1 is key:
            return dumper

        # If it does, ask the dumper to create its own upgraded version
        try:
            return cache[key1]
        except KeyError:
            dumper = cache[key1] = dumper.upgrade(obj, format)
            return dumper

    def get_dumper_by_oid(self, oid: int, format: pq.Format) -> "Dumper":
        """
        Return a Dumper to dump an object to the type with given oid.
        """
        if not self._oid_dumpers:
            self._oid_dumpers = ({}, {})

        # Reuse an existing Dumper class for objects of the same type
        cache = self._oid_dumpers[format]
        try:
            return cache[oid]
        except KeyError:
            # If it's the first time we see this type, look for a dumper
            # configured for it.
            dcls = self.adapters.get_dumper_by_oid(oid, format)
            cache[oid] = dumper = dcls(NoneType, self)

        return dumper

    def load_rows(
        self, row0: int, row1: int, make_row: RowMaker[Row]
    ) -> List[Row]:
        res = self._pgresult
        if not res:
            raise e.InterfaceError("result not set")

        if not (0 <= row0 <= self._ntuples and 0 <= row1 <= self._ntuples):
            raise e.InterfaceError(
                f"rows must be included between 0 and {self._ntuples}"
            )

        records = []
        for row in range(row0, row1):
            record: List[Any] = [None] * self._nfields
            for col in range(self._nfields):
                val = res.get_value(row, col)
                if val is not None:
                    record[col] = self._row_loaders[col](val)
            records.append(make_row(record))

        return records

    def load_row(self, row: int, make_row: RowMaker[Row]) -> Optional[Row]:
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

        return make_row(record)

    def load_sequence(
        self, record: Sequence[Optional[bytes]]
    ) -> Tuple[Any, ...]:
        if len(self._row_loaders) != len(record):
            raise e.ProgrammingError(
                f"cannot load sequence of {len(record)} items:"
                f" {len(self._row_loaders)} loaders registered"
            )

        return tuple(
            (self._row_loaders[i](val) if val is not None else None)
            for i, val in enumerate(record)
        )

    def get_loader(self, oid: int, format: pq.Format) -> "Loader":
        try:
            return self._loaders[format][oid]
        except KeyError:
            pass

        loader_cls = self._adapters.get_loader(oid, format)
        if not loader_cls:
            loader_cls = self._adapters.get_loader(INVALID_OID, format)
            if not loader_cls:
                raise e.InterfaceError("unknown oid loader not found")
        loader = self._loaders[format][oid] = loader_cls(oid, self)
        return loader
