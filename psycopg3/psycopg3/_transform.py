"""
Helper object to transform values between Python and PostgreSQL
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from typing import DefaultDict, TYPE_CHECKING
from collections import defaultdict

from . import pq
from . import errors as e
from .oids import INVALID_OID
from .proto import LoadFunc, AdaptContext, Row, RowMaker
from ._enums import Format

if TYPE_CHECKING:
    from .pq.proto import PGresult
    from .adapt import Dumper, Loader, AdaptersMap
    from .connection import BaseConnection

DumperKey = Union[type, Tuple[type, ...]]
DumperCache = Dict[DumperKey, "Dumper"]

LoaderKey = int
LoaderCache = Dict[LoaderKey, "Loader"]


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
    make_row: RowMaker = tuple

    def __init__(self, context: Optional[AdaptContext] = None):

        # WARNING: don't store context, or you'll create a loop with the Cursor
        if context:
            self._adapters = context.adapters
            self._conn = context.connection
        else:
            from .adapt import global_adapters

            self._adapters = global_adapters
            self._conn = None

        # mapping class, fmt -> Dumper instance
        self._dumpers_cache: DefaultDict[Format, DumperCache] = defaultdict(
            dict
        )

        # mapping oid, fmt -> Loader instance
        self._loaders_cache: Tuple[LoaderCache, LoaderCache] = ({}, {})

        self._row_dumpers: List[Optional["Dumper"]] = []

        # sequence of load functions from value to python
        # the length of the result columns
        self._row_loaders: List[LoadFunc] = []

    @property
    def connection(self) -> Optional["BaseConnection"]:
        return self._conn

    @property
    def adapters(self) -> "AdaptersMap":
        return self._adapters

    @property
    def pgresult(self) -> Optional["PGresult"]:
        return self._pgresult

    def set_pgresult(
        self, result: Optional["PGresult"], set_loaders: bool = True
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
                fmt = result.fformat(i)
                rc.append(self.get_loader(oid, fmt).load)  # type: ignore

    def set_row_types(
        self, types: Sequence[int], formats: Sequence[pq.Format]
    ) -> None:
        rc: List[LoadFunc] = []
        for i in range(len(types)):
            rc.append(self.get_loader(types[i], formats[i]).load)

        self._row_loaders = rc

    def dump_sequence(
        self, params: Sequence[Any], formats: Sequence[Format]
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

    def get_dumper(self, obj: Any, format: Format) -> "Dumper":
        """
        Return a Dumper instance to dump *obj*.
        """
        # Normally, the type of the object dictates how to dump it
        key = type(obj)

        # Reuse an existing Dumper class for objects of the same type
        cache = self._dumpers_cache[format]
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

    def load_rows(self, row0: int, row1: int) -> List[Row]:
        res = self._pgresult
        if not res:
            raise e.InterfaceError("result not set")

        if not (0 <= row0 <= self._ntuples and 0 <= row1 <= self._ntuples):
            raise e.InterfaceError(
                f"rows must be included between 0 and {self._ntuples}"
            )

        records: List[Row] = []
        for row in range(row0, row1):
            record: List[Any] = [None] * self._nfields
            for col in range(self._nfields):
                val = res.get_value(row, col)
                if val is not None:
                    record[col] = self._row_loaders[col](val)
            records.append(self.make_row(record))

        return records

    def load_row(self, row: int) -> Optional[Row]:
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

        return self.make_row(record)  # type: ignore[no-any-return]

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
