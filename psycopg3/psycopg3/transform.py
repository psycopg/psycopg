"""
Helper object to transform values between Python and PostgreSQL
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from typing import TYPE_CHECKING

from . import errors as e
from . import pq
from .proto import AdaptContext, DumpersMap, DumperType
from .proto import LoadFunc, LoadersMap, LoaderType
from .cursor import BaseCursor
from .connection import BaseConnection
from .types.oids import builtins, INVALID_OID

if TYPE_CHECKING:
    from .adapt import Dumper, Loader

Format = pq.Format
TEXT_OID = builtins["text"].oid


class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    def __init__(self, context: AdaptContext = None):
        self._dumpers: DumpersMap
        self._loaders: LoadersMap
        self._dumpers_maps: List[DumpersMap] = []
        self._loaders_maps: List[LoadersMap] = []
        self._setup_context(context)
        self.pgresult = None

        # mapping class, fmt -> Dumper instance
        self._dumpers_cache: Dict[Tuple[type, Format], "Dumper"] = {}

        # mapping oid, fmt -> Loader instance
        self._loaders_cache: Dict[Tuple[int, Format], "Loader"] = {}

        # mapping oid, fmt -> load function
        self._load_funcs: Dict[Tuple[int, Format], LoadFunc] = {}

        # sequence of load functions from value to python
        # the length of the result columns
        self._row_loaders: List[LoadFunc] = []

    def _setup_context(self, context: AdaptContext) -> None:
        if context is None:
            self._connection = None
            self._codec = codecs.lookup("utf8")
            self._dumpers = {}
            self._loaders = {}
            self._dumpers_maps = [self._dumpers]
            self._loaders_maps = [self._loaders]

        elif isinstance(context, Transformer):
            # A transformer created from a transformers: usually it happens
            # for nested types: share the entire state of the parent
            self._connection = context.connection
            self._codec = context.codec
            self._dumpers = context.dumpers
            self._loaders = context.loaders
            self._dumpers_maps.extend(context._dumpers_maps)
            self._loaders_maps.extend(context._loaders_maps)
            # the global maps are already in the lists
            return

        elif isinstance(context, BaseCursor):
            self._connection = context.connection
            self._codec = context.connection.codec
            self._dumpers = {}
            self._dumpers_maps.extend(
                (self._dumpers, context.dumpers, context.connection.dumpers)
            )
            self._loaders = {}
            self._loaders_maps.extend(
                (self._loaders, context.loaders, context.connection.loaders)
            )

        elif isinstance(context, BaseConnection):
            self._connection = context
            self._codec = context.codec
            self._dumpers = {}
            self._dumpers_maps.extend((self._dumpers, context.dumpers))
            self._loaders = {}
            self._loaders_maps.extend((self._loaders, context.loaders))

        from .adapt import Dumper, Loader

        self._dumpers_maps.append(Dumper.globals)
        self._loaders_maps.append(Loader.globals)

    @property
    def connection(self) -> Optional["BaseConnection"]:
        return self._connection

    @property
    def codec(self) -> codecs.CodecInfo:
        return self._codec

    @property
    def pgresult(self) -> Optional[pq.proto.PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[pq.proto.PGresult]) -> None:
        self._pgresult = result
        rc = self._row_loaders = []

        self._ntuples: int
        self._nfields: int
        if result is None:
            self._nfields = self._ntuples = 0
            return

        nf = self._nfields = result.nfields
        self._ntuples = result.ntuples

        for i in range(nf):
            oid = result.ftype(i)
            fmt = result.fformat(i)
            rc.append(self.get_loader(oid, fmt).load)

    @property
    def dumpers(self) -> DumpersMap:
        return self._dumpers

    @property
    def loaders(self) -> LoadersMap:
        return self._loaders

    def set_row_types(self, types: Iterable[Tuple[int, Format]]) -> None:
        rc = self._row_loaders = []
        for oid, fmt in types:
            rc.append(self.get_loader(oid, fmt).load)

    def get_dumper(self, obj: Any, format: Format) -> "Dumper":
        key = (type(obj), format)
        try:
            return self._dumpers_cache[key]
        except KeyError:
            pass

        dumper_cls = self._lookup_dumper(*key)
        self._dumpers_cache[key] = dumper = dumper_cls(key[0], self)
        return dumper

    def _lookup_dumper(self, src: type, format: Format) -> DumperType:
        key = (src, format)
        for amap in self._dumpers_maps:
            if key in amap:
                return amap[key]

        raise e.ProgrammingError(
            f"cannot adapt type {src.__name__} to format {Format(format).name}"
        )

    def load_row(self, row: int) -> Optional[Tuple[Any, ...]]:
        res = self.pgresult
        if res is None:
            return None

        if row >= self._ntuples:
            return None

        rv: List[Any] = []
        for col in range(self._nfields):
            val = res.get_value(row, col)
            if val is None:
                rv.append(None)
            else:
                rv.append(self._row_loaders[col](val))

        return tuple(rv)

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

        loader_cls = self._lookup_loader(*key)
        self._loaders_cache[key] = loader = loader_cls(key[0], self)
        return loader

    def _lookup_loader(self, oid: int, format: Format) -> LoaderType:
        key = (oid, format)

        for tcmap in self._loaders_maps:
            if key in tcmap:
                return tcmap[key]

        from .adapt import Loader

        return Loader.globals[INVALID_OID, format]
