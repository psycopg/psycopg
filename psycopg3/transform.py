"""
Helper object to transform values between Python and PostgreSQL
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import errors as e
from . import pq
from .cursor import BaseCursor
from .types.oids import builtins, INVALID_OID
from .connection import BaseConnection
from .utils.typing import AdaptContext, DumpFunc, DumpersMap, DumperType
from .utils.typing import LoadFunc, LoadersMap, LoaderType, MaybeOid

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

        # mapping class, fmt -> dump function
        self._dump_funcs: Dict[Tuple[type, Format], DumpFunc] = {}

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
            rc.append(self.get_load_function(oid, fmt))

    @property
    def dumpers(self) -> DumpersMap:
        return self._dumpers

    @property
    def loaders(self) -> LoadersMap:
        return self._loaders

    def set_row_types(self, types: Iterable[Tuple[int, Format]]) -> None:
        rc = self._row_loaders = []
        for oid, fmt in types:
            rc.append(self.get_load_function(oid, fmt))

    def dump_sequence(
        self, objs: Iterable[Any], formats: Iterable[Format]
    ) -> Tuple[List[Optional[bytes]], List[int]]:
        out = []
        types = []

        for var, fmt in zip(objs, formats):
            data = self.dump(var, fmt)
            if isinstance(data, tuple):
                oid = data[1]
                data = data[0]
            else:
                oid = TEXT_OID

            out.append(data)
            types.append(oid)

        return out, types

    def dump(self, obj: None, format: Format = Format.TEXT) -> MaybeOid:
        if obj is None:
            return None, TEXT_OID

        src = type(obj)
        func = self.get_dump_function(src, format)
        return func(obj)

    def get_dump_function(self, src: type, format: Format) -> DumpFunc:
        key = (src, format)
        try:
            return self._dump_funcs[key]
        except KeyError:
            pass

        dumper = self.lookup_dumper(src, format)
        func: DumpFunc
        if isinstance(dumper, type):
            func = dumper(src, self).dump
        else:
            func = dumper

        self._dump_funcs[key] = func
        return func

    def lookup_dumper(self, src: type, format: Format) -> DumperType:
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

    def load(self, data: bytes, oid: int, format: Format = Format.TEXT) -> Any:
        if data is not None:
            f = self.get_load_function(oid, format)
            return f(data)
        else:
            return None

    def get_load_function(self, oid: int, format: Format) -> LoadFunc:
        key = (oid, format)
        try:
            return self._load_funcs[key]
        except KeyError:
            pass

        loader = self.lookup_loader(oid, format)
        func: LoadFunc
        if isinstance(loader, type):
            func = loader(oid, self).load
        else:
            func = loader

        self._load_funcs[key] = func
        return func

    def lookup_loader(self, oid: int, format: Format) -> LoaderType:
        key = (oid, format)

        for tcmap in self._loaders_maps:
            if key in tcmap:
                return tcmap[key]

        from .adapt import Loader

        return Loader.globals[INVALID_OID, format]
