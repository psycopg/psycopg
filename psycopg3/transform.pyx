import codecs
from typing import Any, Dict, Iterable, List, Optional, Tuple

from psycopg3.pq cimport libpq

from psycopg3 import errors as e
# from psycopg3.pq.enum import Format
# from psycopg3.types.oids import builtins, INVALID_OID

TEXT_OID = 25


cdef class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    cdef list _dumpers_maps, _loaders_maps, _row_loaders
    cdef dict _dumpers, _loaders, _dump_funcs, _load_funcs
    cdef object _connection, _codec
    cdef PGresult _pgresult
    cdef int _nfields, _ntuples

    def __cinit__(self, context: "AdaptContext" = None):
        self._dumpers_maps: List["DumpersMap"] = []
        self._loaders_maps: List["LoadersMap"] = []
        self._setup_context(context)

        # mapping class, fmt -> dump function
        self._dump_funcs: Dict[Tuple[type, Format], "DumpFunc"] = {}

        # mapping oid, fmt -> load function
        self._load_funcs: Dict[Tuple[int, Format], "LoadFunc"] = {}

        # sequence of load functions from value to python
        # the length of the result columns
        self._row_loaders: List["LoadFunc"] = []

        self.pgresult = None

    def _setup_context(self, context: "AdaptContext") -> None:
        from psycopg3.adapt import Dumper, Loader
        from psycopg3.cursor import BaseCursor
        from psycopg3.connection import BaseConnection

        cdef Transformer ctx
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
            ctx = context
            self._connection = ctx._connection
            self._codec = ctx._codec
            self._dumpers = ctx._dumpers
            self._loaders = ctx._loaders
            self._dumpers_maps.extend(ctx._dumpers_maps)
            self._loaders_maps.extend(ctx._loaders_maps)
            # the global maps are already in the lists
            return

        elif isinstance(context, BaseCursor):
            self._connection = context.connection
            self._codec = context.connection.codec
            self._dumpers = {}
            self._dumpers_maps.extend(
                (self._dumpers, context.dumpers, self.connection.dumpers)
            )
            self._loaders = {}
            self._loaders_maps.extend(
                (self._loaders, context.loaders, self.connection.loaders)
            )

        elif isinstance(context, BaseConnection):
            self._connection = context
            self._codec = context.codec
            self._dumpers = {}
            self._dumpers_maps.extend((self._dumpers, context.dumpers))
            self._loaders = {}
            self._loaders_maps.extend((self._loaders, context.loaders))

        self._dumpers_maps.append(Dumper.globals)
        self._loaders_maps.append(Loader.globals)

    @property
    def connection(self):
        return self._connection

    @property
    def codec(self):
        return self._codec

    @property
    def dumpers(self):
        return self._dumpers

    @property
    def loaders(self):
        return self._loaders

    @property
    def pgresult(self) -> Optional["pq.proto.PGresult"]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional["pq.proto.PGresult"]) -> None:
        self._pgresult = result

        if result is None:
            self._nfields = self._ntuples = 0
            return

        cdef libpq.PGresult *res = self._pgresult.pgresult_ptr
        self._nfields = libpq.PQnfields(res)
        self._ntuples = libpq.PQntuples(res)

        cdef int i
        types = [
            (libpq.PQftype(res, i), libpq.PQfformat(res, i))
            for i in range(self._nfields)]
        self.set_row_types(types)

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

    def dump(self, obj: None, format: Format = 0) -> "MaybeOid":
        if obj is None:
            return None, TEXT_OID

        src = type(obj)
        func = self.get_dump_function(src, format)
        return func(obj)

    def get_dump_function(self, src: type, format: Format) -> "DumpFunc":
        key = (src, format)
        try:
            return self._dump_funcs[key]
        except KeyError:
            pass

        dumper = self.lookup_dumper(src, format)
        func: "DumpFunc"
        if isinstance(dumper, type):
            func = dumper(src, self).dump
        else:
            func = dumper

        self._dump_funcs[key] = func
        return func

    def lookup_dumper(self, src: type, format: Format) -> "DumperType":
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
        self, record: Iterable[Optional[bytes]]
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

    def get_load_function(self, oid: int, format: Format) -> "LoadFunc":
        key = (oid, format)
        try:
            return self._load_funcs[key]
        except KeyError:
            pass

        loader = self.lookup_loader(oid, format)
        func: "LoadFunc"
        if isinstance(loader, type):
            func = loader(oid, self).load
        else:
            func = loader

        self._load_funcs[key] = func
        return func

    def lookup_loader(self, oid: int, format: Format) -> "LoaderType":
        key = (oid, format)

        for tcmap in self._loaders_maps:
            if key in tcmap:
                return tcmap[key]

        from psycopg3.adapt import Loader

        return Loader.globals[0, format]    # INVALID_OID
