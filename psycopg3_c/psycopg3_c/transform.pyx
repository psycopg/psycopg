"""
Helper object to transform values between Python and PostgreSQL

Cython implementation: can access to lower level C features without creating
too many temporary Python objects and performing less memory copying.

"""

# Copyright (C) 2020 The Psycopg Team

from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM

import codecs
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg3_c cimport libpq
from psycopg3_c.pq_cython cimport PGresult

from psycopg3 import errors as e
from psycopg3.pq.enums import Format

TEXT_OID = 25


cdef class RowLoader:
    cdef object pyloader
    cdef PyxLoader pyxloader


cdef class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    cdef list _dumpers_maps, _loaders_maps
    cdef dict _dumpers, _loaders, _dumpers_cache, _loaders_cache, _load_funcs
    cdef object _connection, _codec
    cdef PGresult _pgresult
    cdef int _nfields, _ntuples

    cdef list _row_loaders

    def __cinit__(self, context: "AdaptContext" = None):
        self._dumpers_maps: List["DumpersMap"] = []
        self._loaders_maps: List["LoadersMap"] = []
        self._setup_context(context)

        # mapping class, fmt -> Dumper instance
        self._dumpers_cache: Dict[Tuple[type, Format], "Dumper"] = {}

        # mapping oid, fmt -> Loader instance
        self._loaders_cache: Dict[Tuple[int, Format], "Loader"] = {}

        # mapping oid, fmt -> load function
        self._load_funcs: Dict[Tuple[int, Format], "LoadFunc"] = {}

        self.pgresult = None
        self._row_loaders = []

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
    def pgresult(self) -> Optional[PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[PGresult]) -> None:
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

    def set_row_types(self, types: Sequence[Tuple[int, Format]]) -> None:
        del self._row_loaders[:]

        cdef int i = 0
        cdef dict seen = {}
        for oid_fmt in types:
            if oid_fmt not in seen:
                self._row_loaders.append(
                    self._get_row_loader(oid_fmt[0], oid_fmt[1]))
                seen[oid_fmt] = i
            else:
                self._row_loaders.append(self._row_loaders[seen[oid_fmt]])

            i += 1

    cdef RowLoader _get_row_loader(self, libpq.Oid oid, int fmt):
        cdef RowLoader row_loader = RowLoader()
        loader = self.get_loader(oid, fmt)
        row_loader.pyloader = loader.load

        if isinstance(loader, PyxLoader):
            row_loader.pyxloader = loader
        else:
            row_loader.pyxloader = None

        return row_loader

    def get_dumper(self, obj: Any, format: Format) -> "Dumper":
        key = (type(obj), format)
        try:
            return self._dumpers_cache[key]
        except KeyError:
            pass

        dumper_cls = self.lookup_dumper(*key)
        self._dumpers_cache[key] = dumper = dumper_cls(key[0], self)
        return dumper

    def lookup_dumper(self, src: type, format: Format) -> "DumperType":
        key = (src, format)
        for amap in self._dumpers_maps:
            if key in amap:
                return amap[key]

        raise e.ProgrammingError(
            f"cannot adapt type {src.__name__} to format {Format(format).name}"
        )

    def load_row(self, row: int) -> Optional[Tuple[Any, ...]]:
        if self._pgresult is None:
            return None

        cdef int crow = row
        if crow >= self._ntuples:
            return None

        cdef libpq.PGresult *res = self._pgresult.pgresult_ptr

        cdef RowLoader loader
        cdef int col
        cdef int length
        cdef const char *val
        rv = PyTuple_New(self._nfields)
        for col in range(self._nfields):
            length = libpq.PQgetlength(res, crow, col)
            if length == 0:
                if libpq.PQgetisnull(res, crow, col):
                    Py_INCREF(None)
                    PyTuple_SET_ITEM(rv, col, None)
                    continue

            val = libpq.PQgetvalue(res, crow, col)
            loader = self._row_loaders[col]
            if loader.pyxloader is not None:
                pyval = loader.pyxloader.cload(val, length)
            else:
                # TODO: no copy
                pyval = loader.pyloader(val[:length])

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(rv, col, pyval)

        return rv

    def load_sequence(
        self, record: Sequence[Optional[bytes]]
    ) -> Tuple[Any, ...]:
        cdef list rv = []
        cdef int i
        cdef RowLoader loader
        for i in range(len(record)):
            item = record[i]
            if item is None:
                rv.append(None)
            else:
                loader = self._row_loaders[i]
                rv.append(loader.pyloader(item))

        return tuple(rv)

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
        func = self._load_funcs[key] = loader(oid, self).load
        return func

    def get_loader(self, oid: int, format: Format) -> "Loader":
        key = (oid, format)
        try:
            return self._loaders_cache[key]
        except KeyError:
            pass

        loader_cls = self.lookup_loader(*key)
        self._loaders_cache[key] = loader = loader_cls(key[0], self)
        return loader

    def lookup_loader(self, oid: int, format: Format) -> "LoaderType":
        key = (oid, format)

        for tcmap in self._loaders_maps:
            if key in tcmap:
                return tcmap[key]

        from psycopg3.adapt import Loader

        return Loader.globals[0, format]    # INVALID_OID
