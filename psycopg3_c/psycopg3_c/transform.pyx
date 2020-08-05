"""
Helper object to transform values between Python and PostgreSQL

Cython implementation: can access to lower level C features without creating
too many temporary Python objects and performing less memory copying.

"""

# Copyright (C) 2020 The Psycopg Team

from libc.string cimport memset
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM

import codecs
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg3_c cimport libpq
from psycopg3_c.pq_cython cimport PGresult

from psycopg3 import errors as e
from psycopg3.pq.enums import Format

TEXT_OID = 25


cdef struct RowLoader:
    PyObject *pyloader  # borrowed
    cloader_func cloader
    void *context
    int own_context


cdef class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    cdef list _dumpers_maps, _loaders_maps
    cdef dict _dumpers, _loaders, _dump_funcs, _load_funcs
    cdef object _connection, _codec
    cdef PGresult _pgresult
    cdef int _nfields, _ntuples

    cdef int _nloaders
    cdef RowLoader *_row_loaders
    cdef list _pyloaders  # only used to keep a reference

    def __cinit__(self, context: "AdaptContext" = None):
        self._dumpers_maps: List["DumpersMap"] = []
        self._loaders_maps: List["LoadersMap"] = []
        self._setup_context(context)

        # mapping class, fmt -> dump function
        self._dump_funcs: Dict[Tuple[type, Format], "DumpFunc"] = {}

        # mapping oid, fmt -> load function
        self._load_funcs: Dict[Tuple[int, Format], "LoadFunc"] = {}

        self._nloaders = 0
        self._row_loaders = NULL
        self._pyloaders = []

        self.pgresult = None

    def __dealloc__(self):
        self._clear_row_loaders()

    cdef _clear_row_loaders(self):
        cdef int i
        for i in range(self._nloaders):
            if self._row_loaders[i].own_context:
                PyMem_Free(self._row_loaders[i].context)
        PyMem_Free(self._row_loaders)
        self._row_loaders = NULL
        self._nloaders = 0

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
        cdef int ntypes = len(types)
        self._clear_row_loaders()
        self._row_loaders = <RowLoader *>PyMem_Malloc(ntypes * sizeof(RowLoader))
        memset(self._row_loaders, 0, ntypes * sizeof(RowLoader))
        self._nloaders = ntypes
        self._pyloaders = []

        cdef int i = 0, j
        cdef dict seen = {}
        for oid_fmt in types:
            if oid_fmt not in seen:
                loader = self._set_loader(i, oid_fmt[0], oid_fmt[1])
                seen[oid_fmt] = i
            else:
                self._copy_loader(seen[oid_fmt], i)

            i += 1

    cdef void _copy_loader(self, int col_from, int col_to):
        # Copy the structure, but we may also copy the pointer to the
        # context, and we don't want to free() it twice, so mark it as
        # "not owned"
        self._row_loaders[col_to] = self._row_loaders[col_from]
        self._row_loaders[col_to].own_context = 0

    cdef void _set_loader(self, int col, libpq.Oid oid, int fmt):
        pyloader = self.get_load_function(oid, fmt)

        cdef RowLoader *loader = self._row_loaders + col
        loader.pyloader = <PyObject *>pyloader
        self._pyloaders.append(pyloader)

        cdef CLoader cloader = cloaders.get(pyloader)

        if cloader is not None:
            # The cloader is a normal Python function
            loader.cloader = cloader.cloader
            return

        cloader = cloaders.get(getattr(pyloader, '__func__', None))
        if cloader is not None and cloader.get_context is not NULL:
            # The cloader is the load() method of a Loader class
            # Extract the context from the Loader instance
            loader.cloader = cloader.cloader
            loader.context = cloader.get_context(pyloader.__self__)
            loader.own_context = 1

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
        func = dumper(src, self).dump
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
        if self._pgresult is None:
            return None

        cdef int crow = row
        if crow >= self._ntuples:
            return None

        cdef libpq.PGresult *res = self._pgresult.pgresult_ptr

        cdef RowLoader *loader
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
            loader = self._row_loaders + col
            if loader.cloader is not NULL:
                pyval = loader.cloader(val, length, loader.context)
            else:
                # TODO: no copy
                pyval = (<object>loader.pyloader)(val[:length])

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(rv, col, pyval)

        return rv

    def load_sequence(
        self, record: Sequence[Optional[bytes]]
    ) -> Tuple[Any, ...]:
        cdef list rv = []
        cdef int i
        cdef RowLoader *loader
        for i in range(len(record)):
            item = record[i]
            if item is None:
                rv.append(None)
            else:
                loader = self._row_loaders + i
                rv.append((<object>loader.pyloader)(item))

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
