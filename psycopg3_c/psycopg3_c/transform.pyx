"""
Helper object to transform values between Python and PostgreSQL

Cython implementation: can access to lower level C features without creating
too many temporary Python objects and performing less memory copying.

"""

# Copyright (C) 2020 The Psycopg Team

from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg3_c cimport oids
from psycopg3_c.pq cimport libpq
from psycopg3_c.pq_cython cimport PGresult

from psycopg3 import errors as e
from psycopg3.pq import Format


cdef class RowLoader:
    cdef object pyloader
    cdef CLoader cloader


cdef class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    cdef readonly dict dumpers, loaders
    cdef readonly object connection
    cdef readonly str encoding
    cdef list _dumpers_maps, _loaders_maps
    cdef dict _dumpers_cache, _loaders_cache, _load_funcs
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
            self.connection = None
            self.encoding = "utf-8"
            self.dumpers = {}
            self.loaders = {}
            self._dumpers_maps = [self.dumpers]
            self._loaders_maps = [self.loaders]

        elif isinstance(context, Transformer):
            # A transformer created from a transformers: usually it happens
            # for nested types: share the entire state of the parent
            ctx = context
            self.connection = ctx.connection
            self.encoding = ctx.encoding
            self.dumpers = ctx.dumpers
            self.loaders = ctx.loaders
            self._dumpers_maps.extend(ctx._dumpers_maps)
            self._loaders_maps.extend(ctx._loaders_maps)
            # the global maps are already in the lists
            return

        elif isinstance(context, BaseCursor):
            self.connection = context.connection
            self.encoding = context.connection.client_encoding
            self.dumpers = {}
            self._dumpers_maps.extend(
                (self.dumpers, context.dumpers, self.connection.dumpers)
            )
            self.loaders = {}
            self._loaders_maps.extend(
                (self.loaders, context.loaders, self.connection.loaders)
            )

        elif isinstance(context, BaseConnection):
            self.connection = context
            self.encoding = context.client_encoding
            self.dumpers = {}
            self._dumpers_maps.extend((self.dumpers, context.dumpers))
            self.loaders = {}
            self._loaders_maps.extend((self.loaders, context.loaders))

        self._dumpers_maps.append(Dumper.globals)
        self._loaders_maps.append(Loader.globals)

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

        if isinstance(loader, CLoader):
            row_loader.cloader = loader
        else:
            row_loader.cloader = None

        return row_loader

    def get_dumper(self, obj: Any, format: Format) -> "Dumper":
        # Fast path: return a Dumper class already instantiated from the same type
        cls = type(obj)
        try:
            return self._dumpers_cache[cls, format]
        except KeyError:
            pass

        # We haven't seen this type in this query yet. Look for an adapter
        # in contexts from the most specific to the most generic.
        # Also look for superclasses: if you can adapt a type you should be
        # able to adapt its subtypes, otherwise Liskov is sad.
        for dmap in self._dumpers_maps:
            for scls in cls.__mro__:
                dumper_class = dmap.get((scls, format))
                if not dumper_class:
                    continue

                self._dumpers_cache[cls, format] = dumper = dumper_class(cls, self)
                return dumper

        # If the adapter is not found, look for its name as a string
        for dmap in self._dumpers_maps:
            for scls in cls.__mro__:
                fqn = f"{cls.__module__}.{scls.__qualname__}"
                dumper_class = dmap.get((fqn, format))
                if dumper_class is None:
                    continue

                key = (cls, format)
                dmap[key] = dumper_class
                self._dumpers_cache[key] = dumper = dumper_class(cls, self)
                return dumper

        raise e.ProgrammingError(
            f"cannot adapt type {type(obj).__name__}"
            f" to format {Format(format).name}"
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
            if loader.cloader is not None:
                pyval = loader.cloader.cload(val, length)
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

    def get_loader(self, oid: int, format: Format) -> "Loader":
        key = (oid, format)
        try:
            return self._loaders_cache[key]
        except KeyError:
            pass

        for tcmap in self._loaders_maps:
            if key in tcmap:
                loader_cls = tcmap[key]
                break
        else:
            from psycopg3.adapt import Loader
            loader_cls = Loader.globals[oids.INVALID_OID, format]

        self._loaders_cache[key] = loader = loader_cls(key[0], self)
        return loader
