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


# internal structure: you are not supposed to know this. But it's worth some
# 10% of the innermost loop, so I'm willing to ask for forgiveness later...

ctypedef struct PGresAttValue:
    int     len
    char    *value

ctypedef struct pg_result_int:
    # NOTE: it would be advised that we don't know this structure's content
    int ntups
    int numAttributes
    libpq.PGresAttDesc *attDescs
    PGresAttValue **tuples
    # ...more members, which we ignore


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

    cdef readonly object connection
    cdef readonly object adapters
    cdef dict _dumpers_cache
    cdef dict _text_loaders
    cdef dict _binary_loaders
    cdef PGresult _pgresult
    cdef int _nfields, _ntuples
    cdef list _row_loaders

    def __cinit__(self, context: Optional["AdaptContext"] = None):
        if context is not None:
            self.adapters = context.adapters
            self.connection = context.connection
        else:
            from psycopg3.adapt import global_adapters
            self.adapters = global_adapters
            self.connection = None

        # mapping class, fmt -> Dumper instance
        self._dumpers_cache: Dict[Tuple[type, Format], "Dumper"] = {}

        # mapping oid -> Loader instance (text, binary)
        self._text_loaders = {}
        self._binary_loaders = {}

        self.pgresult = None
        self._row_loaders = []

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
        cdef list types = [None] * self._nfields
        cdef list formats = [None] * self._nfields
        for i in range(self._nfields):
            types[i] = libpq.PQftype(res, i)
            formats[i] = libpq.PQfformat(res, i)
        self.set_row_types(types, formats)

    def set_row_types(self, types: Sequence[int], formats: Sequence[Format]) -> None:
        self._c_set_row_types(types, formats)

    cdef void _c_set_row_types(self, list types, list formats):
        cdef dict text_loaders = {}
        cdef dict binary_loaders = {}
        cdef list loaders = [None] * len(types)
        cdef libpq.Oid oid
        cdef int fmt
        cdef int i
        for i in range(len(types)):
            oid = types[i]
            fmt = formats[i]
            if fmt == 0:
                if oid in text_loaders:
                    loaders[i] = text_loaders[oid]
                else:
                    loaders[i] = text_loaders[oid] \
                        = self._get_row_loader(oid, fmt)
            else:
                if oid in binary_loaders:
                    loaders[i] = binary_loaders[oid]
                else:
                    loaders[i] = binary_loaders[oid] \
                        = self._get_row_loader(oid, fmt)

        self._row_loaders = loaders

    cdef RowLoader _get_row_loader(self, libpq.Oid oid, int fmt):
        cdef RowLoader row_loader = RowLoader()
        loader = self._c_get_loader(oid, fmt)
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
        cdef dict dmap = self.adapters._dumpers
        for scls in cls.__mro__:
            dumper_class = dmap.get((scls, format))
            if not dumper_class:
                continue

            self._dumpers_cache[cls, format] = dumper = dumper_class(cls, self)
            return dumper

        # If the adapter is not found, look for its name as a string
        for scls in cls.__mro__:
            fqn = f"{cls.__module__}.{scls.__qualname__}"
            dumper_class = dmap.get((fqn, format))
            if dumper_class is None:
                continue

            dmap[cls, format] = dumper_class
            self._dumpers_cache[cls, format] = dumper = dumper_class(cls, self)
            return dumper

        raise e.ProgrammingError(
            f"cannot adapt type {type(obj).__name__}"
            f" to format {Format(format).name}"
        )

    def load_row(self, row: int) -> Optional[Tuple[Any, ...]]:
        if self._pgresult is None:
            return None

        cdef int crow = row
        if not 0 <= crow < self._ntuples:
            return None

        cdef libpq.PGresult *res = self._pgresult.pgresult_ptr

        cdef RowLoader loader
        cdef int col
        cdef int length
        cdef const char *val

        # cheeky access to the internal PGresult structure
        cdef pg_result_int *ires = <pg_result_int*>res
        cdef PGresAttValue *attval

        rv = PyTuple_New(self._nfields)
        for col in range(self._nfields):
            attval = &(ires.tuples[crow][col])
            length = attval.len
            if length == -1:  # NULL_LEN
                Py_INCREF(None)
                PyTuple_SET_ITEM(rv, col, None)
                continue

            # TODO: the is some visible python churn around this lookup.
            # replace with a C array of borrowed references pointing to
            # the cloader.cload function pointer
            loader = self._row_loaders[col]
            val = attval.value
            if loader.cloader is not None:
                pyval = loader.cloader.cload(val, length)
            else:
                # TODO: no copy
                pyval = loader.pyloader(val[:length])

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(rv, col, pyval)

        return rv

    def load_sequence(self, record: Sequence[Optional[bytes]]) -> Tuple[Any, ...]:
        cdef int length = len(record)
        rv = PyTuple_New(length)
        cdef RowLoader loader

        cdef int i
        for i in range(length):
            item = record[i]
            if item is None:
                pyval = None
            else:
                loader = self._row_loaders[i]
                pyval = loader.pyloader(item)
            Py_INCREF(pyval)
            PyTuple_SET_ITEM(rv, i, pyval)

        return rv

    def get_loader(self, oid: int, format: Format) -> "Loader":
        return self._c_get_loader(oid, format)

    cdef object _c_get_loader(self, libpq.Oid oid, int format):
        cdef dict cache
        if format == 0:
            cache = self._text_loaders
        else:
            cache = self._binary_loaders

        if oid in cache:
            return cache[oid]

        loader_cls = self.adapters._loaders.get((oid, format))
        if loader_cls is None:
            loader_cls = self.adapters._loaders[oids.INVALID_OID, format]
        loader = cache[oid] = loader_cls(oid, self)
        return loader
