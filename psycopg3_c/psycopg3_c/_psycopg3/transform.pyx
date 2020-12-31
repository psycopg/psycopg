"""
Helper object to transform values between Python and PostgreSQL

Cython implementation: can access to lower level C features without creating
too many temporary Python objects and performing less memory copying.

"""

# Copyright (C) 2020 The Psycopg Team

from cpython.ref cimport Py_INCREF
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.list cimport PyList_New, PyList_GET_ITEM, PyList_SET_ITEM
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject, PyObject_CallFunctionObjArgs

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
    cdef dict _text_dumpers
    cdef dict _binary_dumpers
    cdef dict _text_loaders
    cdef dict _binary_loaders
    cdef pq.PGresult _pgresult
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

        # mapping class -> Dumper instance (text, binary)
        self._text_dumpers = {}
        self._binary_dumpers = {}

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
        cdef object tmp
        cdef list types = PyList_New(self._nfields)
        cdef list formats = PyList_New(self._nfields)
        for i in range(self._nfields):
            tmp = libpq.PQftype(res, i)
            Py_INCREF(tmp)
            PyList_SET_ITEM(types, i, tmp)

            tmp = libpq.PQfformat(res, i)
            Py_INCREF(tmp)
            PyList_SET_ITEM(formats, i, tmp)

        self._c_set_row_types(self._nfields, types, formats)

    def set_row_types(self,
            types: Sequence[int], formats: Sequence[Format]) -> None:
        self._c_set_row_types(len(types), types, formats)

    cdef void _c_set_row_types(self, int ntypes, list types, list formats):
        cdef list loaders = PyList_New(ntypes)
        cdef object loader
        cdef dict text_loaders = {}
        cdef dict binary_loaders = {}

        # these are used more as Python object than C
        cdef object oid
        cdef object fmt
        cdef PyObject *ptr
        for i in range(ntypes):
            oid = <object>PyList_GET_ITEM(types, i)
            fmt = <object>PyList_GET_ITEM(formats, i)
            if fmt == 0:
                ptr = PyDict_GetItem(text_loaders, oid)
                if ptr != NULL:
                    loader = <object>ptr
                else:
                    loader = self._get_row_loader(oid, fmt)
                    PyDict_SetItem(text_loaders, oid, loader)
            else:
                ptr = PyDict_GetItem(binary_loaders, oid)
                if ptr != NULL:
                    loader = <object>ptr
                else:
                    loader = self._get_row_loader(oid, fmt)
                    PyDict_SetItem(binary_loaders, oid, loader)

            Py_INCREF(loader)
            PyList_SET_ITEM(loaders, i, loader)

        self._row_loaders = loaders

    cdef RowLoader _get_row_loader(self, object oid, object fmt):
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
        cdef dict cache
        cdef PyObject *ptr

        cls = type(obj)
        cache = self._binary_dumpers if format else self._text_dumpers
        ptr = PyDict_GetItem(cache, cls)
        if ptr != NULL:
            return <object>ptr

        dumper_class = self.adapters.get_dumper(cls, format)
        if dumper_class:
            d = dumper_class(cls, self)
            cache[cls] = d
            return d

        raise e.ProgrammingError(
            f"cannot adapt type {cls.__name__} to format {Format(format).name}"
        )

    def load_rows(self, int row0, int row1) -> Sequence[Tuple[Any, ...]]:
        if self._pgresult is None:
            raise e.InterfaceError("result not set")

        if not (0 <= row0 <= self._ntuples and 0 <= row1 <= self._ntuples):
            raise e.InterfaceError(
                f"rows must be included between 0 and {self._ntuples}"
            )

        cdef libpq.PGresult *res = self._pgresult.pgresult_ptr
        # cheeky access to the internal PGresult structure
        cdef pg_result_int *ires = <pg_result_int*>res

        cdef int row
        cdef int col
        cdef PGresAttValue *attval
        cdef const char *val
        cdef object record  # not 'tuple' as it would check on assignment

        cdef object records = PyList_New(row1 - row0)
        for row in range(row0, row1):
            record = PyTuple_New(self._nfields)
            Py_INCREF(record)
            PyList_SET_ITEM(records, row - row0, record)

        cdef RowLoader loader
        cdef CLoader cloader
        cdef object pyloader
        cdef PyObject *brecord  # borrowed
        for col in range(self._nfields):
            # TODO: the is some visible python churn around this lookup.
            # replace with a C array of borrowed references pointing to
            # the cloader.cload function pointer
            loader = self._row_loaders[col]
            if loader.cloader is not None:
                cloader = loader.cloader

                for row in range(row0, row1):
                    brecord = PyList_GET_ITEM(records, row - row0)
                    attval = &(ires.tuples[row][col])
                    if attval.len == -1:  # NULL_LEN
                        Py_INCREF(None)
                        PyTuple_SET_ITEM(<object>brecord, col, None)
                        continue

                    pyval = loader.cloader.cload(attval.value, attval.len)
                    Py_INCREF(pyval)
                    PyTuple_SET_ITEM(<object>brecord, col, pyval)

            else:
                pyloader = loader.pyloader

                for row in range(row0, row1):
                    brecord = PyList_GET_ITEM(records, row - row0)
                    attval = &(ires.tuples[row][col])
                    if attval.len == -1:  # NULL_LEN
                        Py_INCREF(None)
                        PyTuple_SET_ITEM(<object>brecord, col, None)
                        continue

                    # TODO: no copy
                    b = attval.value[:attval.len]
                    pyval = PyObject_CallFunctionObjArgs(
                        pyloader, <PyObject *>b, NULL)
                    Py_INCREF(pyval)
                    PyTuple_SET_ITEM(<object>brecord, col, pyval)

        return records

    def load_row(self, int row) -> Optional[Tuple[Any, ...]]:
        if self._pgresult is None:
            return None

        if not 0 <= row < self._ntuples:
            return None

        cdef libpq.PGresult *res = self._pgresult.pgresult_ptr
        # cheeky access to the internal PGresult structure
        cdef pg_result_int *ires = <pg_result_int*>res

        cdef RowLoader loader
        cdef int col
        cdef PGresAttValue *attval
        cdef const char *val
        cdef object record  # not 'tuple' as it would check on assignment

        record = PyTuple_New(self._nfields)
        for col in range(self._nfields):
            attval = &(ires.tuples[row][col])
            if attval.len == -1:  # NULL_LEN
                Py_INCREF(None)
                PyTuple_SET_ITEM(record, col, None)
                continue

            # TODO: the is some visible python churn around this lookup.
            # replace with a C array of borrowed references pointing to
            # the cloader.cload function pointer
            loader = self._row_loaders[col]
            val = attval.value
            if loader.cloader is not None:
                pyval = loader.cloader.cload(val, attval.len)
            else:
                # TODO: no copy
                b = val[:attval.len]
                pyval = PyObject_CallFunctionObjArgs(
                    loader.pyloader, <PyObject *>b, NULL)

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(record, col, pyval)

        return record

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

    cdef object _c_get_loader(self, object oid, object format):
        cdef dict cache
        cdef PyObject *ptr

        cache = self._binary_loaders if format else self._text_loaders
        ptr = PyDict_GetItem(cache, oid)
        if ptr != NULL:
            return <object>ptr

        loader_cls = self.adapters.get_loader(oid, format)
        if not loader_cls:
            loader_cls = self.adapters.get_loader(oids.INVALID_OID, format)
            if not loader_cls:
                raise e.InterfaceError("unknown oid loader not found")

        loader = loader_cls(oid, self)
        PyDict_SetItem(cache, oid, loader)
        return loader
