"""
Helper object to transform values between Python and PostgreSQL

Cython implementation: can access to lower level C features without creating
too many temporary Python objects and performing less memory copying.

"""

# Copyright (C) 2020 The Psycopg Team

from cpython.ref cimport Py_INCREF
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.list cimport (
    PyList_New, PyList_GET_ITEM, PyList_SET_ITEM, PyList_GET_SIZE)
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject, PyObject_CallFunctionObjArgs

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg3 import errors as e


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
    cdef int _unknown_oid

    def __cinit__(self, context: Optional["AdaptContext"] = None):
        self._unknown_oid = oids.INVALID_OID
        if context is not None:
            self.adapters = context.adapters
            self.connection = context.connection

            # PG 9.6 gives an error if an unknown oid is emitted as column
            if self.connection and self.connection.pgconn.server_version < 100000:
                self._unknown_oid = oids.TEXT_OID
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
        self.set_pgresult(result)

    cdef void set_pgresult(self, pq.PGresult result):
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
        cdef PyObject *oid
        cdef PyObject *fmt
        cdef PyObject *ptr
        cdef PyObject *cache
        for i in range(ntypes):
            oid = PyList_GET_ITEM(types, i)
            fmt = PyList_GET_ITEM(formats, i)
            cache = <PyObject *>(binary_loaders if <object>fmt else text_loaders)
            ptr = PyDict_GetItem(<object>cache, <object>oid)
            if ptr != NULL:
                loader = <object>ptr
            else:
                loader = self._get_row_loader(oid, fmt)
                PyDict_SetItem(<object>cache, <object>oid, loader)

            Py_INCREF(loader)
            PyList_SET_ITEM(loaders, i, loader)

        self._row_loaders = loaders

    cdef RowLoader _get_row_loader(self, PyObject *oid, PyObject *fmt):
        cdef RowLoader row_loader = RowLoader()
        loader = self._c_get_loader(oid, fmt)
        row_loader.pyloader = loader.load

        if isinstance(loader, CLoader):
            row_loader.cloader = loader
        else:
            row_loader.cloader = None

        return row_loader

    cpdef object get_dumper(self, object obj, object format):
        # Fast path: return a Dumper class already instantiated from the same type
        cdef dict cache
        cdef PyObject *ptr

        cls = type(obj)
        cache = self._binary_dumpers if format else self._text_dumpers
        ptr = PyDict_GetItem(cache, cls)
        if ptr != NULL:
            return <object>ptr

        dumper_class = PyObject_CallFunctionObjArgs(
            self.adapters.get_dumper, <PyObject *>cls, <PyObject *>format, NULL)
        if dumper_class is not None:
            d = PyObject_CallFunctionObjArgs(
                dumper_class, <PyObject *>cls, <PyObject *>self, NULL)
            PyDict_SetItem(cache, cls, d)
            return d

        raise e.ProgrammingError(
            f"cannot adapt type {cls.__name__} to format {Format(format).name}"
        )

    cpdef dump_sequence(self, object params, object formats):
        # Verify that they are not none and that PyList_GET_ITEM won't blow up
        cdef int nparams = len(params)
        cdef list ps = PyList_New(nparams)
        cdef tuple ts = PyTuple_New(nparams)
        cdef object dumped, oid
        cdef Py_ssize_t size

        cdef int i
        for i in range(nparams):
            param = params[i]
            if param is not None:
                format = formats[i]
                dumper = self.get_dumper(param, format)
                if isinstance(dumper, CDumper):
                    dumped = PyByteArray_FromStringAndSize("", 0)
                    size = (<CDumper>dumper).cdump(param, <bytearray>dumped, 0)
                    PyByteArray_Resize(dumped, size)
                    oid = (<CDumper>dumper).oid
                else:
                    dumped = dumper.dump(param)
                    oid = dumper.oid
            else:
                dumped = None
                oid = self._unknown_oid

            Py_INCREF(dumped)
            PyList_SET_ITEM(ps, i, dumped)
            Py_INCREF(oid)
            PyTuple_SET_ITEM(ts, i, oid)

        return ps, ts

    def load_rows(self, int row0, int row1) -> List[Tuple[Any, ...]]:
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

        cdef PyObject *loader  # borrowed RowLoader
        cdef PyObject *brecord  # borrowed
        row_loaders = self._row_loaders  # avoid an incref/decref per item

        for col in range(self._nfields):
            loader = PyList_GET_ITEM(row_loaders, col)
            if (<RowLoader>loader).cloader is not None:
                for row in range(row0, row1):
                    brecord = PyList_GET_ITEM(records, row - row0)
                    attval = &(ires.tuples[row][col])
                    if attval.len == -1:  # NULL_LEN
                        Py_INCREF(None)
                        PyTuple_SET_ITEM(<object>brecord, col, None)
                        continue

                    pyval = (<RowLoader>loader).cloader.cload(
                        attval.value, attval.len)
                    Py_INCREF(pyval)
                    PyTuple_SET_ITEM(<object>brecord, col, pyval)

            else:
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
                        (<RowLoader>loader).pyloader, <PyObject *>b, NULL)
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

        cdef PyObject *loader  # borrowed RowLoader
        cdef int col
        cdef PGresAttValue *attval
        cdef const char *val
        cdef object record  # not 'tuple' as it would check on assignment

        record = PyTuple_New(self._nfields)
        row_loaders = self._row_loaders  # avoid an incref/decref per item

        for col in range(self._nfields):
            attval = &(ires.tuples[row][col])
            if attval.len == -1:  # NULL_LEN
                Py_INCREF(None)
                PyTuple_SET_ITEM(record, col, None)
                continue

            val = attval.value
            loader = PyList_GET_ITEM(row_loaders, col)
            if (<RowLoader>loader).cloader is not None:
                pyval = (<RowLoader>loader).cloader.cload(val, attval.len)
            else:
                # TODO: no copy
                b = val[:attval.len]
                pyval = PyObject_CallFunctionObjArgs(
                    (<RowLoader>loader).pyloader, <PyObject *>b, NULL)

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(record, col, pyval)

        return record

    cpdef object load_sequence(self, record: Sequence[Optional[bytes]]):
        cdef int nfields = len(record)
        out = PyTuple_New(nfields)
        cdef PyObject *loader  # borrowed RowLoader
        cdef int col
        cdef char *ptr
        cdef Py_ssize_t size

        row_loaders = self._row_loaders  # avoid an incref/decref per item
        if PyList_GET_SIZE(row_loaders) != nfields:
            raise e.ProgrammingError(
                f"cannot load sequence of {nfields} items:"
                f" {len(self._row_loaders)} loaders registered")

        for col in range(nfields):
            item = record[col]
            if item is None:
                Py_INCREF(None)
                PyTuple_SET_ITEM(out, col, None)
                continue

            loader = PyList_GET_ITEM(row_loaders, col)
            if (<RowLoader>loader).cloader is not None:
                _buffer_as_string_and_size(item, &ptr, &size)
                pyval = (<RowLoader>loader).cloader.cload(ptr, size)
            else:
                pyval = PyObject_CallFunctionObjArgs(
                    (<RowLoader>loader).pyloader, <PyObject *>item, NULL)

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(out, col, pyval)

        return out

    def get_loader(self, oid: int, format: Format) -> "Loader":
        return self._c_get_loader(<PyObject *>oid, <PyObject *>format)

    cdef object _c_get_loader(self, PyObject *oid, PyObject *fmt):
        cdef PyObject *ptr
        cdef PyObject *cache

        cache = <PyObject *>(
            self._binary_loaders if <object>fmt == 0 else self._text_loaders)
        ptr = PyDict_GetItem(<object>cache, <object>oid)
        if ptr != NULL:
            return <object>ptr

        loader_cls = self.adapters.get_loader(<object>oid, <object>fmt)
        if loader_cls is None:
            loader_cls = self.adapters.get_loader(oids.INVALID_OID, <object>fmt)
            if loader_cls is None:
                raise e.InterfaceError("unknown oid loader not found")

        loader = PyObject_CallFunctionObjArgs(
            loader_cls, oid, <PyObject *>self, NULL)
        PyDict_SetItem(<object>cache, <object>oid, loader)
        return loader
