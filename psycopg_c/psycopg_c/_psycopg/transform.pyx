"""
Helper object to transform values between Python and PostgreSQL

Cython implementation: can access to lower level C features without creating
too many temporary Python objects and performing less memory copying.

"""

# Copyright (C) 2020 The Psycopg Team

cimport cython
from cpython.ref cimport Py_INCREF, Py_DECREF
from cpython.set cimport PySet_Add, PySet_Contains
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.list cimport (
    PyList_New, PyList_CheckExact,
    PyList_GET_ITEM, PyList_SET_ITEM, PyList_GET_SIZE)
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject, PyObject_CallFunctionObjArgs

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg import errors as e
from psycopg.pq import Format as PqFormat
from psycopg.rows import Row, RowMaker
from psycopg._encodings import pgconn_encoding

NoneType = type(None)

# internal structure: you are not supposed to know this. But it's worth some
# 10% of the innermost loop, so I'm willing to ask for forgiveness later...

ctypedef struct PGresAttValue:
    int len
    char *value

ctypedef struct pg_result_int:
    # NOTE: it would be advised that we don't know this structure's content
    int ntups
    int numAttributes
    libpq.PGresAttDesc *attDescs
    PGresAttValue **tuples
    # ...more members, which we ignore


@cython.freelist(16)
cdef class RowLoader:
    cdef CLoader cloader
    cdef object pyloader
    cdef object loadfunc


@cython.freelist(16)
cdef class RowDumper:
    cdef CDumper cdumper
    cdef object pydumper
    cdef object dumpfunc
    cdef object oid
    cdef object format


cdef class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that attributes
    such as the server version or the connection encoding will not change. The
    object have its state so adapting several values of the same type can be
    optimised.

    """

    cdef readonly object connection
    cdef readonly object adapters
    cdef readonly object types
    cdef readonly object formats
    cdef str _encoding
    cdef int _none_oid

    # mapping class -> Dumper instance (auto, text, binary)
    cdef dict _auto_dumpers
    cdef dict _text_dumpers
    cdef dict _binary_dumpers

    # mapping oid -> Loader instance (text, binary)
    cdef dict _text_loaders
    cdef dict _binary_loaders

    # mapping oid -> Dumper instance (text, binary)
    cdef dict _oid_text_dumpers
    cdef dict _oid_binary_dumpers

    cdef pq.PGresult _pgresult
    cdef int _nfields, _ntuples
    cdef list _row_dumpers
    cdef list _row_loaders

    cdef dict _oid_types

    def __cinit__(self, context: Optional["AdaptContext"] = None):
        if context is not None:
            self.adapters = context.adapters
            self.connection = context.connection
        else:
            from psycopg import postgres
            self.adapters = postgres.adapters
            self.connection = None

        self.types = self.formats = None
        self._none_oid = -1

    @classmethod
    def from_context(cls, context: Optional["AdaptContext"]):
        """
        Return a Transformer from an AdaptContext.

        If the context is a Transformer instance, just return it.
        """
        return _tx_from_context(context)

    @property
    def encoding(self) -> str:
        if not self._encoding:
            conn = self.connection
            self._encoding = pgconn_encoding(conn.pgconn) if conn else "utf-8"
        return self._encoding

    @property
    def pgresult(self) -> Optional[PGresult]:
        return self._pgresult

    cpdef set_pgresult(
        self,
        pq.PGresult result,
        object set_loaders = True,
        object format = None
    ):
        self._pgresult = result

        if result is None:
            self._nfields = self._ntuples = 0
            if set_loaders:
                self._row_loaders = []
            return

        cdef libpq.PGresult *res = self._pgresult._pgresult_ptr
        self._nfields = libpq.PQnfields(res)
        self._ntuples = libpq.PQntuples(res)

        if not set_loaders:
            return

        if not self._nfields:
            self._row_loaders = []
            return

        if format is None:
            format = libpq.PQfformat(res, 0)

        cdef list loaders = PyList_New(self._nfields)
        cdef PyObject *row_loader
        cdef object oid

        cdef int i
        for i in range(self._nfields):
            oid = libpq.PQftype(res, i)
            row_loader = self._c_get_loader(<PyObject *>oid, <PyObject *>format)
            Py_INCREF(<object>row_loader)
            PyList_SET_ITEM(loaders, i, <object>row_loader)

        self._row_loaders = loaders

    def set_dumper_types(self, types: Sequence[int], format: Format) -> None:
        cdef Py_ssize_t ntypes = len(types)
        dumpers = PyList_New(ntypes)
        cdef int i
        for i in range(ntypes):
            oid = types[i]
            dumper_ptr = self.get_dumper_by_oid(
                <PyObject *>oid, <PyObject *>format)
            Py_INCREF(<object>dumper_ptr)
            PyList_SET_ITEM(dumpers, i, <object>dumper_ptr)

        self._row_dumpers = dumpers
        self.types = tuple(types)
        self.formats = [format] * ntypes

    def set_loader_types(self, types: Sequence[int], format: Format) -> None:
        self._c_loader_types(len(types), types, format)

    cdef void _c_loader_types(self, Py_ssize_t ntypes, list types, object format):
        cdef list loaders = PyList_New(ntypes)

        # these are used more as Python object than C
        cdef PyObject *oid
        cdef PyObject *row_loader
        for i in range(ntypes):
            oid = PyList_GET_ITEM(types, i)
            row_loader = self._c_get_loader(oid, <PyObject *>format)
            Py_INCREF(<object>row_loader)
            PyList_SET_ITEM(loaders, i, <object>row_loader)

        self._row_loaders = loaders

    cpdef as_literal(self, obj):
        cdef PyObject *row_dumper = self.get_row_dumper(
            <PyObject *>obj, <PyObject *>PG_TEXT)

        if (<RowDumper>row_dumper).cdumper is not None:
            dumper = (<RowDumper>row_dumper).cdumper
        else:
            dumper = (<RowDumper>row_dumper).pydumper

        rv = dumper.quote(obj)
        oid = dumper.oid
        # If the result is quoted and the oid not unknown or text,
        # add an explicit type cast.
        # Check the last char because the first one might be 'E'.
        if oid and oid != oids.TEXT_OID and rv and rv[-1] == 39:
            if self._oid_types is None:
                self._oid_types = {}
            type_ptr = PyDict_GetItem(<object>self._oid_types, oid)
            if type_ptr == NULL:
                type_sql = b""
                ti = self.adapters.types.get(oid)
                if ti is not None:
                    if oid < 8192:
                        # builtin: prefer "timestamptz" to "timestamp with time zone"
                        type_sql = ti.name.encode(self.encoding)
                    else:
                        type_sql = ti.regtype.encode(self.encoding)
                    if oid == ti.array_oid:
                        type_sql += b"[]"

                type_ptr = <PyObject *>type_sql
                PyDict_SetItem(<object>self._oid_types, oid, type_sql)

            if <object>type_ptr:
                rv = b"%s::%s" % (rv, <object>type_ptr)

        return rv

    def get_dumper(self, obj, format) -> "Dumper":
        cdef PyObject *row_dumper = self.get_row_dumper(
            <PyObject *>obj, <PyObject *>format)
        return (<RowDumper>row_dumper).pydumper

    cdef PyObject *get_row_dumper(self, PyObject *obj, PyObject *fmt) except NULL:
        """
        Return a borrowed reference to the RowDumper for the given obj/fmt.
        """
        # Fast path: return a Dumper class already instantiated from the same type
        cdef PyObject *cache
        cdef PyObject *ptr
        cdef PyObject *ptr1
        cdef RowDumper row_dumper

        # Normally, the type of the object dictates how to dump it
        key = type(<object>obj)

        # Establish where would the dumper be cached
        bfmt = PyUnicode_AsUTF8String(<object>fmt)
        cdef char cfmt = PyBytes_AS_STRING(bfmt)[0]
        if cfmt == b's':
            if self._auto_dumpers is None:
                self._auto_dumpers = {}
            cache = <PyObject *>self._auto_dumpers
        elif cfmt == b'b':
            if self._binary_dumpers is None:
                self._binary_dumpers = {}
            cache = <PyObject *>self._binary_dumpers
        elif cfmt == b't':
            if self._text_dumpers is None:
                self._text_dumpers = {}
            cache = <PyObject *>self._text_dumpers
        else:
            raise ValueError(
                f"format should be a psycopg.adapt.Format, not {<object>fmt}")

        # Reuse an existing Dumper class for objects of the same type
        ptr = PyDict_GetItem(<object>cache, key)
        if ptr == NULL:
            dcls = PyObject_CallFunctionObjArgs(
                self.adapters.get_dumper, <PyObject *>key, fmt, NULL)
            dumper = PyObject_CallFunctionObjArgs(
                dcls, <PyObject *>key, <PyObject *>self, NULL)

            row_dumper = _as_row_dumper(dumper)
            PyDict_SetItem(<object>cache, key, row_dumper)
            ptr = <PyObject *>row_dumper

        # Check if the dumper requires an upgrade to handle this specific value
        if (<RowDumper>ptr).cdumper is not None:
            key1 = (<RowDumper>ptr).cdumper.get_key(<object>obj, <object>fmt)
        else:
            key1 = PyObject_CallFunctionObjArgs(
                (<RowDumper>ptr).pydumper.get_key, obj, fmt, NULL)
        if key1 is key:
            return ptr

        # If it does, ask the dumper to create its own upgraded version
        ptr1 = PyDict_GetItem(<object>cache, key1)
        if ptr1 != NULL:
            return ptr1

        if (<RowDumper>ptr).cdumper is not None:
            dumper = (<RowDumper>ptr).cdumper.upgrade(<object>obj, <object>fmt)
        else:
            dumper = PyObject_CallFunctionObjArgs(
                (<RowDumper>ptr).pydumper.upgrade, obj, fmt, NULL)

        row_dumper = _as_row_dumper(dumper)
        PyDict_SetItem(<object>cache, key1, row_dumper)
        return <PyObject *>row_dumper

    cdef PyObject *get_dumper_by_oid(self, PyObject *oid, PyObject *fmt) except NULL:
        """
        Return a borrowed reference to the RowDumper for the given oid/fmt.
        """
        cdef PyObject *ptr
        cdef PyObject *cache
        cdef RowDumper row_dumper

        # Establish where would the dumper be cached
        cdef int cfmt = <object>fmt
        if cfmt == 0:
            if self._oid_text_dumpers is None:
                self._oid_text_dumpers = {}
            cache = <PyObject *>self._oid_text_dumpers
        elif cfmt == 1:
            if self._oid_binary_dumpers is None:
                self._oid_binary_dumpers = {}
            cache = <PyObject *>self._oid_binary_dumpers
        else:
            raise ValueError(
                f"format should be a psycopg.pq.Format, not {<object>fmt}")

        # Reuse an existing Dumper class for objects of the same type
        ptr = PyDict_GetItem(<object>cache, <object>oid)
        if ptr == NULL:
            dcls = PyObject_CallFunctionObjArgs(
                self.adapters.get_dumper_by_oid, oid, fmt, NULL)
            dumper = PyObject_CallFunctionObjArgs(
                dcls, <PyObject *>NoneType, <PyObject *>self, NULL)

            row_dumper = _as_row_dumper(dumper)
            PyDict_SetItem(<object>cache, <object>oid, row_dumper)
            ptr = <PyObject *>row_dumper

        return ptr

    cpdef dump_sequence(self, object params, object formats):
        # Verify that they are not none and that PyList_GET_ITEM won't blow up
        cdef Py_ssize_t nparams = len(params)
        cdef list out = PyList_New(nparams)

        cdef int i
        cdef PyObject *dumper_ptr  # borrowed pointer to row dumper
        cdef object dumped
        cdef Py_ssize_t size

        if self._none_oid < 0:
            self._none_oid = self.adapters.get_dumper(NoneType, "s").oid

        dumpers = self._row_dumpers

        if dumpers:
            for i in range(nparams):
                param = params[i]
                if param is not None:
                    dumper_ptr = PyList_GET_ITEM(dumpers, i)
                    if (<RowDumper>dumper_ptr).cdumper is not None:
                        dumped = PyByteArray_FromStringAndSize("", 0)
                        size = (<RowDumper>dumper_ptr).cdumper.cdump(
                            param, <bytearray>dumped, 0)
                        PyByteArray_Resize(dumped, size)
                    else:
                        dumped = PyObject_CallFunctionObjArgs(
                            (<RowDumper>dumper_ptr).dumpfunc,
                            <PyObject *>param, NULL)
                else:
                    dumped = None

                Py_INCREF(dumped)
                PyList_SET_ITEM(out, i, dumped)

            return out

        cdef tuple types = PyTuple_New(nparams)
        cdef list pqformats = PyList_New(nparams)

        for i in range(nparams):
            param = params[i]
            if param is not None:
                dumper_ptr = self.get_row_dumper(
                    <PyObject *>param, <PyObject *>formats[i])
                if (<RowDumper>dumper_ptr).cdumper is not None:
                    dumped = PyByteArray_FromStringAndSize("", 0)
                    size = (<RowDumper>dumper_ptr).cdumper.cdump(
                        param, <bytearray>dumped, 0)
                    PyByteArray_Resize(dumped, size)
                else:
                    dumped = PyObject_CallFunctionObjArgs(
                        (<RowDumper>dumper_ptr).dumpfunc,
                        <PyObject *>param, NULL)
                oid = (<RowDumper>dumper_ptr).oid
                fmt = (<RowDumper>dumper_ptr).format
            else:
                dumped = None
                oid = self._none_oid
                fmt = PQ_TEXT

            Py_INCREF(dumped)
            PyList_SET_ITEM(out, i, dumped)

            Py_INCREF(oid)
            PyTuple_SET_ITEM(types, i, oid)

            Py_INCREF(fmt)
            PyList_SET_ITEM(pqformats, i, fmt)

        self.types = types
        self.formats = pqformats
        return out

    def load_rows(self, int row0, int row1, object make_row) -> List[Row]:
        if self._pgresult is None:
            raise e.InterfaceError("result not set")

        if not (0 <= row0 <= self._ntuples and 0 <= row1 <= self._ntuples):
            raise e.InterfaceError(
                f"rows must be included between 0 and {self._ntuples}"
            )

        cdef libpq.PGresult *res = self._pgresult._pgresult_ptr
        # cheeky access to the internal PGresult structure
        cdef pg_result_int *ires = <pg_result_int*>res

        cdef int row
        cdef int col
        cdef PGresAttValue *attval
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
                        pyval = None
                    else:
                        pyval = (<RowLoader>loader).cloader.cload(
                            attval.value, attval.len)

                    Py_INCREF(pyval)
                    PyTuple_SET_ITEM(<object>brecord, col, pyval)

            else:
                for row in range(row0, row1):
                    brecord = PyList_GET_ITEM(records, row - row0)
                    attval = &(ires.tuples[row][col])
                    if attval.len == -1:  # NULL_LEN
                        pyval = None
                    else:
                        b = PyMemoryView_FromObject(
                            ViewBuffer._from_buffer(
                                self._pgresult,
                                <unsigned char *>attval.value, attval.len))
                        pyval = PyObject_CallFunctionObjArgs(
                            (<RowLoader>loader).loadfunc, <PyObject *>b, NULL)

                    Py_INCREF(pyval)
                    PyTuple_SET_ITEM(<object>brecord, col, pyval)

        if make_row is not tuple:
            for i in range(row1 - row0):
                brecord = PyList_GET_ITEM(records, i)
                record = PyObject_CallFunctionObjArgs(
                    make_row, <PyObject *>brecord, NULL)
                Py_INCREF(record)
                PyList_SET_ITEM(records, i, record)
                Py_DECREF(<object>brecord)
        return records

    def load_row(self, int row, object make_row) -> Optional[Row]:
        if self._pgresult is None:
            return None

        if not 0 <= row < self._ntuples:
            return None

        cdef libpq.PGresult *res = self._pgresult._pgresult_ptr
        # cheeky access to the internal PGresult structure
        cdef pg_result_int *ires = <pg_result_int*>res

        cdef PyObject *loader  # borrowed RowLoader
        cdef int col
        cdef PGresAttValue *attval
        cdef object record  # not 'tuple' as it would check on assignment

        record = PyTuple_New(self._nfields)
        row_loaders = self._row_loaders  # avoid an incref/decref per item

        for col in range(self._nfields):
            attval = &(ires.tuples[row][col])
            if attval.len == -1:  # NULL_LEN
                pyval = None
            else:
                loader = PyList_GET_ITEM(row_loaders, col)
                if (<RowLoader>loader).cloader is not None:
                    pyval = (<RowLoader>loader).cloader.cload(
                        attval.value, attval.len)
                else:
                    b = PyMemoryView_FromObject(
                        ViewBuffer._from_buffer(
                            self._pgresult,
                            <unsigned char *>attval.value, attval.len))
                    pyval = PyObject_CallFunctionObjArgs(
                        (<RowLoader>loader).loadfunc, <PyObject *>b, NULL)

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(record, col, pyval)

        if make_row is not tuple:
            record = PyObject_CallFunctionObjArgs(
                make_row, <PyObject *>record, NULL)
        return record

    cpdef object load_sequence(self, record: Sequence[Optional[Buffer]]):
        cdef Py_ssize_t nfields = len(record)
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
                    (<RowLoader>loader).loadfunc, <PyObject *>item, NULL)

            Py_INCREF(pyval)
            PyTuple_SET_ITEM(out, col, pyval)

        return out

    def get_loader(self, oid: int, format: pq.Format) -> "Loader":
        cdef PyObject *row_loader = self._c_get_loader(
            <PyObject *>oid, <PyObject *>format)
        return (<RowLoader>row_loader).pyloader

    cdef PyObject *_c_get_loader(self, PyObject *oid, PyObject *fmt) except NULL:
        """
        Return a borrowed reference to the RowLoader instance for given oid/fmt
        """
        cdef PyObject *ptr
        cdef PyObject *cache

        if <object>fmt == PQ_TEXT:
            if self._text_loaders is None:
                self._text_loaders = {}
            cache = <PyObject *>self._text_loaders
        elif <object>fmt == PQ_BINARY:
            if self._binary_loaders is None:
                self._binary_loaders = {}
            cache = <PyObject *>self._binary_loaders
        else:
            raise ValueError(
                f"format should be a psycopg.pq.Format, not {format}")

        ptr = PyDict_GetItem(<object>cache, <object>oid)
        if ptr != NULL:
            return ptr

        loader_cls = self.adapters.get_loader(<object>oid, <object>fmt)
        if loader_cls is None:
            loader_cls = self.adapters.get_loader(oids.INVALID_OID, <object>fmt)
            if loader_cls is None:
                raise e.InterfaceError("unknown oid loader not found")

        loader = PyObject_CallFunctionObjArgs(
            loader_cls, oid, <PyObject *>self, NULL)

        cdef RowLoader row_loader = RowLoader()
        row_loader.pyloader = loader
        row_loader.loadfunc = loader.load
        if isinstance(loader, CLoader):
            row_loader.cloader = <CLoader>loader

        PyDict_SetItem(<object>cache, <object>oid, row_loader)
        return <PyObject *>row_loader


cdef object _as_row_dumper(object dumper):
    cdef RowDumper row_dumper = RowDumper()

    row_dumper.pydumper = dumper
    row_dumper.dumpfunc = dumper.dump
    row_dumper.oid = dumper.oid
    row_dumper.format = dumper.format

    if isinstance(dumper, CDumper):
        row_dumper.cdumper = <CDumper>dumper

    return row_dumper


cdef Transformer _tx_from_context(object context):
    if isinstance(context, Transformer):
        return context
    else:
        return Transformer(context)
