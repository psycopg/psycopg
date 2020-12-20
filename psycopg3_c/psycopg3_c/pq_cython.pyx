"""
libpq Python wrapper using cython bindings.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.string cimport strlen
from posix.unistd cimport getpid
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AsString, PyBytes_AsStringAndSize
from cpython.buffer cimport PyObject_CheckBuffer, PyBUF_SIMPLE
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_AS_STRING

import logging
from typing import List, Optional, Sequence, Tuple

from psycopg3_c.pq cimport libpq as impl
from psycopg3_c.pq.libpq cimport Oid

from psycopg3.pq.misc import PGnotify, ConninfoOption, PQerror, PGresAttDesc
from psycopg3.pq.misc import error_message
from psycopg3.pq import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)


__impl__ = 'c'

logger = logging.getLogger('psycopg3')


def version():
    return impl.PQlibVersion()


cdef void notice_receiver(void *arg, const impl.PGresult *res_ptr) with gil:
    cdef PGconn pgconn = <object>arg
    if pgconn.notice_handler is None:
        return

    cdef PGresult res = PGresult._from_ptr(<impl.PGresult *>res_ptr)
    try:
        pgconn.notice_handler(res)
    except Exception as e:
        logger.exception("error in notice receiver: %s", e)

    res.pgresult_ptr = NULL  # avoid destroying the pgresult_ptr


include "pq/pgconn.pyx"


cdef (int, Oid *, char * const*, int *, int *) _query_params_args(
    list param_values: Optional[Sequence[Optional[bytes]]],
    param_types: Optional[Sequence[int]],
    list param_formats: Optional[Sequence[Format]],
) except *:
    cdef int i

    # the PostgresQuery convers the param_types to tuple, so this operation
    # is most often no-op
    cdef tuple tparam_types
    if param_types is not None and not isinstance(param_types, tuple):
        tparam_types = tuple(param_types)
    else:
        tparam_types = param_types

    cdef int nparams = len(param_values) if param_values else 0
    if tparam_types is not None and len(tparam_types) != nparams:
        raise ValueError(
            "got %d param_values but %d param_types"
            % (nparams, len(tparam_types))
        )
    if param_formats is not None and len(param_formats) != nparams:
        raise ValueError(
            "got %d param_values but %d param_formats"
            % (nparams, len(param_formats))
        )

    cdef char **aparams = NULL
    cdef int *alenghts = NULL
    cdef char *ptr
    cdef Py_ssize_t length

    if nparams:
        aparams = <char **>PyMem_Malloc(nparams * sizeof(char *))
        alenghts = <int *>PyMem_Malloc(nparams * sizeof(int))
        for i in range(nparams):
            obj = param_values[i]
            if obj is None:
                aparams[i] = NULL
                alenghts[i] = 0
            else:
                # TODO: it is a leak if this fails (but it should only fail
                # on internal error, e.g. if obj is not a buffer)
                _buffer_as_string_and_size(obj, &ptr, &length)
                aparams[i] = ptr
                alenghts[i] = length

    cdef Oid *atypes = NULL
    if tparam_types:
        atypes = <Oid *>PyMem_Malloc(nparams * sizeof(Oid))
        for i in range(nparams):
            atypes[i] = tparam_types[i]

    cdef int *aformats = NULL
    if param_formats is not None:
        aformats = <int *>PyMem_Malloc(nparams * sizeof(int *))
        for i in range(nparams):
            aformats[i] = param_formats[i]

    return (nparams, atypes, aparams, alenghts, aformats)


cdef void _clear_query_params(
    Oid *ctypes, char *const *cvalues, int *clenghts, int *cformats
):
    PyMem_Free(ctypes)
    PyMem_Free(<char **>cvalues)
    PyMem_Free(clenghts)
    PyMem_Free(cformats)


cdef _options_from_array(impl.PQconninfoOption *opts):
    rv = []
    cdef int i = 0
    cdef impl.PQconninfoOption* opt
    while 1:
        opt = opts + i
        if opt.keyword is NULL:
            break
        rv.append(
            ConninfoOption(
                keyword=opt.keyword,
                envvar=opt.envvar if opt.envvar is not NULL else None,
                compiled=opt.compiled if opt.compiled is not NULL else None,
                val=opt.val if opt.val is not NULL else None,
                label=opt.label if opt.label is not NULL else None,
                dispchar=opt.dispchar if opt.dispchar is not NULL else None,
                dispsize=opt.dispsize,
            )
        )
        i += 1

    return rv


cdef class PGresult:
    def __cinit__(self):
        self.pgresult_ptr = NULL

    @staticmethod
    cdef PGresult _from_ptr(impl.PGresult *ptr):
        cdef PGresult rv = PGresult.__new__(PGresult)
        rv.pgresult_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.clear()

    def clear(self) -> None:
        if self.pgresult_ptr is not NULL:
            impl.PQclear(self.pgresult_ptr)
            self.pgresult_ptr = NULL

    @property
    def pgresult_ptr(self) -> Optional[int]:
        if self.pgresult_ptr:
            return <long><void *>self.pgresult_ptr
        else:
            return None

    @property
    def status(self) -> ExecStatus:
        cdef int rv = impl.PQresultStatus(self.pgresult_ptr)
        return ExecStatus(rv)

    @property
    def error_message(self) -> bytes:
        return impl.PQresultErrorMessage(self.pgresult_ptr)

    def error_field(self, fieldcode: DiagnosticField) -> Optional[bytes]:
        cdef char * rv = impl.PQresultErrorField(self.pgresult_ptr, fieldcode)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def ntuples(self) -> int:
        return impl.PQntuples(self.pgresult_ptr)

    @property
    def nfields(self) -> int:
        return impl.PQnfields(self.pgresult_ptr)

    def fname(self, column_number: int) -> Optional[bytes]:
        cdef char *rv = impl.PQfname(self.pgresult_ptr, column_number)
        if rv is not NULL:
            return rv
        else:
            return None

    def ftable(self, column_number: int) -> int:
        return impl.PQftable(self.pgresult_ptr, column_number)

    def ftablecol(self, column_number: int) -> int:
        return impl.PQftablecol(self.pgresult_ptr, column_number)

    def fformat(self, column_number: int) -> Format:
        return Format(impl.PQfformat(self.pgresult_ptr, column_number))

    def ftype(self, column_number: int) -> int:
        return impl.PQftype(self.pgresult_ptr, column_number)

    def fmod(self, column_number: int) -> int:
        return impl.PQfmod(self.pgresult_ptr, column_number)

    def fsize(self, column_number: int) -> int:
        return impl.PQfsize(self.pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> Format:
        return Format(impl.PQbinaryTuples(self.pgresult_ptr))

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        cdef int crow = row_number
        cdef int ccol = column_number
        cdef int length = impl.PQgetlength(self.pgresult_ptr, crow, ccol)
        cdef char *v;
        if length:
            v = impl.PQgetvalue(self.pgresult_ptr, crow, ccol)
            # TODO: avoid copy
            return v[:length]
        else:
            if impl.PQgetisnull(self.pgresult_ptr, crow, ccol):
                return None
            else:
                return b""

    @property
    def nparams(self) -> int:
        return impl.PQnparams(self.pgresult_ptr)

    def param_type(self, param_number: int) -> int:
        return impl.PQparamtype(self.pgresult_ptr, param_number)

    @property
    def command_status(self) -> Optional[bytes]:
        cdef char *rv = impl.PQcmdStatus(self.pgresult_ptr)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def command_tuples(self) -> Optional[int]:
        cdef char *rv = impl.PQcmdTuples(self.pgresult_ptr)
        if rv is NULL:
            return None
        cdef bytes brv = rv
        return int(brv) if brv else None

    @property
    def oid_value(self) -> int:
        return impl.PQoidValue(self.pgresult_ptr)

    def set_attributes(self, descriptions: List[PGresAttDesc]):
        cdef int num = len(descriptions)
        cdef impl.PGresAttDesc *attrs = <impl.PGresAttDesc *>PyMem_Malloc(
            num * sizeof(impl.PGresAttDesc))

        for i in range(num):
            descr = descriptions[i]
            attrs[i].name = descr.name
            attrs[i].tableid = descr.tableid
            attrs[i].columnid = descr.columnid
            attrs[i].format = descr.format
            attrs[i].typid = descr.typid
            attrs[i].typlen = descr.typlen
            attrs[i].atttypmod = descr.atttypmod

        cdef int res = impl.PQsetResultAttrs(self.pgresult_ptr, num, attrs);
        PyMem_Free(attrs)
        if (res == 0):
            raise PQerror("PQsetResultAttrs failed")


cdef class PGcancel:
    def __cinit__(self):
        self.pgcancel_ptr = NULL

    @staticmethod
    cdef PGcancel _from_ptr(impl.PGcancel *ptr):
        cdef PGcancel rv = PGcancel.__new__(PGcancel)
        rv.pgcancel_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.free()

    def free(self) -> None:
        if self.pgcancel_ptr is not NULL:
            impl.PQfreeCancel(self.pgcancel_ptr)
            self.pgcancel_ptr = NULL

    def cancel(self) -> None:
        cdef char buf[256]
        cdef int res = impl.PQcancel(self.pgcancel_ptr, buf, sizeof(buf))
        if not res:
            raise PQerror(
                f"cancel failed: {buf.decode('utf8', 'ignore')}"
            )


class Conninfo:
    @classmethod
    def get_defaults(cls) -> List[ConninfoOption]:
        cdef impl.PQconninfoOption *opts = impl.PQconndefaults()
        if opts is NULL :
            raise MemoryError("couldn't allocate connection defaults")
        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    @classmethod
    def parse(cls, conninfo: bytes) -> List[ConninfoOption]:
        cdef char *errmsg = NULL
        cdef impl.PQconninfoOption *opts = impl.PQconninfoParse(conninfo, &errmsg)
        if opts is NULL:
            if errmsg is NULL:
                raise MemoryError("couldn't allocate on conninfo parse")
            else:
                exc = PQerror(errmsg.decode("utf8", "replace"))
                impl.PQfreemem(errmsg)
                raise exc

        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    def __repr__(self):
        return f"<{type(self).__name__} ({self.keyword.decode('ascii')})>"


include "pq/escaping.pyx"


cdef class PQBuffer:
    """
    Wrap a chunk of memory allocated by the libpq and expose it as memoryview.
    """
    @staticmethod
    cdef PQBuffer _from_buffer(unsigned char *buf, Py_ssize_t len):
        cdef PQBuffer rv = PQBuffer.__new__(PQBuffer)
        rv.buf = buf
        rv.len = len
        return rv

    def __cinit__(self):
        self.buf = NULL
        self.len = 0

    def __dealloc__(self):
        if self.buf:
            impl.PQfreemem(self.buf)

    def __repr__(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
            f"({bytes(self)})"
        )

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        buffer.buf = self.buf
        buffer.obj = self
        buffer.len = self.len
        buffer.itemsize = sizeof(unsigned char)
        buffer.readonly = 1
        buffer.ndim = 1
        buffer.format = NULL  # unsigned char
        buffer.shape = &self.len
        buffer.strides = NULL
        buffer.suboffsets = NULL
        buffer.internal = NULL

    def __releasebuffer__(self, Py_buffer *buffer):
        pass


cdef int _buffer_as_string_and_size(
    data: "Buffer", char **ptr, Py_ssize_t *length
) except -1:
    cdef Py_buffer buf

    if isinstance(data, bytes):
        PyBytes_AsStringAndSize(data, ptr, length)
    elif PyObject_CheckBuffer(data):
        PyObject_GetBuffer(data, &buf, PyBUF_SIMPLE)
        ptr[0] = <char *>buf.buf
        length[0] = buf.len
        PyBuffer_Release(&buf)
    else:
        raise TypeError(f"bytes or buffer expected, got {type(data)}")
