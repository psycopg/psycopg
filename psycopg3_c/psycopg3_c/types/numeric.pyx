"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython

from libc.stdint cimport *
from libc.string cimport memcpy, strlen
from cpython.mem cimport PyMem_Free
from cpython.long cimport PyLong_FromString, PyLong_FromLong, PyLong_AsLongLong
from cpython.long cimport PyLong_FromLongLong, PyLong_FromUnsignedLong
from cpython.float cimport PyFloat_FromDouble, PyFloat_AsDouble

from psycopg3_c._psycopg3.endian cimport (
    be16toh, be32toh, be64toh, htobe32, htobe64)

cdef extern from "Python.h":
    # work around https://github.com/cython/cython/issues/3909
    double PyOS_string_to_double(
        const char *s, char **endptr, PyObject *overflow_exception) except? -1.0
    char *PyOS_double_to_string(
        double val, char format_code, int precision, int flags, int *ptype
    ) except NULL
    int PyOS_snprintf(char *str, size_t size, const char *format, ...)
    int Py_DTSF_ADD_DOT_0


# @cython.final  # TODO? causes compile warnings
cdef class IntDumper(CDumper):

    format = Format.TEXT

    def __cinit__(self):
        self.oid = oids.INT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int size = 22  # max int as string
        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        cdef long long val = PyLong_AsLongLong(obj)
        cdef int written = PyOS_snprintf(buf, size, "%lld", val)
        return written

    def quote(self, obj) -> bytearray:
        cdef Py_ssize_t length

        rv = PyByteArray_FromStringAndSize("", 0)
        if obj >= 0:
            length = self.cdump(obj, rv, 0)
        else:
            PyByteArray_Resize(rv, 23)
            rv[0] = b' '
            length = 1 + self.cdump(obj, rv, 1)

        PyByteArray_Resize(rv, length)
        return rv


@cython.final
cdef class Int4BinaryDumper(CDumper):

    format = Format.BINARY

    def __cinit__(self):
        self.oid = oids.INT4_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int32_t))
        cdef long long val = PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint32_t *ptvar = <uint32_t *>(&val)
        cdef int32_t beval = htobe32(ptvar[0])
        memcpy(buf, <void *>&beval, sizeof(int32_t))
        return sizeof(int32_t)


@cython.final
cdef class Int8BinaryDumper(CDumper):

    format = Format.BINARY

    def __cinit__(self):
        self.oid = oids.INT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int64_t))
        cdef long long val = PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint64_t *ptvar = <uint64_t *>(&val)
        cdef int64_t beval = htobe64(ptvar[0])
        memcpy(buf, <void *>&beval, sizeof(int64_t))
        return sizeof(int64_t)


@cython.final
cdef class IntLoader(CLoader):

    format = Format.TEXT

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromString(data, NULL, 10)


@cython.final
cdef class Int2BinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int16_t>be16toh((<uint16_t *>data)[0]))


@cython.final
cdef class Int4BinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0]))


@cython.final
cdef class Int8BinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLongLong(<int64_t>be64toh((<uint64_t *>data)[0]))


@cython.final
cdef class OidBinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromUnsignedLong(be32toh((<uint32_t *>data)[0]))


@cython.final
cdef class FloatDumper(CDumper):

    format = Format.TEXT

    def __cinit__(self):
        self.oid = oids.FLOAT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef double d = PyFloat_AsDouble(obj)
        cdef char *out = PyOS_double_to_string(
            d, b'r', 0, Py_DTSF_ADD_DOT_0, NULL)
        cdef Py_ssize_t length = strlen(out)
        cdef char *tgt = CDumper.ensure_size(rv, offset, length)
        memcpy(tgt, out, length)
        PyMem_Free(out)
        return length

    def quote(self, obj) -> bytes:
        value = bytes(self.dump(obj))
        cdef PyObject *ptr = PyDict_GetItem(_special_float, value)
        if ptr != NULL:
            return <object>ptr

        return value if obj >= 0 else b" " + value

cdef dict _special_float = {
    b"inf": b"'Infinity'::float8",
    b"-inf": b"'-Infinity'::float8",
    b"nan": b"'NaN'::float8",
}


@cython.final
cdef class FloatBinaryDumper(CDumper):

    format = Format.BINARY

    def __cinit__(self):
        self.oid = oids.FLOAT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef double d = PyFloat_AsDouble(obj)
        cdef uint64_t *intptr = <uint64_t *>&d
        cdef uint64_t swp = htobe64(intptr[0])
        cdef char *tgt = CDumper.ensure_size(rv, offset, sizeof(swp))
        memcpy(tgt, <void *>&swp, sizeof(swp))
        return sizeof(swp)


@cython.final
cdef class FloatLoader(CLoader):

    format = Format.TEXT

    cdef object cload(self, const char *data, size_t length):
        cdef double d = PyOS_string_to_double(
            data, NULL, <PyObject *>OverflowError)
        return PyFloat_FromDouble(d)


@cython.final
cdef class Float4BinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint32_t asint = be32toh((<uint32_t *>data)[0])
        # avoid warning:
        # dereferencing type-punned pointer will break strict-aliasing rules
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<float *>swp)[0])


@cython.final
cdef class Float8BinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t asint = be64toh((<uint64_t *>data)[0])
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<double *>swp)[0])
