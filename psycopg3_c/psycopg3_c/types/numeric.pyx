"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

cimport cython

from libc.stdint cimport *
from libc.string cimport memcpy, strlen
from cpython.mem cimport PyMem_Free
from cpython.long cimport (
    PyLong_FromString, PyLong_FromLong, PyLong_FromLongLong,
    PyLong_FromUnsignedLong, PyLong_AsLongLong)
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.float cimport PyFloat_FromDouble, PyFloat_AsDouble

from psycopg3_c._psycopg3 cimport endian

from psycopg3.wrappers.numeric import Int2, Int4, Int8, IntNumeric

cdef extern from "Python.h":
    # work around https://github.com/cython/cython/issues/3909
    double PyOS_string_to_double(
        const char *s, char **endptr, PyObject *overflow_exception) except? -1.0
    char *PyOS_double_to_string(
        double val, char format_code, int precision, int flags, int *ptype
    ) except NULL
    int PyOS_snprintf(char *str, size_t size, const char *format, ...)
    int Py_DTSF_ADD_DOT_0
    long long PyLong_AsLongLongAndOverflow(object pylong, int *overflow) except? -1


# defined in numutils.c
cdef extern from *:
    """
int pg_lltoa(int64_t value, char *a);
    """
    int pg_lltoa(int64_t value, char *a)

DEF MAXINT8LEN = 20


cdef class _NumberDumper(CDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef long long val
        cdef int overflow
        cdef char *buf
        cdef char *src
        cdef Py_ssize_t length

        val = PyLong_AsLongLongAndOverflow(obj, &overflow)
        if not overflow:
            buf = CDumper.ensure_size(rv, offset, MAXINT8LEN + 1)
            length = pg_lltoa(val, buf)
        else:
            b = bytes(str(obj), "utf-8")
            PyBytes_AsStringAndSize(b, &src, &length)
            buf = CDumper.ensure_size(rv, offset, length)
            memcpy(buf, src, length)

        return length

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
cdef class Int2Dumper(_NumberDumper):

    def __cinit__(self):
        self.oid = oids.INT2_OID


@cython.final
cdef class Int4Dumper(_NumberDumper):

    def __cinit__(self):
        self.oid = oids.INT4_OID


@cython.final
cdef class Int8Dumper(_NumberDumper):

    def __cinit__(self):
        self.oid = oids.INT8_OID


@cython.final
cdef class IntNumericDumper(_NumberDumper):

    def __cinit__(self):
        self.oid = oids.NUMERIC_OID


@cython.final
cdef class Int2BinaryDumper(CDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.INT2_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int16_t))
        cdef int16_t val = PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint16_t *ptvar = <uint16_t *>(&val)
        cdef int16_t beval = endian.htobe16(ptvar[0])
        memcpy(buf, <void *>&beval, sizeof(int16_t))
        return sizeof(int16_t)


@cython.final
cdef class Int4BinaryDumper(CDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.INT4_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int32_t))
        cdef int32_t val = PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint32_t *ptvar = <uint32_t *>(&val)
        cdef int32_t beval = endian.htobe32(ptvar[0])
        memcpy(buf, <void *>&beval, sizeof(int32_t))
        return sizeof(int32_t)


@cython.final
cdef class Int8BinaryDumper(CDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.INT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int64_t))
        cdef int64_t val = PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint64_t *ptvar = <uint64_t *>(&val)
        cdef int64_t beval = endian.htobe64(ptvar[0])
        memcpy(buf, <void *>&beval, sizeof(int64_t))
        return sizeof(int64_t)


@cython.final
cdef class IntNumericBinaryDumper(CDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        raise NotImplementedError("binary decimal dump not implemented yet")


cdef class IntDumper(_NumberDumper):

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        raise TypeError(
            f"{type(self).__name__} is a dispatcher to other dumpers:"
            " dump() is not supposed to be called"
        )

    cpdef get_key(self, obj, format):
        cdef long long val
        cdef int overflow

        val = PyLong_AsLongLongAndOverflow(obj, &overflow)
        if overflow:
            return IntNumeric

        if INT32_MIN <= obj <= INT32_MAX:
            if INT16_MIN <= obj <= INT16_MAX:
                return Int2
            else:
                return Int4
        else:
            if INT64_MIN <= obj <= INT64_MAX:
                return Int8
            else:
                return IntNumeric

    _int2_dumper = Int2Dumper
    _int4_dumper = Int4Dumper
    _int8_dumper = Int8Dumper
    _int_numeric_dumper = IntNumericDumper

    cpdef upgrade(self, obj, format):
        cdef long long val
        cdef int overflow

        val = PyLong_AsLongLongAndOverflow(obj, &overflow)
        if overflow:
            return self._int_numeric_dumper(IntNumeric)

        if INT32_MIN <= obj <= INT32_MAX:
            if INT16_MIN <= obj <= INT16_MAX:
                return self._int2_dumper(Int2)
            else:
                return self._int4_dumper(Int4)
        else:
            if INT64_MIN <= obj <= INT64_MAX:
                return self._int8_dumper(Int8)
            else:
                return self._int_numeric_dumper(IntNumeric)


cdef class IntBinaryDumper(IntDumper):

    format = PQ_BINARY

    _int2_dumper = Int2BinaryDumper
    _int4_dumper = Int4BinaryDumper
    _int8_dumper = Int8BinaryDumper
    _int_numeric_dumper = IntNumericBinaryDumper


@cython.final
cdef class IntLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        # if the number ends with a 0 we don't need a copy
        if data[length] == b'\0':
            return PyLong_FromString(data, NULL, 10)

        # Otherwise we have to copy it aside
        if length > MAXINT8LEN:
            raise ValueError("string too big for an int")

        cdef char[MAXINT8LEN + 1] buf
        memcpy(buf, data, length)
        buf[length] = 0
        return PyLong_FromString(buf, NULL, 10)



@cython.final
cdef class Int2BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int16_t>endian.be16toh((<uint16_t *>data)[0]))


@cython.final
cdef class Int4BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int32_t>endian.be32toh((<uint32_t *>data)[0]))


@cython.final
cdef class Int8BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLongLong(<int64_t>endian.be64toh((<uint64_t *>data)[0]))


@cython.final
cdef class OidBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromUnsignedLong(endian.be32toh((<uint32_t *>data)[0]))


@cython.final
cdef class FloatDumper(CDumper):

    format = PQ_TEXT

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

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.FLOAT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef double d = PyFloat_AsDouble(obj)
        cdef uint64_t *intptr = <uint64_t *>&d
        cdef uint64_t swp = endian.htobe64(intptr[0])
        cdef char *tgt = CDumper.ensure_size(rv, offset, sizeof(swp))
        memcpy(tgt, <void *>&swp, sizeof(swp))
        return sizeof(swp)


@cython.final
cdef class FloatLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        cdef double d = PyOS_string_to_double(
            data, NULL, <PyObject *>OverflowError)
        return PyFloat_FromDouble(d)


@cython.final
cdef class Float4BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint32_t asint = endian.be32toh((<uint32_t *>data)[0])
        # avoid warning:
        # dereferencing type-punned pointer will break strict-aliasing rules
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<float *>swp)[0])


@cython.final
cdef class Float8BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t asint = endian.be64toh((<uint64_t *>data)[0])
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<double *>swp)[0])
