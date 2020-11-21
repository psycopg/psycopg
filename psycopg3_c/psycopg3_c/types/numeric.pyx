"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.stdint cimport *
from psycopg3_c.endian cimport be16toh, be32toh, be64toh

from cpython.long cimport PyLong_FromString, PyLong_FromLong
from cpython.long cimport PyLong_FromLongLong, PyLong_FromUnsignedLong
from cpython.float cimport PyFloat_FromDouble

# work around https://github.com/cython/cython/issues/3909
cdef extern from "Python.h":
    double PyOS_string_to_double(
        const char *s, char **endptr, object overflow_exception) except? -1.0


cdef class IntLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromString(data, NULL, 10)


cdef class Int2BinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int16_t>be16toh((<uint16_t *>data)[0]))


cdef class Int4BinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0]))


cdef class Int8BinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLongLong(<int64_t>be64toh((<uint64_t *>data)[0]))


cdef class OidBinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromUnsignedLong(be32toh((<uint32_t *>data)[0]))


cdef class FloatLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        cdef double d = PyOS_string_to_double(data, NULL, OverflowError)
        return PyFloat_FromDouble(d)


cdef class Float4BinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        cdef uint32_t asint = be32toh((<uint32_t *>data)[0])
        # avoid warning:
        # dereferencing type-punned pointer will break strict-aliasing rules
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<float *>swp)[0])


cdef class Float8BinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t asint = be64toh((<uint64_t *>data)[0])
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<double *>swp)[0])


cdef class BoolLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        # this creates better C than `return data[0] == b't'`
        return True if data[0] == b't' else False


cdef class BoolBinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return True if data[0] else False


cdef void register_numeric_c_adapters():
    logger.debug("registering optimised numeric c adapters")

    from psycopg3.oids import builtins
    from psycopg3.adapt import Loader

    IntLoader.register(builtins["int2"].oid)
    IntLoader.register(builtins["int4"].oid)
    IntLoader.register(builtins["int8"].oid)
    IntLoader.register(builtins["oid"].oid)
    FloatLoader.register(builtins["float4"].oid)
    FloatLoader.register(builtins["float8"].oid)
    BoolLoader.register(builtins["bool"].oid)

    Int2BinaryLoader.register_binary(builtins["int2"].oid)
    Int4BinaryLoader.register_binary(builtins["int4"].oid)
    Int8BinaryLoader.register_binary(builtins["int8"].oid)
    OidBinaryLoader.register_binary(builtins["oid"].oid)
    Float4BinaryLoader.register_binary(builtins["float4"].oid)
    Float8BinaryLoader.register_binary(builtins["float8"].oid)
    BoolBinaryLoader.register_binary(builtins["bool"].oid)
