"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.stdint cimport *
from psycopg3_c.endian cimport be16toh, be32toh, be64toh


from cpython.long cimport (
    PyLong_FromLong, PyLong_FromLongLong, PyLong_FromUnsignedLong)


cdef class IntLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return int(data)


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


cdef class BoolBinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        if data[0]:
            return True
        else:
            return False


cdef void register_numeric_c_loaders():
    logger.debug("registering optimised numeric c loaders")

    from psycopg3.oids import builtins
    from psycopg3.adapt import Loader

    IntLoader.register(builtins["int2"].oid)
    IntLoader.register(builtins["int4"].oid)
    IntLoader.register(builtins["int8"].oid)
    IntLoader.register(builtins["oid"].oid)

    Int2BinaryLoader.register_binary(builtins["int2"].oid)
    Int4BinaryLoader.register_binary(builtins["int4"].oid)
    Int8BinaryLoader.register_binary(builtins["int8"].oid)
    OidBinaryLoader.register_binary(builtins["oid"].oid)
    BoolBinaryLoader.register_binary(builtins["bool"].oid)
