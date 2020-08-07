"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.stdint cimport *
from psycopg3_c.endian cimport be16toh, be32toh, be64toh


from cpython.long cimport (
    PyLong_FromLong, PyLong_FromLongLong, PyLong_FromUnsignedLong)


cdef class TextIntLoader(PyxLoader):
    cdef object cload(self, const char *data, size_t length):
        return int(data)


cdef class BinaryInt2Loader(PyxLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int16_t>be16toh((<uint16_t *>data)[0]))


cdef class BinaryInt4Loader(PyxLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0]))


cdef class BinaryInt8Loader(PyxLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLongLong(<int64_t>be64toh((<uint64_t *>data)[0]))


cdef class BinaryOidLoader(PyxLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromUnsignedLong(be32toh((<uint32_t *>data)[0]))


cdef class BinaryBoolLoader(PyxLoader):
    cdef object cload(self, const char *data, size_t length):
        if data[0]:
            return True
        else:
            return False


cdef void register_numeric_c_loaders():
    logger.debug("registering optimised numeric c loaders")

    from psycopg3.adapt import Loader
    from psycopg3.types import builtins

    Loader.register(builtins["int2"].oid, TextIntLoader)
    Loader.register(builtins["int4"].oid, TextIntLoader)
    Loader.register(builtins["int8"].oid, TextIntLoader)
    Loader.register(builtins["oid"].oid, TextIntLoader)

    Loader.register_binary(builtins["int2"].oid, BinaryInt2Loader)
    Loader.register_binary(builtins["int4"].oid, BinaryInt4Loader)
    Loader.register_binary(builtins["int8"].oid, BinaryInt8Loader)
    Loader.register_binary(builtins["oid"].oid, BinaryOidLoader)
    Loader.register_binary(builtins["bool"].oid, BinaryBoolLoader)
