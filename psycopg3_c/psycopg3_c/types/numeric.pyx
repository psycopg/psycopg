"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.stdint cimport *
from psycopg3_c.endian cimport be16toh, be32toh, be64toh


from cpython.long cimport (
    PyLong_FromLong, PyLong_FromLongLong, PyLong_FromUnsignedLong)


cdef class TextIntLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return int(data)


cdef class BinaryInt2Loader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int16_t>be16toh((<uint16_t *>data)[0]))


cdef class BinaryInt4Loader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0]))


cdef class BinaryInt8Loader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLongLong(<int64_t>be64toh((<uint64_t *>data)[0]))


cdef class BinaryOidLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromUnsignedLong(be32toh((<uint32_t *>data)[0]))


cdef class BinaryBoolLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        if data[0]:
            return True
        else:
            return False


cdef void register_numeric_c_loaders():
    logger.debug("registering optimised numeric c loaders")

    from psycopg3.adapt import Loader
    from psycopg3.types import builtins

    TextIntLoader.register(builtins["int2"].oid)
    TextIntLoader.register(builtins["int4"].oid)
    TextIntLoader.register(builtins["int8"].oid)
    TextIntLoader.register(builtins["oid"].oid)

    BinaryInt2Loader.register_binary(builtins["int2"].oid)
    BinaryInt4Loader.register_binary(builtins["int4"].oid)
    BinaryInt8Loader.register_binary(builtins["int8"].oid)
    BinaryOidLoader.register_binary(builtins["oid"].oid)
    BinaryBoolLoader.register_binary(builtins["bool"].oid)
