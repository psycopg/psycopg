"""
Cython adapters for datetime types.
"""

# Copyright (C) 2020 The Psycopg Team


from libc.stdint cimport *

from psycopg3.types.endian cimport be16toh, be32toh, be64toh


from cpython.long cimport (
    PyLong_FromLong, PyLong_FromLongLong, PyLong_FromUnsignedLong)


cdef object load_date_binary(const char *data, size_t length, void *context):
    return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0]))


cdef object load_time_tz_binary(const char *data, size_t length, void *context):
    return PyLong_FromLongLong(<int64_t>be64toh((<uint64_t *>data)[0])), PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[1]))


cdef object load_datetime_tz_binary(const char *data, size_t length, void *context):
    return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0])), PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[1]))


cdef void register_text_c_loaders():
    logger.debug("registering optimised datetime c loaders")
    from psycopg3.types import datetime
    register_text_c_loaders(datetime.load_date_binary, load_date_binary)
    register_text_c_loaders(datetime.load_time_tz_binary, load_time_tz_binary)
    register_text_c_loaders(datetime.load_datetime_tz_binary, load_datetime_tz_binary)

