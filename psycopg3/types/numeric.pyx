from libc.stdint cimport *
from psycopg3.types.endian cimport be16toh, be32toh, be64toh

from cpython.long cimport (
    PyLong_FromLong, PyLong_FromLongLong, PyLong_FromUnsignedLong)


cdef object load_int_text(const char *data, size_t length, void *context):
    return int(data)

cdef object load_int2_binary(const char *data, size_t length, void *context):
    return PyLong_FromLong(<int16_t>be16toh((<uint16_t *>data)[0]))

cdef object load_int4_binary(const char *data, size_t length, void *context):
    return PyLong_FromLong(<int32_t>be32toh((<uint32_t *>data)[0]))

cdef object load_int8_binary(const char *data, size_t length, void *context):
    return PyLong_FromLongLong(<int64_t>be64toh((<uint64_t *>data)[0]))

cdef object load_oid_binary(const char *data, size_t length, void *context):
    return PyLong_FromUnsignedLong(be32toh((<uint32_t *>data)[0]))

cdef object load_bool_binary(const char *data, size_t length, void *context):
    if data[0]:
        return True
    else:
        return False
