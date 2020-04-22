cimport cpython

cdef object load_int_text(const char *data, size_t length, void *context):
    return int(data)

cdef object load_int2_binary(const char *data, size_t length, void *context):
    return cpython.PyLong_FromLong(unpack_int16(data, 2))

cdef object load_int4_binary(const char *data, size_t length, void *context):
    return cpython.PyLong_FromLong(unpack_int32(data, 4))

cdef object load_int8_binary(const char *data, size_t length, void *context):
    return cpython.PyLong_FromLongLong(unpack_int64(data, 8))

cdef object load_oid_binary(const char *data, size_t length, void *context):
    return cpython.PyLong_FromUnsignedLong(unpack_uint32(data, 4))

cdef object load_bool_binary(const char *data, size_t length, void *context):
    if data[0]:
        return True
    else:
        return False

cdef long unpack_int16(const char *data, size_t length):
    return 0

cdef long unpack_int32(const char *data, size_t length):
    return 0

cdef long unpack_uint32(const char *data, size_t length):
    return 0

cdef long long unpack_int64(const char *data, size_t length):
    return 0
