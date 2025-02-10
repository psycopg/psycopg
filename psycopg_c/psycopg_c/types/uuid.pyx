cimport cython
from cpython.long cimport PyLong_FromUnsignedLongLong


cdef extern from *:
    """
static const int8_t hex_to_int_map[] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 0-15
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 16-31
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 32-47
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,            // 48-63 ('0'-'9')
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 64-79 ('A'-'F')
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 80-95
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 96-111 ('a'-'f')
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 112-127
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 128-143
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 144-159
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 160-175
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 176-191
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 192-207
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 208-223
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 224-239
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1   // 240-255
};
"""
    const int8_t[256] hex_to_int_map


cdef class _UUIDLoader(CLoader):

    cdef object _object_new
    cdef object _uuid_type
    cdef PyObject *_wuuid_type
    cdef object _safeuuid_unknown

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        from psycopg_c import _uuid

        self._object_new = object.__new__
        self._uuid_type = _uuid.UUID
        self._wuuid_type = <PyObject *>_uuid._WritableUUID
        self._safeuuid_unknown = _uuid.SafeUUID_unknown

    cdef object _return_uuid(self, uint64_t low, uint64_t high):
        cdef object py_low = PyLong_FromUnsignedLongLong(low)
        cdef object py_high = PyLong_FromUnsignedLongLong(high)
        cdef object py_value = (py_high << 64) | py_low

        cdef object u = PyObject_CallFunctionObjArgs(
            self._object_new, self._wuuid_type, NULL)
        u.int = py_value
        u.is_safe = self._safeuuid_unknown
        u.__class__ = self._uuid_type
        return u


@cython.final
cdef class UUIDLoader(_UUIDLoader):
    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t high = 0
        cdef uint64_t low = 0
        cdef size_t i
        cdef int ndigits = 0
        cdef int8_t c

        for i in range(length):
            c = data[i]
            if hex_to_int_map[c] == -1:
                continue

            if ndigits < 16:
                high = (high << 4) | hex_to_int_map[c]
            else:
                low = (low << 4) | hex_to_int_map[c]
            ndigits += 1

        if ndigits != 32:
            raise ValueError("Invalid UUID string")

        return self._return_uuid(low, high)


@cython.final
cdef class UUIDBinaryLoader(_UUIDLoader):
    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t be[2]
        if length != sizeof(be):
            raise ValueError("Invalid UUID data")
        memcpy(&be, data, sizeof(be))

        cdef uint64_t high = endian.be64toh(be[0])
        cdef uint64_t low = endian.be64toh(be[1])
        return self._return_uuid(low, high)
