cimport cython

from types import ModuleType
from cpython.bytes cimport PyBytes_AsString
from cpython.long cimport PyLong_FromUnsignedLongLong

cdef extern from "Python.h":
    # PyUnicode_AsUTF8 was added to cpython.unicode in 3.1.x but we still
    # support 3.0.x
    const char *PyUnicode_AsUTF8(object unicode) except NULL


uuid: ModuleType | None = None


@cython.final
cdef class UUIDDumper(CDumper):
    format = PQ_TEXT
    oid = oids.UUID_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef const char *src = PyUnicode_AsUTF8(obj.hex)
        cdef char *buf = CDumper.ensure_size(rv, offset, 32)
        memcpy(buf, src, 32)
        return 32


@cython.final
cdef class UUIDBinaryDumper(CDumper):
    format = PQ_BINARY
    oid = oids.UUID_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef const char *src = PyBytes_AsString(obj.bytes)
        cdef char *buf = CDumper.ensure_size(rv, offset, 16)
        memcpy(buf, src, 16)
        return 16


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


@cython.final
cdef class UUIDLoader(CLoader):
    format = PQ_TEXT

    cdef PyObject *_uuid_type
    cdef object _uuid_new
    cdef object _obj_setattr
    cdef PyObject *_safeuuid_unknown

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        global uuid
        # uuid is slow to import, lazy load it
        if uuid is None:
            import uuid

        self._uuid_type = <PyObject *>uuid.UUID
        self._uuid_new = uuid.UUID.__new__
        self._obj_setattr = object.__setattr__
        self._safeuuid_unknown = <PyObject *>uuid.SafeUUID.unknown

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

        cdef object int_value = (PyLong_FromUnsignedLongLong(high) << 64) | PyLong_FromUnsignedLongLong(low)

        cdef object u = PyObject_CallFunctionObjArgs(self._uuid_new, self._uuid_type, NULL)
        PyObject_CallFunctionObjArgs(self._obj_setattr, <PyObject *>u, <PyObject *>"is_safe", self._safeuuid_unknown, NULL)
        PyObject_CallFunctionObjArgs(self._obj_setattr, <PyObject *>u, <PyObject *>"int", <PyObject *>int_value, NULL)
        return u


@cython.final
cdef class UUIDBinaryLoader(CLoader):
    format = PQ_BINARY

    cdef PyObject *_uuid_type
    cdef object _uuid_new
    cdef object _obj_setattr
    cdef PyObject *_safeuuid_unknown

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        global uuid
        # uuid is slow to import, lazy load it
        if uuid is None:
            import uuid

        self._uuid_type = <PyObject *>uuid.UUID
        self._uuid_new = uuid.UUID.__new__
        self._obj_setattr = object.__setattr__
        self._safeuuid_unknown = <PyObject *>uuid.SafeUUID.unknown

    cdef object cload(self, const char *data, size_t length):
        cdef object int_value = int.from_bytes(data[:length], 'big')
        cdef object u = PyObject_CallFunctionObjArgs(self._uuid_new, self._uuid_type, NULL)
        PyObject_CallFunctionObjArgs(self._obj_setattr, <PyObject *>u, <PyObject *>"is_safe", self._safeuuid_unknown, NULL)
        PyObject_CallFunctionObjArgs(self._obj_setattr, <PyObject *>u, <PyObject *>"int", <PyObject *>int_value, NULL)
        return u
