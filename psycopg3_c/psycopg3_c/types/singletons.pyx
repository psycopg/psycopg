"""
Cython adapters for boolean.
"""

# Copyright (C) 2020-2021 The Psycopg Team

cimport cython


@cython.final
cdef class BoolDumper(CDumper):

    format = PQ_TEXT

    def __cinit__(self):
        self.oid = oids.BOOL_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        CDumper.ensure_size(rv, offset, 1)

        # Fast paths, just a pointer comparison
        cdef char val
        if obj is True:
            val = b"t"
        elif obj is False:
            val = b"f"
        elif obj:
            val = b"t"
        else:
            val = b"f"

        cdef char *buf = PyByteArray_AS_STRING(rv)
        buf[offset] = val
        return 1

    def quote(self, obj: bool) -> bytes:
        if obj is True:
            return b"true"
        elif obj is False:
            return b"false"
        else:
            return b"true" if obj else b"false"


@cython.final
cdef class BoolBinaryDumper(CDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.BOOL_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        CDumper.ensure_size(rv, offset, 1)

        # Fast paths, just a pointer comparison
        cdef char val
        if obj is True:
            val = b"\x01"
        elif obj is False:
            val = b"\x00"
        elif obj:
            val = b"\x01"
        else:
            val = b"\x00"

        cdef char *buf = PyByteArray_AS_STRING(rv)
        buf[offset] = val
        return 1


@cython.final
cdef class BoolLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        # this creates better C than `return data[0] == b't'`
        return True if data[0] == b't' else False


@cython.final
cdef class BoolBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return True if data[0] else False
