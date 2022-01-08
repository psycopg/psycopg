"""
Cython adapters for boolean.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython


@cython.final
cdef class BoolDumper(CDumper):

    format = PQ_TEXT
    oid = oids.BOOL_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, 1)

        # Fast paths, just a pointer comparison
        if obj is True:
            buf[0] = b"t"
        elif obj is False:
            buf[0] = b"f"
        elif obj:
            buf[0] = b"t"
        else:
            buf[0] = b"f"

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
    oid = oids.BOOL_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *buf = CDumper.ensure_size(rv, offset, 1)

        # Fast paths, just a pointer comparison
        if obj is True:
            buf[0] = b"\x01"
        elif obj is False:
            buf[0] = b"\x00"
        elif obj:
            buf[0] = b"\x01"
        else:
            buf[0] = b"\x00"

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
