"""
Cython adapters for boolean.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg3.pq import Format


cdef class BoolDumper(CDumper):

    format = Format.TEXT

    def __cinit__(self):
        self.oid = oids.BOOL_OID

    def dump(self, obj) -> bytes:
        # Fast paths, just a pointer comparison
        if obj is True:
            return b"t"
        elif obj is False:
            return b"f"
        else:
            return b"t" if obj else b"f"

    def quote(self, obj: bool) -> bytes:
        if obj is True:
            return b"true"
        elif obj is False:
            return b"false"
        else:
            return b"true" if obj else b"false"


cdef class BoolBinaryDumper(BoolDumper):

    format = Format.BINARY

    def dump(self, obj) -> bytes:
        if obj is True:
            return b"\x01"
        elif obj is False:
            return b"\x00"
        else:
            return b"\x01" if obj else b"\x00"


cdef class BoolLoader(CLoader):

    format = Format.TEXT

    cdef object cload(self, const char *data, size_t length):
        # this creates better C than `return data[0] == b't'`
        return True if data[0] == b't' else False


cdef class BoolBinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return True if data[0] else False


cdef void register_singletons_c_adapters():
    logger.debug("registering optimised singletons c adapters")

    BoolDumper.register(bool)
    BoolBinaryDumper.register(bool)

    BoolLoader.register(oids.BOOL_OID)
    BoolBinaryLoader.register(oids.BOOL_OID)
