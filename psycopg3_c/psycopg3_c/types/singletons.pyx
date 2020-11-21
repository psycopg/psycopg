"""
Cython adapters for boolean.
"""

# Copyright (C) 2020 The Psycopg Team

cdef class BoolDumper(CDumper):
    oid = 16  # TODO: bool oid

    def dump(self, obj: bool) -> bytes:
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
    def dump(self, obj: bool) -> bytes:
        if obj is True:
            return b"\x01"
        elif obj is False:
            return b"\x00"
        else:
            return b"\x01" if obj else b"\x00"


cdef class BoolLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        # this creates better C than `return data[0] == b't'`
        return True if data[0] == b't' else False


cdef class BoolBinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return True if data[0] else False


cdef void register_singletons_c_adapters():
    logger.debug("registering optimised singletons c adapters")

    from psycopg3.oids import builtins

    BoolDumper.register(bool)
    BoolBinaryDumper.register_binary(bool)

    BoolLoader.register(builtins["bool"].oid)
    BoolBinaryLoader.register_binary(builtins["bool"].oid)
