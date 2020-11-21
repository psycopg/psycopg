"""
Cython adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.unicode cimport PyUnicode_Decode, PyUnicode_DecodeUTF8
from psycopg3_c cimport libpq


cdef class TextLoader(CLoader):
    cdef int is_utf8
    cdef char *encoding
    cdef bytes _bytes_encoding  # needed to keep `encoding` alive

    def __init__(self, oid: int, context: "AdaptContext" = None):
        super().__init__(oid, context)

        self.is_utf8 = 0
        self.encoding = "utf-8"

        conn = self.connection
        if conn:
            self._bytes_encoding = conn.client_encoding.encode("utf-8")
            self.encoding = self._bytes_encoding
            if self._bytes_encoding == b"utf-8":
                self.is_utf8 = 1
            elif self._bytes_encoding == b"ascii":
                self.encoding = NULL

    cdef object cload(self, const char *data, size_t length):
        if self.is_utf8:
            return PyUnicode_DecodeUTF8(<char *>data, length, NULL)
        elif self.encoding:
            return PyUnicode_Decode(<char *>data, length, self.encoding, NULL)
        else:
            return data[:length]


cdef class ByteaLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        cdef size_t len_out
        cdef unsigned char *out = libpq.PQunescapeBytea(
            <const unsigned char *>data, &len_out)
        if out is NULL:
            raise MemoryError(
                f"couldn't allocate for unescape_bytea of {len(data)} bytes"
            )

        rv = out[:len_out]
        libpq.PQfreemem(out)
        return rv


cdef class ByteaBinaryLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return data[:length]


cdef void register_text_c_adapters():
    logger.debug("registering optimised text c adapters")

    from psycopg3.oids import builtins
    from psycopg3.adapt import Loader

    TextLoader.register(0)    # INVALID_OID
    TextLoader.register(builtins["text"].oid)
    TextLoader.register_binary(builtins["text"].oid)
    TextLoader.register(builtins["varchar"].oid)
    TextLoader.register_binary(builtins["varchar"].oid)

    ByteaLoader.register(builtins['bytea'].oid)
    ByteaBinaryLoader.register_binary(builtins['bytea'].oid)
