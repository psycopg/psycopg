"""
Cython adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8
from psycopg3_c cimport libpq


cdef class StringLoader(CLoader):
    cdef int is_utf8
    cdef object pydecoder

    def __init__(self, oid: int, context: "AdaptContext" = None):
        super().__init__(oid, context)

        self.is_utf8 = 0
        self.pydecoder = None
        conn = self.connection
        if conn is not None:
            if conn.client_encoding == "UTF8":
                self.is_utf8 = 1
            elif conn.client_encoding != "SQL_ASCII":
                self.pydecoder = conn.codec.decode
        else:
            self.pydecoder = codecs.lookup("utf8").decode

    cdef object cload(self, const char *data, size_t length):
        if self.is_utf8:
            return PyUnicode_DecodeUTF8(<char *>data, length, NULL)

        b = PyBytes_FromStringAndSize(data, length)
        if self.pydecoder is not None:
            return self.pydecoder(b)[0]
        else:
            return b


cdef class TextByteaLoader(CLoader):
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


cdef class BinaryByteaLoader(CLoader):
    cdef object cload(self, const char *data, size_t length):
        return data[:length]


cdef void register_text_c_loaders():
    logger.debug("registering optimised text c loaders")

    from psycopg3.adapt import Loader
    from psycopg3.types import builtins

    Loader.register(0, StringLoader)    # INVALID_OID
    Loader.register(builtins["text"].oid, StringLoader)
    Loader.register_binary(builtins["text"].oid, StringLoader)
    Loader.register(builtins["varchar"].oid, StringLoader)
    Loader.register_binary(builtins["varchar"].oid, StringLoader)

    Loader.register(builtins['bytea'].oid, TextByteaLoader)
    Loader.register_binary(builtins['bytea'].oid, BinaryByteaLoader)
