"""
Cython adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.bytes cimport PyBytes_AsString, PyBytes_AsStringAndSize
from cpython.unicode cimport PyUnicode_Decode, PyUnicode_DecodeUTF8
from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_AsEncodedString

from psycopg3_c.pq cimport Escaping


cdef class _StringDumper(CDumper):
    cdef int is_utf8
    cdef char *encoding
    cdef bytes _bytes_encoding  # needed to keep `encoding` alive

    def __init__(self, src: type, context: Optional[AdaptContext]):
        super().__init__(src, context)

        self.is_utf8 = 0
        self.encoding = "utf-8"

        conn = self.connection
        if conn is not None:
            self._bytes_encoding = conn.client_encoding.encode("utf-8")
            self.encoding = PyBytes_AsString(self._bytes_encoding)
            if (
                self._bytes_encoding == b"utf-8"
                or self._bytes_encoding == b"ascii"
            ):
                self.is_utf8 = 1


cdef class StringBinaryDumper(_StringDumper):

    format = Format.BINARY

    def dump(self, obj) -> bytes:
        # the server will raise DataError subclass if the string contains 0x00
        if self.is_utf8:
            return PyUnicode_AsUTF8String(obj)
        else:
            return PyUnicode_AsEncodedString(obj, self.encoding, NULL)


cdef class StringDumper(_StringDumper):

    format = Format.TEXT

    def dump(self, obj) -> bytes:
        cdef bytes rv
        cdef char *buf

        if self.is_utf8:
            rv = PyUnicode_AsUTF8String(obj)
        else:
            rv = PyUnicode_AsEncodedString(obj, self.encoding, NULL)

        try:
            # the function raises ValueError if the bytes contains 0x00
            PyBytes_AsStringAndSize(rv, &buf, NULL)
        except ValueError:
            from psycopg3 import DataError

            raise DataError(
                "PostgreSQL text fields cannot contain NUL (0x00) bytes"
            )

        return rv


cdef class TextLoader(CLoader):

    format = Format.TEXT

    cdef int is_utf8
    cdef char *encoding
    cdef bytes _bytes_encoding  # needed to keep `encoding` alive

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)

        self.is_utf8 = 0
        self.encoding = "utf-8"

        conn = self.connection
        if conn is not None:
            self._bytes_encoding = conn.client_encoding.encode("utf-8")
            self.encoding = PyBytes_AsString(self._bytes_encoding)
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


cdef class TextBinaryLoader(TextLoader):
    format = Format.BINARY


cdef class BytesDumper(CDumper):

    format = Format.TEXT

    cdef Escaping esc

    def __cinit__(self):
        self.oid = oids.BYTEA_OID

    def __init__(self, src: type, context: Optional[AdaptContext] = None):
        super().__init__(src, context)
        self.esc = Escaping(self._pgconn)

    def dump(self, obj) -> memoryview:
        return self.esc.escape_bytea(obj)


cdef class BytesBinaryDumper(BytesDumper):

    format = Format.BINARY

    def dump(self, obj):
        return obj


cdef class ByteaLoader(CLoader):

    format = Format.TEXT

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

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return data[:length]


cdef void register_text_c_adapters():
    logger.debug("registering optimised text c adapters")

    StringDumper.register(str)
    StringBinaryDumper.register(str)

    TextLoader.register(oids.INVALID_OID)
    TextLoader.register(oids.BPCHAR_OID)
    TextLoader.register(oids.NAME_OID)
    TextLoader.register(oids.TEXT_OID)
    TextLoader.register(oids.VARCHAR_OID)
    TextBinaryLoader.register(oids.BPCHAR_OID)
    TextBinaryLoader.register(oids.NAME_OID)
    TextBinaryLoader.register(oids.TEXT_OID)
    TextBinaryLoader.register(oids.VARCHAR_OID)

    BytesDumper.register(bytes)
    BytesDumper.register(bytearray)
    BytesDumper.register(memoryview)
    BytesBinaryDumper.register(bytes)
    BytesBinaryDumper.register(bytearray)
    BytesBinaryDumper.register(memoryview)

    ByteaLoader.register(oids.BYTEA_OID)
    ByteaBinaryLoader.register(oids.BYTEA_OID)
    ByteaBinaryLoader.register(oids.INVALID_OID)
