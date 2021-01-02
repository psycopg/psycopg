"""
Cython adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython

from libc.string cimport memcpy, memchr
from cpython.bytes cimport PyBytes_AsString, PyBytes_AsStringAndSize
from cpython.unicode cimport (
    PyUnicode_AsEncodedString,
    PyUnicode_AsUTF8String,
    PyUnicode_CheckExact,
    PyUnicode_Decode,
    PyUnicode_DecodeUTF8,
)

from psycopg3_c.pq cimport libpq, Escaping, _buffer_as_string_and_size

cdef extern from "Python.h":
    const char *PyUnicode_AsUTF8AndSize(unicode obj, Py_ssize_t *size)


cdef class _StringDumper(CDumper):
    cdef int is_utf8
    cdef char *encoding
    cdef bytes _bytes_encoding  # needed to keep `encoding` alive

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)

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

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        # the server will raise DataError subclass if the string contains 0x00
        cdef Py_ssize_t size;
        cdef const char *src

        if self.is_utf8:
            # Probably the fastest path, but doesn't work with subclasses
            if PyUnicode_CheckExact(obj):
                src = PyUnicode_AsUTF8AndSize(obj, &size)
                if src == NULL:
                    # re-encode using a function raising an exception.
                    # TODO: is there a better way?
                    PyUnicode_AsUTF8String(obj)
            else:
                b = PyUnicode_AsUTF8String(obj)
                PyBytes_AsStringAndSize(b, <char **>&src, &size)
        else:
            b = PyUnicode_AsEncodedString(obj, self.encoding, NULL)
            PyBytes_AsStringAndSize(b, <char **>&src, &size)

        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        memcpy(buf, src, size)
        return size


@cython.final
cdef class StringBinaryDumper(_StringDumper):

    format = Format.BINARY


@cython.final
cdef class StringDumper(_StringDumper):

    format = Format.TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef Py_ssize_t size = StringBinaryDumper.cdump(self, obj, rv, offset)

        # Like the binary dump, but check for 0, or the string will be truncated
        cdef const char *buf = PyByteArray_AS_STRING(rv)
        if NULL != memchr(buf + offset, 0x00, size):
            from psycopg3 import DataError
            raise DataError(
                "PostgreSQL text fields cannot contain NUL (0x00) bytes"
            )
        return size


cdef class _TextLoader(CLoader):

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

@cython.final
cdef class TextLoader(_TextLoader):

    format = Format.TEXT


@cython.final
cdef class TextBinaryLoader(_TextLoader):

    format = Format.BINARY


@cython.final
cdef class BytesDumper(CDumper):

    format = Format.TEXT

    def __cinit__(self):
        self.oid = oids.BYTEA_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:

        cdef size_t len_out
        cdef unsigned char *out
        cdef char *ptr
        cdef Py_ssize_t length

        _buffer_as_string_and_size(obj, &ptr, &length)

        if self._pgconn is not None and self._pgconn.pgconn_ptr != NULL:
            out = libpq.PQescapeByteaConn(
                self._pgconn.pgconn_ptr, <unsigned char *>ptr, length, &len_out)
        else:
            out = libpq.PQescapeBytea(<unsigned char *>ptr, length, &len_out)

        if out is NULL:
            raise MemoryError(
                f"couldn't allocate for escape_bytea of {length} bytes"
            )

        len_out -= 1  # out includes final 0
        cdef char *buf = CDumper.ensure_size(rv, offset, len_out)
        memcpy(buf, out, len_out)
        libpq.PQfreemem(out)
        return len_out


@cython.final
cdef class BytesBinaryDumper(CDumper):

    format = Format.BINARY

    def __cinit__(self):
        self.oid = oids.BYTEA_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef char *src
        cdef Py_ssize_t size;
        _buffer_as_string_and_size(obj, &src, &size)

        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        memcpy(buf, src, size)
        return  size


@cython.final
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


@cython.final
cdef class ByteaBinaryLoader(CLoader):

    format = Format.BINARY

    cdef object cload(self, const char *data, size_t length):
        return data[:length]
