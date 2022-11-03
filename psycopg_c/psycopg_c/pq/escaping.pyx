"""
psycopg_c.pq.Escaping object implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.string cimport strlen
from cpython.mem cimport PyMem_Malloc, PyMem_Free


cdef class Escaping:
    def __init__(self, PGconn conn = None):
        self.conn = conn

    cpdef escape_literal(self, data):
        cdef char *out
        cdef char *ptr
        cdef Py_ssize_t length

        if self.conn is None:
            raise e.OperationalError("escape_literal failed: no connection provided")
        if self.conn._pgconn_ptr is NULL:
            raise e.OperationalError("the connection is closed")

        _buffer_as_string_and_size(data, &ptr, &length)

        out = libpq.PQescapeLiteral(self.conn._pgconn_ptr, ptr, length)
        if out is NULL:
            raise e.OperationalError(
                f"escape_literal failed: {error_message(self.conn)}"
            )

        rv = out[:strlen(out)]
        libpq.PQfreemem(out)
        return rv

    cpdef escape_identifier(self, data):
        cdef char *out
        cdef char *ptr
        cdef Py_ssize_t length

        _buffer_as_string_and_size(data, &ptr, &length)

        if self.conn is None:
            raise e.OperationalError("escape_identifier failed: no connection provided")
        if self.conn._pgconn_ptr is NULL:
            raise e.OperationalError("the connection is closed")

        out = libpq.PQescapeIdentifier(self.conn._pgconn_ptr, ptr, length)
        if out is NULL:
            raise e.OperationalError(
                f"escape_identifier failed: {error_message(self.conn)}"
            )

        rv = out[:strlen(out)]
        libpq.PQfreemem(out)
        return rv

    cpdef escape_string(self, data):
        cdef int error
        cdef size_t len_out
        cdef char *ptr
        cdef char *buf_out
        cdef Py_ssize_t length

        _buffer_as_string_and_size(data, &ptr, &length)

        if self.conn is not None:
            if self.conn._pgconn_ptr is NULL:
                raise e.OperationalError("the connection is closed")

            buf_out = <char *>PyMem_Malloc(length * 2 + 1)
            len_out = libpq.PQescapeStringConn(
                self.conn._pgconn_ptr, buf_out, ptr, length, &error
            )
            if error:
                PyMem_Free(buf_out)
                raise e.OperationalError(
                    f"escape_string failed: {error_message(self.conn)}"
                )

        else:
            buf_out = <char *>PyMem_Malloc(length * 2 + 1)
            len_out = libpq.PQescapeString(buf_out, ptr, length)

        rv = buf_out[:len_out]
        PyMem_Free(buf_out)
        return rv

    cpdef escape_bytea(self, data):
        cdef size_t len_out
        cdef unsigned char *out
        cdef char *ptr
        cdef Py_ssize_t length

        if self.conn is not None and self.conn._pgconn_ptr is NULL:
            raise e.OperationalError("the connection is closed")

        _buffer_as_string_and_size(data, &ptr, &length)

        if self.conn is not None:
            out = libpq.PQescapeByteaConn(
                self.conn._pgconn_ptr, <unsigned char *>ptr, length, &len_out)
        else:
            out = libpq.PQescapeBytea(<unsigned char *>ptr, length, &len_out)

        if out is NULL:
            raise MemoryError(
                f"couldn't allocate for escape_bytea of {len(data)} bytes"
            )

        rv = out[:len_out - 1]  # out includes final 0
        libpq.PQfreemem(out)
        return rv

    cpdef unescape_bytea(self, const unsigned char *data):
        # not needed, but let's keep it symmetric with the escaping:
        # if a connection is passed in, it must be valid.
        if self.conn is not None:
            if self.conn._pgconn_ptr is NULL:
                raise e.OperationalError("the connection is closed")

        cdef size_t len_out
        cdef unsigned char *out = libpq.PQunescapeBytea(data, &len_out)
        if out is NULL:
            raise MemoryError(
                f"couldn't allocate for unescape_bytea of {len(data)} bytes"
            )

        rv = out[:len_out]
        libpq.PQfreemem(out)
        return rv
