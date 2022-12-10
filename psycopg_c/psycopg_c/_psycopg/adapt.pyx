"""
C implementation of the adaptation system.

This module maps each Python adaptation function to a C adaptation function.
Notice that C adaptation functions have a different signature because they can
avoid making a memory copy, however this makes impossible to expose them to
Python.

This module exposes facilities to map the builtin adapters in python to
equivalent C implementations.

"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any

cimport cython

from libc.string cimport memcpy, memchr
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_GET_SIZE, PyByteArray_AS_STRING

from psycopg_c.pq cimport _buffer_as_string_and_size, Escaping

from psycopg import errors as e
from psycopg.pq.misc import error_message


@cython.freelist(8)
cdef class CDumper:

    cdef readonly object cls
    cdef pq.PGconn _pgconn

    oid = oids.INVALID_OID

    def __cinit__(self, cls, context: Optional[AdaptContext] = None):
        self.cls = cls
        conn = context.connection if context is not None else None
        self._pgconn = conn.pgconn if conn is not None else None

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        """Store the Postgres representation *obj* into *rv* at *offset*

        Return the number of bytes written to rv or -1 on Python exception.

        Subclasses must implement this method. The `dump()` implementation
        transforms the result of this method to a bytearray so that it can be
        returned to Python.

        The function interface allows C code to use this method automatically
        to create larger buffers, e.g. for copy, composite objects, etc.

        Implementation note: as you will always need to make sure that rv
        has enough space to include what you want to dump, `ensure_size()`
        might probably come handy.
        """
        raise NotImplementedError()

    def dump(self, obj):
        """Return the Postgres representation of *obj* as Python array of bytes"""
        cdef rv = PyByteArray_FromStringAndSize("", 0)
        cdef Py_ssize_t length = self.cdump(obj, rv, 0)
        PyByteArray_Resize(rv, length)
        return rv

    def quote(self, obj):
        cdef char *ptr
        cdef char *ptr_out
        cdef Py_ssize_t length

        value = self.dump(obj)

        if self._pgconn is not None:
            esc = Escaping(self._pgconn)
            # escaping and quoting
            return esc.escape_literal(value)

        # This path is taken when quote is asked without a connection,
        # usually it means by psycopg.sql.quote() or by
        # 'Composible.as_string(None)'. Most often than not this is done by
        # someone generating a SQL file to consume elsewhere.

        rv = PyByteArray_FromStringAndSize("", 0)

        # No quoting, only quote escaping, random bs escaping. See further.
        esc = Escaping()
        out = esc.escape_string(value)

        _buffer_as_string_and_size(out, &ptr, &length)

        if not memchr(ptr, b'\\', length):
            # If the string has no backslash, the result is correct and we
            # don't need to bother with standard_conforming_strings.
            PyByteArray_Resize(rv, length + 2)  # Must include the quotes
            ptr_out = PyByteArray_AS_STRING(rv)
            ptr_out[0] = b"'"
            memcpy(ptr_out + 1, ptr, length)
            ptr_out[length + 1] = b"'"
            return rv

        # The libpq has a crazy behaviour: PQescapeString uses the last
        # standard_conforming_strings setting seen on a connection. This
        # means that backslashes might be escaped or might not.
        #
        # A syntax E'\\' works everywhere, whereas E'\' is an error. OTOH,
        # if scs is off, '\\' raises a warning and '\' is an error.
        #
        # Check what the libpq does, and if it doesn't escape the backslash
        # let's do it on our own. Never mind the race condition.
        PyByteArray_Resize(rv, length + 4)  # Must include " E'...'" quotes
        ptr_out = PyByteArray_AS_STRING(rv)
        ptr_out[0] = b" "
        ptr_out[1] = b"E"
        ptr_out[2] = b"'"
        memcpy(ptr_out + 3, ptr, length)
        ptr_out[length + 3] = b"'"

        if esc.escape_string(b"\\") == b"\\":
            rv = bytes(rv).replace(b"\\", b"\\\\")
        return rv

    cpdef object get_key(self, object obj, object format):
        return self.cls

    cpdef object upgrade(self, object obj, object format):
        return self

    @staticmethod
    cdef char *ensure_size(bytearray ba, Py_ssize_t offset, Py_ssize_t size) except NULL:
        """
        Grow *ba*, if necessary, to contains at least *size* bytes after *offset*

        Return the pointer in the bytearray at *offset*, i.e. the place where
        you want to write *size* bytes.
        """
        cdef Py_ssize_t curr_size = PyByteArray_GET_SIZE(ba)
        cdef Py_ssize_t new_size = offset + size
        if curr_size < new_size:
            PyByteArray_Resize(ba, new_size)

        return PyByteArray_AS_STRING(ba) + offset


@cython.freelist(8)
cdef class CLoader:
    cdef public libpq.Oid oid
    cdef pq.PGconn _pgconn

    def __cinit__(self, int oid, context: Optional[AdaptContext] = None):
        self.oid = oid
        conn = context.connection if context is not None else None
        self._pgconn = conn.pgconn if conn is not None else None

    cdef object cload(self, const char *data, size_t length):
        raise NotImplementedError()

    def load(self, object data) -> Any:
        cdef char *ptr
        cdef Py_ssize_t length
        _buffer_as_string_and_size(data, &ptr, &length)
        return self.cload(ptr, length)


cdef class _CRecursiveLoader(CLoader):

    cdef Transformer _tx

    def __cinit__(self, oid: int, context: Optional[AdaptContext] = None):
        self._tx = Transformer.from_context(context)
