"""
C implementation of the adaptation system.

This module maps each Python adaptation function to a C adaptation function.
Notice that C adaptation functions have a different signature because they can
avoid making a memory copy, however this makes impossible to expose them to
Python.

This module exposes facilities to map the builtin adapters in python to
equivalent C implementations.

"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any

cimport cython
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_GET_SIZE, PyByteArray_AS_STRING

from psycopg3_c.pq cimport _buffer_as_string_and_size

from psycopg3 import errors as e
from psycopg3.pq.misc import error_message

import logging
logger = logging.getLogger("psycopg3.adapt")


@cython.freelist(8)
cdef class CDumper:
    cdef readonly object cls
    cdef public libpq.Oid oid
    cdef pq.PGconn _pgconn

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
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

        Implementation note: as you will alway need to make sure that rv
        has enough space to include what you want to dump, `ensure_size()`
        might probably come handy.
        """
        raise NotImplementedError()

    def dump(self, obj: Any) -> bytearray:
        """Return the Postgres representation of *obj* as Python array of bytes"""
        cdef rv = PyByteArray_FromStringAndSize("", 0)
        cdef Py_ssize_t length = self.cdump(obj, rv, 0)
        PyByteArray_Resize(rv, length)
        return rv

    def quote(self, obj: Any) -> bytearray:
        cdef char *ptr
        cdef char *ptr_out
        cdef Py_ssize_t length, len_out
        cdef int error
        cdef bytearray rv

        pyout = self.dump(obj)
        _buffer_as_string_and_size(pyout, &ptr, &length)
        rv = PyByteArray_FromStringAndSize("", 0)
        PyByteArray_Resize(rv, length * 2 + 3)  # Must include the quotes
        ptr_out = PyByteArray_AS_STRING(rv)

        if self._pgconn is not None:
            if self._pgconn.pgconn_ptr == NULL:
                raise e.OperationalError("the connection is closed")

            len_out = libpq.PQescapeStringConn(
                self._pgconn.pgconn_ptr, ptr_out + 1, ptr, length, &error
            )
            if error:
                raise e.OperationalError(
                    f"escape_string failed: {error_message(self._pgconn)}"
                )
        else:
            len_out = libpq.PQescapeString(ptr_out + 1, ptr, length)

        ptr_out[0] = b'\''
        ptr_out[len_out + 1] = b'\''
        PyByteArray_Resize(rv, len_out + 2)

        return rv

    cdef object get_key(self, object obj, object format):
        return self.cls

    cdef object upgrade(self, object obj, object format):
        return self

    @classmethod
    def register(
        this_cls,
        cls: Union[type, str],
        context: Optional[AdaptContext] = None,
        int format = PQ_TEXT,
    ) -> None:
        if context is not None:
            adapters = context.adapters
        else:
            from psycopg3.adapt import global_adapters as adapters

        adapters.register_dumper(cls, this_cls)

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

    def __init__(self, int oid, context: Optional[AdaptContext] = None):
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

    @classmethod
    def register(
        cls,
        oid: Union[int, str],
        context: Optional["AdaptContext"] = None,
        int format = PQ_TEXT,
    ) -> None:
        if context is not None:
            adapters = context.adapters
        else:
            from psycopg3.adapt import global_adapters as adapters

        adapters.register_loader(oid, cls)
