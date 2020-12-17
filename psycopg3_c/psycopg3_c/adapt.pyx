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

from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_AS_STRING

from psycopg3_c cimport libpq as impl
from psycopg3_c.adapt cimport cloader_func, get_context_func
from psycopg3_c.pq_cython cimport Escaping, _buffer_as_string_and_size

from psycopg3 import errors as e
from psycopg3.pq import Format
from psycopg3.pq.misc import error_message

import logging
logger = logging.getLogger("psycopg3.adapt")


cdef class CDumper:
    cdef object _src
    cdef object _context
    cdef object _connection
    cdef PGconn _pgconn
    cdef impl.Oid _oid

    def __init__(self, src: type, context: AdaptContext = None):
        self._src = src
        self._context = context
        self._connection = _connection_from_context(context)
        self._pgconn = (
            self._connection.pgconn if self._connection is not None else None
        )

    @property
    def src(self) -> type:
        return self._src

    @property
    def context(self) -> AdaptContext:
        return self._context

    @property
    def connection(self):
        return self._connection

    def dump(self, obj: Any) -> bytes:
        raise NotImplementedError()

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

            len_out = impl.PQescapeStringConn(
                self._pgconn.pgconn_ptr, ptr_out + 1, ptr, length, &error
            )
            if error:
                raise e.OperationalError(
                    f"escape_string failed: {error_message(self.connection)}"
                )
        else:
            len_out = impl.PQescapeString(ptr_out + 1, ptr, length)

        ptr_out[0] = b'\''
        ptr_out[len_out + 1] = b'\''
        PyByteArray_Resize(rv, len_out + 2)

        return rv

    @property
    def oid(self) -> int:
        # Implicitly initialised to zero
        # Subclasses may implement __cinit__ to set a per-class value
        return self._oid

    @classmethod
    def register(
        cls,
        src: Union[type, str],
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> None:
        if not isinstance(src, (str, type)):
            raise TypeError(
                f"dumpers should be registered on classes, got {src} instead"
            )
        from psycopg3.adapt import Dumper

        where = context.dumpers if context else Dumper.globals
        where[src, format] = cls


cdef class CLoader:
    cdef impl.Oid _oid
    cdef object _context
    cdef object _connection

    def __init__(self, oid: int, context: "AdaptContext" = None):
        self._oid = oid
        self._context = context
        self._connection = _connection_from_context(context)

    @property
    def oid(self) -> int:
        return self._oid

    @property
    def context(self) -> AdaptContext:
        return self._context

    @property
    def connection(self):
        return self._connection

    cdef object cload(self, const char *data, size_t length):
        raise NotImplementedError()

    def load(self, data: bytes) -> Any:
        cdef char *buffer
        cdef Py_ssize_t length
        PyBytes_AsStringAndSize(data, &buffer, &length)
        return self.cload(data, length)

    @classmethod
    def register(
        cls,
        oid: int,
        context: "AdaptContext" = None,
        format: Format = Format.TEXT,
    ) -> None:
        if not isinstance(oid, int):
            raise TypeError(
                f"loaders should be registered on oid, got {oid} instead"
            )

        from psycopg3.adapt import Loader

        where = context.loaders if context else Loader.globals
        where[oid, format] = cls


cdef _connection_from_context(object context):
    from psycopg3.adapt import connection_from_context
    return connection_from_context(context)


def register_builtin_c_adapters():
    """
    Register all the builtin optimized adpaters.

    This function is supposed to be called only once, after the Python adapters
    are registered.

    """
    logger.debug("registering optimised c adapters")
    register_numeric_c_adapters()
    register_singletons_c_adapters()
    register_text_c_adapters()
