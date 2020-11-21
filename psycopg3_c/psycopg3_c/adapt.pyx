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

from psycopg3_c cimport libpq as impl
from psycopg3_c.adapt cimport cloader_func, get_context_func
from psycopg3_c.pq_cython cimport Escaping

from psycopg3.pq.enums import Format

import logging
logger = logging.getLogger("psycopg3.adapt")


cdef class CDumper:
    cdef object _src
    cdef object _context
    cdef object _connection

    def __init__(self, src: type, context: AdaptContext = None):
        self._src = src
        self._context = context
        self._connection = _connection_from_context(context)

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

    def quote(self, obj: Any) -> bytes:
        # TODO: can be optimized
        cdef bytes value = self.dump(obj)
        cdef bytes tmp
        cdef Escaping esc

        if self.connection:
            esc = Escaping(self.connection.pgconn)
            return esc.escape_literal(value)

        else:
            esc = Escaping()
            tmp = esc.escape_string(value)
            return b"'%s'" % tmp

    @property
    def oid(self) -> int:
        return 0

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

    @classmethod
    def register_binary(
        cls, src: Union[type, str], context: AdaptContext = None
    ) -> None:
        cls.register(src, context, format=Format.BINARY)


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

    @classmethod
    def register_binary(
        cls, oid: int, context: AdaptContext = None
    ) -> None:
        cls.register(oid, context, format=Format.BINARY)


cdef _connection_from_context(object context):
    from psycopg3.adapt import _connection_from_context
    return _connection_from_context(context)


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
