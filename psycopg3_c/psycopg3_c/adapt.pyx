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

from psycopg3_c.adapt cimport cloader_func, get_context_func
from psycopg3_c cimport libpq as impl

import logging
logger = logging.getLogger("psycopg3.adapt")


# TODO: rename after dropping CLoader below
cdef class PyxLoader:
    cdef impl.Oid oid
    cdef object context
    cdef object connection

    def __init__(self, oid: int, context: "AdaptContext" = None):
        from psycopg3.adapt import _connection_from_context

        self.oid = oid
        self.context = context
        self.connection = _connection_from_context(context)

    cdef object cload(self, const char *data, size_t length):
        raise NotImplementedError()

    def load(self, data: bytes) -> Any:
        cdef char *buffer
        cdef Py_ssize_t length
        PyBytes_AsStringAndSize(data, &buffer, &length)
        return self.cload(data, length)


def register_builtin_c_loaders():
    """
    Register all the builtin optimized methods.

    This function is supposed to be called only once, after the Python loaders
    are registered.

    """
    logger.debug("registering optimised c loaders")
    register_numeric_c_loaders()
    register_text_c_loaders()
