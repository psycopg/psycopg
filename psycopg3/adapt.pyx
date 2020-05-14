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

from psycopg3.adapt cimport cloader_func, get_context_func

import logging
logger = logging.getLogger("psycopg3.adapt")


cdef class CLoader:
    cdef object pyloader
    cdef cloader_func cloader
    cdef get_context_func get_context


cloaders = {}

cdef void register_c_loader(
    object pyloader,
    cloader_func cloader,
    get_context_func get_context = NULL
):
    """
    Map a python loader to an optimised C version.

    Whenever the python loader would be used the C version may be chosen in
    preference.
    """
    cdef CLoader cl = CLoader()
    cl.pyloader = pyloader
    cl.cloader = cloader
    cl.get_context = get_context
    cloaders[pyloader] = cl


def register_builtin_c_loaders():
    """
    Register all the builtin optimized methods.

    This function is supposed to be called only once, after the Python loaders
    are registered.

    """
    if cloaders:
        logger.warning("c loaders already registered")
        return

    logger.debug("registering optimised c loaders")
    register_numeric_c_loaders()
    register_text_c_loaders()
