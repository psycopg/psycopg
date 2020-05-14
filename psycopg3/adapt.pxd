"""
C definitions of the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.object cimport PyObject

# The type of a function reading a result from the database and returning
# a Python object.
ctypedef object (*cloader_func)(const char *data, size_t length, void *context)

# Take in input a Loader instance and return a context for a `cloader_func`.
ctypedef void * (*get_context_func)(object conn)
