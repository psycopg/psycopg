"""
psycopg_c._psycopg optimization module.

The module contains optimized C code used in preference to Python code
if a compiler is available.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg_c cimport pq
from psycopg_c.pq cimport libpq
from psycopg_c._psycopg cimport oids

import logging

from psycopg.pq import Format as _pq_Format
from psycopg._enums import PyFormat as _py_Format

logger = logging.getLogger("psycopg")

PQ_TEXT = _pq_Format.TEXT
PQ_BINARY = _pq_Format.BINARY

PG_AUTO = _py_Format.AUTO
PG_TEXT = _py_Format.TEXT
PG_BINARY = _py_Format.BINARY


cdef extern from *:
    """
#ifndef ARRAYSIZE
#define ARRAYSIZE(a) ((sizeof(a) / sizeof(*(a))))
#endif
    """
    int ARRAYSIZE(void *array)


include "_psycopg/adapt.pyx"
include "_psycopg/copy.pyx"
include "_psycopg/generators.pyx"
include "_psycopg/transform.pyx"

include "types/array.pyx"
include "types/datetime.pyx"
include "types/numeric.pyx"
include "types/bool.pyx"
include "types/string.pyx"
