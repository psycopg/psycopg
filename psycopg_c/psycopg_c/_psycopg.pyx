"""
psycopg_c._psycopg optimization module.

The module contains optimized C code used in preference to Python code
if a compiler is available.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg_c cimport pq
from psycopg_c.pq cimport libpq
from psycopg_c._psycopg cimport oids

from psycopg.pq import Format as _pq_Format
from psycopg._enums import PyFormat as _py_Format

PQ_TEXT = _pq_Format.TEXT
PQ_BINARY = _pq_Format.BINARY

PG_AUTO = _py_Format.AUTO
PG_TEXT = _py_Format.TEXT
PG_BINARY = _py_Format.BINARY


include "_psycopg/adapt.pyx"
include "_psycopg/copy.pyx"
include "_psycopg/generators.pyx"
include "_psycopg/transform.pyx"

include "types/datetime.pyx"
include "types/numeric.pyx"
include "types/bool.pyx"
include "types/string.pyx"
