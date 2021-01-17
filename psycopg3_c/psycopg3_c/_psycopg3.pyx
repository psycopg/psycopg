"""
psycopg3_c._psycopg3 optimization module.

The module contains optimized C code used in preference to Python code
if a compiler is available.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from psycopg3_c cimport pq
from psycopg3_c.pq cimport libpq
from psycopg3_c._psycopg3 cimport oids

from psycopg3.pq import Format as _pq_Format
from psycopg3._enums import Format as _pg_Format

PQ_TEXT = _pq_Format.TEXT
PQ_BINARY = _pq_Format.BINARY

PG_AUTO = _pg_Format.AUTO
PG_TEXT = _pg_Format.TEXT
PG_BINARY = _pg_Format.BINARY


include "_psycopg3/adapt.pyx"
include "_psycopg3/copy.pyx"
include "_psycopg3/generators.pyx"
include "_psycopg3/transform.pyx"

include "types/numeric.pyx"
include "types/singletons.pyx"
include "types/text.pyx"
