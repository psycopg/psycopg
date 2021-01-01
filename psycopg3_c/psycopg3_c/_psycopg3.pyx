"""
psycopg3_c._psycopg3 optimization module.

The module contains optimized C code used in preference to Python code
if a compiler is available.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg3_c cimport pq
from psycopg3_c.pq cimport libpq
from psycopg3_c._psycopg3 cimport oids

from psycopg3.pq import Format

FORMAT_TEXT = Format.TEXT
FORMAT_BINARY = Format.BINARY


include "_psycopg3/adapt.pyx"
include "_psycopg3/copy.pyx"
include "_psycopg3/generators.pyx"
include "_psycopg3/transform.pyx"

include "types/numeric.pyx"
include "types/singletons.pyx"
include "types/text.pyx"
