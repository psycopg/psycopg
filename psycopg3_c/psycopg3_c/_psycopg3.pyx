"""
psycopg3._psycopg3 optimization module.

The module contains optimized C code used in preference to Python code
if a compiler is available.
"""

# Copyright (C) 2020 The Psycopg Team

include "types/numeric.pyx"
include "types/singletons.pyx"
include "types/text.pyx"
include "generators.pyx"
include "adapt.pyx"
include "transform.pyx"
