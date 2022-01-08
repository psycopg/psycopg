"""
psycopg_c.pq cython module.

This file is necessary to allow c-importing pxd files from this directory.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg_c.pq cimport libpq
