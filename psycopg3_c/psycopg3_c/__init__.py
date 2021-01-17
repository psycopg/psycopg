"""
psycopg3 -- PostgreSQL database adapter for Python -- C optimization package
"""

# Copyright (C) 2020-2021 The Psycopg Team

import sys

# This package shouldn't be imported before psycopg3 itself, or weird things
# will happen
if "psycopg3" not in sys.modules:
    raise ImportError(
        "the psycopg3 package should be imported before psycopg3_c"
    )

from .version import __version__  # noqa
