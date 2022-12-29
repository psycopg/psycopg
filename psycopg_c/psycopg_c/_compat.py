"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys

if sys.version_info < (3, 8):
    import importlib_metadata as metadata
else:
    from importlib import metadata


__all__ = ["metadata"]
