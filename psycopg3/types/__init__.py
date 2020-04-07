"""
psycopg3 types package
"""

# Copyright (C) 2020 The Psycopg Team


from .oids import builtins

# Register default adapters
from . import composite, numeric, text  # noqa

# Register associations with array oids
from . import array  # noqa

__all__ = ["builtins"]
