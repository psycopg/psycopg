"""
psycopg3 types package
"""

# Copyright (C) 2020 The Psycopg Team


from .oids import builtins

# Register default adapters
from . import array, composite, datetime, numeric, text  # noqa

# Register associations with array oids
array.register_all_arrays()


__all__ = ["builtins"]
