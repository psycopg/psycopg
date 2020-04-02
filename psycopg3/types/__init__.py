"""
psycopg3 types package
"""

# Copyright (C) 2020 The Psycopg Team


from .oids import type_oid

# Register default adapters
from . import numeric, text  # noqa

__all__ = ["type_oid"]
