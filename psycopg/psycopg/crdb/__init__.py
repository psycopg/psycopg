"""
CockroachDB support package.
"""

# Copyright (C) 2022 The Psycopg Team

from . import _types
from .connection import CrdbConnection, AsyncCrdbConnection, CrdbConnectionInfo
from ._anyio import AnyIOCrdbConnection

adapters = _types.adapters  # exposed by the package
connect = CrdbConnection.connect

_types.register_crdb_types(adapters.types)
_types.register_crdb_adapters(adapters)

__all__ = [
    "AnyIOCrdbConnection",
    "AsyncCrdbConnection",
    "CrdbConnection",
    "CrdbConnectionInfo",
]
