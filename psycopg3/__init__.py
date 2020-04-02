"""
psycopg3 -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team

from .consts import VERSION as __version__  # noqa
from .connection import AsyncConnection, Connection

from .errors import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)

# register default adapters
from .types import numeric  # noqa
from .types import text  # noqa


# DBAPI compliancy
connect = Connection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

__all__ = [
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "AsyncConnection",
    "Connection",
    "connect",
]
