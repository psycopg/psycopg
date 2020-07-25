"""
psycopg3 -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team

from . import pq
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

from .dbapi20 import BINARY, DATETIME, NUMBER, ROWID, STRING
from .dbapi20 import Binary, Date, DateFromTicks, Time, TimeFromTicks
from .dbapi20 import Timestamp, TimestampFromTicks

from .version import __version__  # noqa

# DBAPI compliancy
connect = Connection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


# register default adapters
from . import types  # noqa

# Override adapters with fast version if available
if pq.__impl__ == "c":
    from psycopg3_c._psycopg3 import register_builtin_c_loaders

    register_builtin_c_loaders()


__all__ = (
    ["Warning", "Error", "InterfaceError", "DatabaseError", "DataError"]
    + ["OperationalError", "IntegrityError", "InternalError"]
    + ["ProgrammingError", "NotSupportedError"]
    + ["AsyncConnection", "Connection", "connect"]
    + ["BINARY", "DATETIME", "NUMBER", "ROWID", "STRING"]
    + ["Binary", "Date", "DateFromTicks", "Time", "TimeFromTicks"]
    + ["Timestamp", "TimestampFromTicks"]
)
