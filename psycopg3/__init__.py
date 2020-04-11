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
from . import types  # noqa

from .dbapi20 import BINARY, DATETIME, NUMBER, ROWID, STRING
from .dbapi20 import Binary, Date, DateFromTicks, Time, TimeFromTicks
from .dbapi20 import Timestamp, TimestampFromTicks

# DBAPI compliancy
connect = Connection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

__all__ = (
    ["Warning", "Error", "InterfaceError", "DatabaseError", "DataError"]
    + ["OperationalError", "IntegrityError", "InternalError"]
    + ["ProgrammingError", "NotSupportedError"]
    + ["AsyncConnection", "Connection", "connect"]
    + ["BINARY", "DATETIME", "NUMBER", "ROWID", "STRING"]
    + ["Binary", "Date", "DateFromTicks", "Time", "TimeFromTicks"]
    + ["Timestamp", "TimestampFromTicks"]
)
