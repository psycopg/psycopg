"""
psycopg3 -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team

from . import pq
from .connection import AsyncConnection, Connection, Notify
from .cursor import AsyncCursor, Cursor, Column
from .copy import Copy, AsyncCopy

from .errors import Warning, Error, InterfaceError, DatabaseError
from .errors import DataError, OperationalError, IntegrityError
from .errors import InternalError, ProgrammingError, NotSupportedError

from .dbapi20 import BINARY, DATETIME, NUMBER, ROWID, STRING
from .dbapi20 import Binary, Date, DateFromTicks, Time, TimeFromTicks
from .dbapi20 import Timestamp, TimestampFromTicks

from .version import __version__

# register default adapters
from . import types

# DBAPI compliancy
connect = Connection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


# Override adapters with fast version if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    _psycopg3.register_builtin_c_loaders()
