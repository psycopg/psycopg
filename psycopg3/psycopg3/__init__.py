"""
psycopg3 -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team

from . import pq
from . import types
from .copy import Copy, AsyncCopy
from .adapt import global_adapters
from .cursor import AsyncCursor, Cursor
from .errors import Warning, Error, InterfaceError, DatabaseError
from .errors import DataError, OperationalError, IntegrityError
from .errors import InternalError, ProgrammingError, NotSupportedError
from ._column import Column
from .connection import AsyncConnection, Connection, Notify
from .transaction import Rollback, Transaction, AsyncTransaction

from .dbapi20 import BINARY, DATETIME, NUMBER, ROWID, STRING, BinaryDumper
from .dbapi20 import Binary, Date, DateFromTicks, Time, TimeFromTicks
from .dbapi20 import Timestamp, TimestampFromTicks

from .version import __version__

# register default adapters
types.register_default_globals(global_adapters)

# DBAPI compliancy
connect = Connection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"
BinaryDumper.register(Binary, global_adapters)  # dbapi20


# Override adapters with fast version if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    _psycopg3.register_builtin_c_adapters()


# Note: defining the exported methods helps both Sphynx in documenting that
# this is the canonical place to obtain them and should be used by MyPy too,
# so that function signatures are consistent with the documentation.
__all__ = [
    "AsyncConnection",
    "AsyncCopy",
    "AsyncCursor",
    "AsyncTransaction",
    "Column",
    "Connection",
    "Copy",
    "Cursor",
    "Notify",
    "Rollback",
    "Transaction",
]
