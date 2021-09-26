"""
psycopg -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020-2021 The Psycopg Team

import logging

from . import pq
from . import types
from . import postgres
from .copy import Copy, AsyncCopy
from ._enums import IsolationLevel
from .cursor import Cursor
from .errors import Warning, Error, InterfaceError, DatabaseError
from .errors import DataError, OperationalError, IntegrityError
from .errors import InternalError, ProgrammingError, NotSupportedError
from ._column import Column
from .conninfo import ConnectionInfo
from .connection import BaseConnection, Connection, Notify
from .transaction import Rollback, Transaction, AsyncTransaction
from .cursor_async import AsyncCursor
from .server_cursor import AsyncServerCursor, ServerCursor
from .connection_async import AsyncConnection

from . import dbapi20
from .dbapi20 import BINARY, DATETIME, NUMBER, ROWID, STRING
from .dbapi20 import Binary, BinaryTextDumper, BinaryBinaryDumper
from .dbapi20 import Date, DateFromTicks, Time, TimeFromTicks
from .dbapi20 import Timestamp, TimestampFromTicks

from .version import __version__

# Set the logger to a quiet default, can be enabled if needed
logger = logging.getLogger("psycopg")
if logger.level == logging.NOTSET:
    logger.setLevel(logging.WARNING)

# register default adapters for PostgreSQL
adapters = postgres.adapters  # exposed by the package

# DBAPI compliancy
connect = Connection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"

postgres.register_default_adapters(adapters)

# After the default one because they can deal with the bytea oid better
dbapi20.register_dbapi20_adapters(adapters)

# Must come after all the types are registered
types.array.register_all_arrays(adapters)

# Note: defining the exported methods helps both Sphynx in documenting that
# this is the canonical place to obtain them and should be used by MyPy too,
# so that function signatures are consistent with the documentation.
__all__ = [
    "__version__",
    "AsyncConnection",
    "AsyncCopy",
    "AsyncCursor",
    "AsyncServerCursor",
    "AsyncTransaction",
    "BaseConnection",
    "Column",
    "Connection",
    "Copy",
    "Cursor",
    "IsolationLevel",
    "Notify",
    "Rollback",
    "ServerCursor",
    "Transaction",
    # DBAPI exports
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
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
]
