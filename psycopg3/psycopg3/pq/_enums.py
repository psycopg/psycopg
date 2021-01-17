"""
libpq enum definitions for psycopg3
"""

# Copyright (C) 2020-2021 The Psycopg Team

from enum import IntEnum, auto


class ConnStatus(IntEnum):
    """
    Current status of the connection.
    """

    __module__ = "psycopg3.pq"

    OK = 0
    """The connection is in a working state."""
    BAD = auto()
    """The connection is closed."""

    STARTED = auto()
    MADE = auto()
    AWAITING_RESPONSE = auto()
    AUTH_OK = auto()
    SETENV = auto()
    SSL_STARTUP = auto()
    NEEDED = auto()
    CHECK_WRITABLE = auto()
    CONSUME = auto()
    GSS_STARTUP = auto()
    CHECK_TARGET = auto()


class PollingStatus(IntEnum):
    """
    The status of the socket during a connection.

    If ``READING`` or ``WRITING`` you may select before polling again.
    """

    __module__ = "psycopg3.pq"

    FAILED = 0
    """Connection attempt failed."""
    READING = auto()
    """Will have to wait before reading new data."""
    WRITING = auto()
    """Will have to wait before writing new data."""
    OK = auto()
    """Connection completed."""

    ACTIVE = auto()


class ExecStatus(IntEnum):
    """
    The status of a command.
    """

    __module__ = "psycopg3.pq"

    EMPTY_QUERY = 0
    """The string sent to the server was empty."""

    COMMAND_OK = auto()
    """Successful completion of a command returning no data."""

    TUPLES_OK = auto()
    """
    Successful completion of a command returning data (such as a SELECT or SHOW).
    """

    COPY_OUT = auto()
    """Copy Out (from server) data transfer started."""

    COPY_IN = auto()
    """Copy In (to server) data transfer started."""

    BAD_RESPONSE = auto()
    """The server's response was not understood."""

    NONFATAL_ERROR = auto()
    """A nonfatal error (a notice or warning) occurred."""

    FATAL_ERROR = auto()
    """A fatal error occurred."""

    COPY_BOTH = auto()
    """
    Copy In/Out (to and from server) data transfer started.

    This feature is currently used only for streaming replication, so this
    status should not occur in ordinary applications.
    """

    SINGLE_TUPLE = auto()
    """
    The PGresult contains a single result tuple from the current command.

    This status occurs only when single-row mode has been selected for the
    query.
    """


class TransactionStatus(IntEnum):
    """
    The transaction status of a connection.
    """

    __module__ = "psycopg3.pq"

    IDLE = 0
    """Connection ready, no transaction active."""

    ACTIVE = auto()
    """A command is in progress."""

    INTRANS = auto()
    """Connection idle in an open transaction."""

    INERROR = auto()
    """An error happened in the current transaction."""

    UNKNOWN = auto()
    """Unknown connection state, broken connection."""


class Ping(IntEnum):
    """Response from a ping attempt."""

    __module__ = "psycopg3.pq"

    OK = 0
    """
    The server is running and appears to be accepting connections.
    """

    REJECT = auto()
    """
    The server is running but is in a state that disallows connections.
    """

    NO_RESPONSE = auto()
    """
    The server could not be contacted.
    """

    NO_ATTEMPT = auto()
    """
    No attempt was made to contact the server.
    """


class DiagnosticField(IntEnum):
    """
    Fields in an error report.
    """

    __module__ = "psycopg3.pq"

    # from postgres_ext.h
    SEVERITY = ord("S")
    SEVERITY_NONLOCALIZED = ord("V")
    SQLSTATE = ord("C")
    MESSAGE_PRIMARY = ord("M")
    MESSAGE_DETAIL = ord("D")
    MESSAGE_HINT = ord("H")
    STATEMENT_POSITION = ord("P")
    INTERNAL_POSITION = ord("p")
    INTERNAL_QUERY = ord("q")
    CONTEXT = ord("W")
    SCHEMA_NAME = ord("s")
    TABLE_NAME = ord("t")
    COLUMN_NAME = ord("c")
    DATATYPE_NAME = ord("d")
    CONSTRAINT_NAME = ord("n")
    SOURCE_FILE = ord("F")
    SOURCE_LINE = ord("L")
    SOURCE_FUNCTION = ord("R")


class Format(IntEnum):
    """
    Enum representing the format of a query argument or return value.

    These values are only the ones managed by the libpq. `~psycopg3` may also
    support automatically-chosen values: see `psycopg3.adapt.Format`.
    """

    __module__ = "psycopg3.pq"

    TEXT = 0
    """Text parameter."""
    BINARY = 1
    """Binary parameter."""
