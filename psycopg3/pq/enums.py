"""
libpq enum definitions for psycopg3
"""

# Copyright (C) 2020 The Psycopg Team

from enum import IntEnum, auto


class ConnStatus(IntEnum):
    CONNECTION_OK = 0
    CONNECTION_BAD = auto()

    CONNECTION_STARTED = auto()
    CONNECTION_MADE = auto()
    CONNECTION_AWAITING_RESPONSE = auto()
    CONNECTION_AUTH_OK = auto()
    CONNECTION_SETENV = auto()
    CONNECTION_SSL_STARTUP = auto()
    CONNECTION_NEEDED = auto()
    CONNECTION_CHECK_WRITABLE = auto()
    CONNECTION_CONSUME = auto()
    CONNECTION_GSS_STARTUP = auto()
    CONNECTION_CHECK_TARGET = auto()


class PollingStatus(IntEnum):
    PGRES_POLLING_FAILED = 0
    PGRES_POLLING_READING = auto()
    PGRES_POLLING_WRITING = auto()
    PGRES_POLLING_OK = auto()
    PGRES_POLLING_ACTIVE = auto()


class ExecStatus(IntEnum):
    PGRES_EMPTY_QUERY = 0
    PGRES_COMMAND_OK = auto()
    PGRES_TUPLES_OK = auto()
    PGRES_COPY_OUT = auto()
    PGRES_COPY_IN = auto()
    PGRES_BAD_RESPONSE = auto()
    PGRES_NONFATAL_ERROR = auto()
    PGRES_FATAL_ERROR = auto()
    PGRES_COPY_BOTH = auto()
    PGRES_SINGLE_TUPLE = auto()


class TransactionStatus(IntEnum):
    PQTRANS_IDLE = 0
    PQTRANS_ACTIVE = auto()
    PQTRANS_INTRANS = auto()
    PQTRANS_INERROR = auto()
    PQTRANS_UNKNOWN = auto()


class Ping(IntEnum):
    PQPING_OK = 0
    PQPING_REJECT = auto()
    PQPING_NO_RESPONSE = auto()
    PQPING_NO_ATTEMPT = auto()


class DiagnosticField(IntEnum):
    # from postgres_ext.h
    PG_DIAG_SEVERITY = ord("S")
    PG_DIAG_SEVERITY_NONLOCALIZED = ord("V")
    PG_DIAG_SQLSTATE = ord("C")
    PG_DIAG_MESSAGE_PRIMARY = ord("M")
    PG_DIAG_MESSAGE_DETAIL = ord("D")
    PG_DIAG_MESSAGE_HINT = ord("H")
    PG_DIAG_STATEMENT_POSITION = ord("P")
    PG_DIAG_INTERNAL_POSITION = ord("p")
    PG_DIAG_INTERNAL_QUERY = ord("q")
    PG_DIAG_CONTEXT = ord("W")
    PG_DIAG_SCHEMA_NAME = ord("s")
    PG_DIAG_TABLE_NAME = ord("t")
    PG_DIAG_COLUMN_NAME = ord("c")
    PG_DIAG_DATATYPE_NAME = ord("d")
    PG_DIAG_CONSTRAINT_NAME = ord("n")
    PG_DIAG_SOURCE_FILE = ord("F")
    PG_DIAG_SOURCE_LINE = ord("L")
    PG_DIAG_SOURCE_FUNCTION = ord("R")
