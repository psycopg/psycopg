"""
libpq enum definitions for psycopg3
"""

# Copyright (C) 2020 The Psycopg Team

from enum import IntEnum, auto


class ConnStatus(IntEnum):
    OK = 0
    BAD = auto()
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
    FAILED = 0
    READING = auto()
    WRITING = auto()
    OK = auto()
    ACTIVE = auto()


class ExecStatus(IntEnum):
    EMPTY_QUERY = 0
    COMMAND_OK = auto()
    TUPLES_OK = auto()
    COPY_OUT = auto()
    COPY_IN = auto()
    BAD_RESPONSE = auto()
    NONFATAL_ERROR = auto()
    FATAL_ERROR = auto()
    COPY_BOTH = auto()
    SINGLE_TUPLE = auto()


class TransactionStatus(IntEnum):
    IDLE = 0
    ACTIVE = auto()
    INTRANS = auto()
    INERROR = auto()
    UNKNOWN = auto()


class Ping(IntEnum):
    OK = 0
    REJECT = auto()
    NO_RESPONSE = auto()
    NO_ATTEMPT = auto()


class DiagnosticField(IntEnum):
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
    TEXT = 0
    BINARY = 1
