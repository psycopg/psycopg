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


class PostgresPollingStatus(IntEnum):
    PGRES_POLLING_FAILED = 0
    PGRES_POLLING_READING = auto()
    PGRES_POLLING_WRITING = auto()
    PGRES_POLLING_OK = auto()
    PGRES_POLLING_ACTIVE = auto()


class TransactionStatus(IntEnum):
    PQTRANS_IDLE = 0
    PQTRANS_ACTIVE = auto()
    PQTRANS_INTRANS = auto()
    PQTRANS_INERROR = auto()
    PQTRANS_UNKNOWN = auto()


class PGPing(IntEnum):
    PQPING_OK = 0
    PQPING_REJECT = auto()
    PQPING_NO_RESPONSE = auto()
    PQPING_NO_ATTEMPT = auto()
