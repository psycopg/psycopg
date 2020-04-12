"""
psycopg3 libpq wrapper

This package exposes the libpq functionalities as Python objects and functions.

The real implementation (the binding to the C library) is
implementation-dependant but all the implementations share the same interface.
"""

# Copyright (C) 2020 The Psycopg Team

from types import ModuleType

from .enums import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)
from .encodings import py_codecs
from .misc import error_message, ConninfoOption


def import_libpq() -> ModuleType:
    """
    Find the best libpw wrapper available.
    """
    from . import pq_ctypes

    return pq_ctypes


pq_module = import_libpq()

version = pq_module.version
PGconn = pq_module.PGconn
PGresult = pq_module.PGresult
PQerror = pq_module.PQerror
Conninfo = pq_module.Conninfo
Escaping = pq_module.Escaping

__all__ = (
    "ConnStatus",
    "PollingStatus",
    "TransactionStatus",
    "ExecStatus",
    "Ping",
    "DiagnosticField",
    "Format",
    "PGconn",
    "Conninfo",
    "PQerror",
    "error_message",
    "ConninfoOption",
    "py_codecs",
    "version",
)
