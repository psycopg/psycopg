"""
psycopg3 libpq wrapper

This package exposes the libpq functionalities as Python objects and functions.

The real implementation (the binding to the C library) is
implementation-dependant but all the implementations share the same interface.
"""

# Copyright (C) 2020 The Psycopg Team

import os
import logging
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

logger = logging.getLogger(__name__)


def import_libpq() -> ModuleType:
    """
    Find the best libpw wrapper available.
    """
    impl = os.environ.get("PSYCOPG3_IMPL", "").lower()

    if not impl or impl == "c":
        try:
            from . import pq_cython
        except Exception as e:
            if not impl:
                logger.debug(f"C pq wrapper not available: %s", e)
            else:
                raise ImportError(
                    f"requested pq implementation '{impl}' not available"
                ) from e
        else:
            return pq_cython

    if not impl or impl == "ctypes":
        try:
            from . import pq_ctypes
        except Exception as e:
            if not impl:
                logger.debug(f"ctypes pq wrapper not available: %s", e)
            else:
                raise ImportError(
                    f"requested pq implementation '{impl}' not available"
                ) from e
        else:
            return pq_ctypes

    if impl:
        raise ImportError(f"requested pq impementation '{impl}' unknown")
    else:
        raise ImportError(f"no pq wrapper available")


pq_module = import_libpq()

__impl__ = pq_module.__impl__
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
