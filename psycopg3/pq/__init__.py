"""
psycopg3 libpq wrapper

This package exposes the libpq functionalities as Python objects and functions.

The real implementation (the binding to the C library) is
implementation-dependant but all the implementations share the same interface.
"""

# Copyright (C) 2020 The Psycopg Team

import os
import logging
from typing import Callable, Type

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
from .misc import error_message, ConninfoOption, PQerror
from . import proto

logger = logging.getLogger(__name__)

__impl__: str
version: Callable[[], int]
PGconn: Type[proto.PGconn]
PGresult: Type[proto.PGresult]
Conninfo: Type[proto.Conninfo]
Escaping: Type[proto.Escaping]


def import_from_libpq() -> None:
    """
    Import pq objects implementation from the best libpw wrapper available.
    """
    global __impl__, version, PGconn, PGresult, Conninfo, Escaping

    impl = os.environ.get("PSYCOPG3_IMPL", "").lower()

    if not impl or impl == "c":
        try:
            # TODO: extension module not recognised by mypy?
            from psycopg3 import _psycopg3  # type: ignore
        except Exception as e:
            if not impl:
                logger.debug(f"C pq wrapper not available: %s", e)
            else:
                raise ImportError(
                    f"requested pq implementation '{impl}' not available"
                ) from e
        else:
            __impl__ = _psycopg3.__impl__
            version = _psycopg3.version
            PGconn = _psycopg3.PGconn
            PGresult = _psycopg3.PGresult
            Conninfo = _psycopg3.Conninfo
            Escaping = _psycopg3.Escaping
            return

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
            __impl__ = pq_ctypes.__impl__
            version = pq_ctypes.version
            PGconn = pq_ctypes.PGconn
            PGresult = pq_ctypes.PGresult
            Conninfo = pq_ctypes.Conninfo
            Escaping = pq_ctypes.Escaping
            return

    if impl:
        raise ImportError(f"requested pq impementation '{impl}' unknown")
    else:
        raise ImportError(f"no pq wrapper available")


import_from_libpq()

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
