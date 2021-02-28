"""
psycopg3 libpq wrapper

This package exposes the libpq functionalities as Python objects and functions.

The real implementation (the binding to the C library) is
implementation-dependant but all the implementations share the same interface.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import os
import logging
from typing import Callable, List, Type

from .misc import ConninfoOption, PGnotify, PGresAttDesc
from .misc import error_message
from ._enums import ConnStatus, DiagnosticField, ExecStatus, Format
from ._enums import Ping, PollingStatus, TransactionStatus
from . import proto

logger = logging.getLogger(__name__)

__impl__: str
"""The currently loaded implementation of the `!psycopg3.pq` package.

Possible values include ``python``, ``c``, ``binary``.
"""

version: Callable[[], int]
PGconn: Type[proto.PGconn]
PGresult: Type[proto.PGresult]
Conninfo: Type[proto.Conninfo]
Escaping: Type[proto.Escaping]
PGcancel: Type[proto.PGcancel]


def import_from_libpq() -> None:
    """
    Import pq objects implementation from the best libpq wrapper available.

    If an implementation is requested try to import only it, otherwise
    try to import the best implementation available.
    """
    # import these names into the module on success as side effect
    global __impl__, version, PGconn, PGresult, Conninfo, Escaping, PGcancel

    impl = os.environ.get("PSYCOPG3_IMPL", "").lower()
    module = None
    attempts: List[str] = []

    def handle_error(name: str, e: Exception) -> None:
        if not impl:
            msg = f"error importing '{name}' wrapper: {e}"
            logger.debug(msg)
            attempts.append(msg)
        else:
            msg = f"error importing requested '{name}' wrapper: {e}"
            raise ImportError(msg) from e

    # The best implementation: fast but requires the system libpq installed
    if not impl or impl == "c":
        try:
            # TODO: extension module not recognised by mypy?
            from psycopg3_c import pq as module  # type: ignore
        except Exception as e:
            handle_error("c", e)

    # Second best implementation: fast and stand-alone
    if not module and (not impl or impl == "binary"):
        try:
            from psycopg3_binary import pq as module  # type: ignore
        except Exception as e:
            handle_error("binary", e)

    # Pure Python implementation, slow and requires the system libpq installed.
    if not module and (not impl or impl == "python"):
        try:
            from . import pq_ctypes as module  # type: ignore[no-redef]
        except Exception as e:
            handle_error("python", e)

    if module:
        __impl__ = module.__impl__
        version = module.version
        PGconn = module.PGconn
        PGresult = module.PGresult
        Conninfo = module.Conninfo
        Escaping = module.Escaping
        PGcancel = module.PGcancel
    elif impl:
        raise ImportError(f"requested pq impementation '{impl}' unknown")
    else:
        sattempts = "\n".join(f"- {attempt}" for attempt in attempts)
        raise ImportError(
            f"""\
no pq wrapper available.
Attempts made:
{sattempts}"""
        )


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
    "PGnotify",
    "Conninfo",
    "PGresAttDesc",
    "error_message",
    "ConninfoOption",
    "version",
)
