"""
libpq Python wrapper using cython bindings.
"""

# Copyright (C) 2020 The Psycopg Team

from libc.string cimport strlen
from posix.unistd cimport getpid
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AsString, PyBytes_AsStringAndSize
from cpython.buffer cimport PyObject_CheckBuffer, PyBUF_SIMPLE
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_AS_STRING

import logging
from typing import List, Optional, Sequence, Tuple

from psycopg3_c.pq cimport libpq as impl
from psycopg3_c.pq.libpq cimport Oid

from psycopg3.pq.misc import PGnotify, ConninfoOption, PQerror, PGresAttDesc
from psycopg3.pq.misc import error_message
from psycopg3.pq import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)


__impl__ = 'c'

logger = logging.getLogger('psycopg3')


def version():
    return impl.PQlibVersion()


include "pq/pgconn.pyx"
include "pq/pgresult.pyx"
include "pq/pgcancel.pyx"
include "pq/conninfo.pyx"
include "pq/escaping.pyx"
include "pq/pqbuffer.pyx"
