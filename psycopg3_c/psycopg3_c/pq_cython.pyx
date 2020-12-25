"""
libpq Python wrapper using cython bindings.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg3_c.pq cimport libpq

from psycopg3.pq.misc import PQerror, error_message

from psycopg3.pq import ConnStatus, PollingStatus, ExecStatus
from psycopg3.pq import TransactionStatus, Ping, DiagnosticField, Format


__impl__ = 'c'


def version():
    return libpq.PQlibVersion()


include "pq/pgconn.pyx"
include "pq/pgresult.pyx"
include "pq/pgcancel.pyx"
include "pq/conninfo.pyx"
include "pq/escaping.pyx"
include "pq/pqbuffer.pyx"
