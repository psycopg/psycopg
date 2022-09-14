"""
libpq Python wrapper using cython bindings.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg_c.pq cimport libpq

import logging

from psycopg import errors as e
from psycopg.pq import Format
from psycopg.pq.misc import error_message

logger = logging.getLogger("psycopg")

__impl__ = 'c'
__build_version__ = libpq.PG_VERSION_NUM


def version():
    return libpq.PQlibVersion()


include "pq/pgconn.pyx"
include "pq/pgresult.pyx"
include "pq/pgcancel.pyx"
include "pq/conninfo.pyx"
include "pq/escaping.pyx"
include "pq/pqbuffer.pyx"


# importing the ssl module sets up Python's libcrypto callbacks
import ssl  # noqa

# disable libcrypto setup in libpq, so it won't stomp on the callbacks
# that have already been set up
libpq.PQinitOpenSSL(1, 0)
