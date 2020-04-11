"""
Generators implementing communication protocols with the libpq

Certain operations (connection, querying) are an interleave of libpq calls and
waiting for the socket to be ready. This module contains the code to execute
the operations, yielding a polling state whenever there is to wait. The
functions in the `waiting` module are the ones who wait more or less
cooperatively for the socket to be ready and make these generators continue.
"""

# Copyright (C) 2020 The Psycopg Team

import logging
from typing import Generator, List, Tuple
from .waiting import Wait, Ready

from . import pq
from . import errors as e

ConnectGen = Generator[Tuple[int, Wait], Ready, pq.PGconn]
QueryGen = Generator[Tuple[int, Wait], Ready, List[pq.PGresult]]

logger = logging.getLogger(__name__)


def connect(conninfo: str) -> ConnectGen:
    """
    Generator to create a database connection without blocking.

    Yield pairs (fileno, `Wait`) whenever an operation would block. The
    generator can be restarted sending the appropriate `Ready` state when
    the file descriptor is ready.
    """
    conn = pq.PGconn.connect_start(conninfo.encode("utf8"))
    logger.debug("connection started, status %s", conn.status.name)
    while 1:
        if conn.status == pq.ConnStatus.BAD:
            raise e.OperationalError(
                f"connection is bad: {pq.error_message(conn)}"
            )

        status = conn.connect_poll()
        logger.debug("connection polled, status %s", conn.status.name)
        if status == pq.PollingStatus.OK:
            break
        elif status == pq.PollingStatus.READING:
            yield conn.socket, Wait.R
        elif status == pq.PollingStatus.WRITING:
            yield conn.socket, Wait.W
        elif status == pq.PollingStatus.FAILED:
            raise e.OperationalError(
                f"connection failed: {pq.error_message(conn)}"
            )
        else:
            raise e.InternalError(f"unexpected poll status: {status}")

    conn.nonblocking = 1
    return conn


def execute(pgconn: pq.PGconn) -> QueryGen:
    """
    Generator returning query results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Yield pairs (fileno, `Wait`) whenever an operation would block. The
    generator can be restarted sending the appropriate `Ready` state when
    the file descriptor is ready.

    Return the list of results returned by the database (whether success
    or error).
    """
    results: List[pq.PGresult] = []

    while 1:
        f = pgconn.flush()
        if f == 0:
            break

        ready = yield pgconn.socket, Wait.RW
        if ready & Ready.R:
            pgconn.consume_input()
        continue

    while 1:
        pgconn.consume_input()
        if pgconn.is_busy():
            ready = yield pgconn.socket, Wait.R
        res = pgconn.get_result()
        if res is None:
            break
        results.append(res)
        if res.status in (
            pq.ExecStatus.COPY_IN,
            pq.ExecStatus.COPY_OUT,
            pq.ExecStatus.COPY_BOTH,
        ):
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

    return results
