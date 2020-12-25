"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020 The Psycopg Team

import logging
from typing import List

from psycopg3 import errors as e
from psycopg3.proto import PQGen
from psycopg3.waiting import Wait, Ready
from psycopg3 import pq
from psycopg3_c.pq cimport libpq
from psycopg3_c.pq_cython cimport PGconn, PGresult

cdef object WAIT_W = Wait.W
cdef object WAIT_R = Wait.R
cdef object WAIT_RW = Wait.RW
cdef int READY_R = Ready.R

def connect(conninfo: str) -> PQGenConn[pq.proto.PGconn]:
    """
    Generator to create a database connection without blocking.

    """
    cdef PGconn conn = PGconn.connect_start(conninfo.encode("utf8"))
    logger.debug("connection started, status %s", conn.status.name)
    cdef libpq.PGconn *pgconn_ptr = conn.pgconn_ptr
    cdef int conn_status = libpq.PQstatus(pgconn_ptr)
    cdef int poll_status

    while 1:
        if conn_status == libpq.CONNECTION_BAD:
            raise e.OperationalError(
                f"connection is bad: {pq.error_message(conn)}"
            )

        poll_status = libpq.PQconnectPoll(pgconn_ptr)
        logger.debug("connection polled, status %s", conn.status.name)
        if poll_status == libpq.PGRES_POLLING_OK:
            break
        elif poll_status == libpq.PGRES_POLLING_READING:
            yield (libpq.PQsocket(pgconn_ptr), WAIT_R)
        elif poll_status == libpq.PGRES_POLLING_WRITING:
            yield (libpq.PQsocket(pgconn_ptr), WAIT_W)
        elif poll_status == libpq.PGRES_POLLING_FAILED:
            raise e.OperationalError(
                f"connection failed: {pq.error_message(conn)}"
            )
        else:
            raise e.InternalError(f"unexpected poll status: {poll_status}")

    conn.nonblocking = 1
    return conn


def execute(PGconn pgconn) -> PQGen[List[pq.proto.PGresult]]:
    """
    Generator sending a query and returning results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Return the list of results returned by the database (whether success
    or error).
    """
    results: List[pq.proto.PGresult] = []
    cdef libpq.PGconn *pgconn_ptr = pgconn.pgconn_ptr
    cdef int status
    cdef libpq.PGnotify *notify
    cdef libpq.PGresult *pgres
    cdef int cires, ibres

    # Sending the query
    while 1:
        if libpq.PQflush(pgconn_ptr) == 0:
            break

        status = yield WAIT_RW
        if status & READY_R:
            with nogil:
                # This call may read notifies which will be saved in the
                # PGconn buffer and passed to Python later.
                cires = libpq.PQconsumeInput(pgconn_ptr)
            if 1 != cires:
                raise pq.PQerror(
                    f"consuming input failed: {pq.error_message(pgconn)}")
        continue

    # Fetching the result
    while 1:
        with nogil:
            cires = libpq.PQconsumeInput(pgconn_ptr)
            if cires == 1:
                ibres = libpq.PQisBusy(pgconn_ptr)

        if 1 != cires:
            raise pq.PQerror(
                f"consuming input failed: {pq.error_message(pgconn)}")
        if ibres:
            yield WAIT_R
            continue

        # Consume notifies
        if pgconn.notify_handler:
            while 1:
                pynotify = pgconn.notifies()
                if pynotify is None:
                    break
                pgconn.notify_handler(pynotify)
        else:
            while 1:
                notify = libpq.PQnotifies(pgconn_ptr)
                if notify is NULL:
                    break
                libpq.PQfreemem(notify)

        pgres = libpq.PQgetResult(pgconn_ptr)
        if pgres is NULL:
            break
        results.append(PGresult._from_ptr(pgres))

        status = libpq.PQresultStatus(pgres)
        if status in (libpq.PGRES_COPY_IN, libpq.PGRES_COPY_OUT, libpq.PGRES_COPY_BOTH):
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

    return results
