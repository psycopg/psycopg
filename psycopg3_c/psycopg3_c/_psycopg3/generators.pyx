"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020-2021 The Psycopg Team

from cpython.object cimport PyObject_CallFunctionObjArgs

import logging
from typing import List

from psycopg3 import errors as e
from psycopg3.pq import proto, error_message, PQerror
from psycopg3.proto import PQGen
from psycopg3.waiting import Wait, Ready

cdef object WAIT_W = Wait.W
cdef object WAIT_R = Wait.R
cdef object WAIT_RW = Wait.RW
cdef int READY_R = Ready.R

def connect(conninfo: str) -> PQGenConn[proto.PGconn]:
    """
    Generator to create a database connection without blocking.

    """
    cdef pq.PGconn conn = pq.PGconn.connect_start(conninfo.encode("utf8"))
    logger.debug("connection started, status %s", conn.status)
    cdef libpq.PGconn *pgconn_ptr = conn.pgconn_ptr
    cdef int conn_status = libpq.PQstatus(pgconn_ptr)
    cdef int poll_status

    while 1:
        if conn_status == libpq.CONNECTION_BAD:
            raise e.OperationalError(
                f"connection is bad: {error_message(conn)}"
            )

        poll_status = libpq.PQconnectPoll(pgconn_ptr)
        logger.debug("connection polled, status %s", conn.status)
        if poll_status == libpq.PGRES_POLLING_OK:
            break
        elif poll_status == libpq.PGRES_POLLING_READING:
            yield (libpq.PQsocket(pgconn_ptr), WAIT_R)
        elif poll_status == libpq.PGRES_POLLING_WRITING:
            yield (libpq.PQsocket(pgconn_ptr), WAIT_W)
        elif poll_status == libpq.PGRES_POLLING_FAILED:
            raise e.OperationalError(
                f"connection failed: {error_message(conn)}"
            )
        else:
            raise e.InternalError(f"unexpected poll status: {poll_status}")

    conn.nonblocking = 1
    return conn


def execute(pq.PGconn pgconn) -> PQGen[List[proto.PGresult]]:
    """
    Generator sending a query and returning results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Return the list of results returned by the database (whether success
    or error).
    """
    cdef list results = []
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
                raise PQerror(
                    f"consuming input failed: {error_message(pgconn)}")
        continue

    cdef object notify_handler = pgconn.notify_handler

    # Fetching the result
    while 1:
        with nogil:
            cires = libpq.PQconsumeInput(pgconn_ptr)
            if cires == 1:
                ibres = libpq.PQisBusy(pgconn_ptr)

        if 1 != cires:
            raise PQerror(
                f"consuming input failed: {error_message(pgconn)}")
        if ibres:
            yield WAIT_R
            continue

        # Consume notifies
        if notify_handler is not None:
            while 1:
                pynotify = pgconn.notifies()
                if pynotify is None:
                    break
                PyObject_CallFunctionObjArgs(
                    notify_handler, <PyObject *>pynotify, NULL
                )
        else:
            while 1:
                notify = libpq.PQnotifies(pgconn_ptr)
                if notify is NULL:
                    break
                libpq.PQfreemem(notify)

        pgres = libpq.PQgetResult(pgconn_ptr)
        if pgres is NULL:
            break
        results.append(pq.PGresult._from_ptr(pgres))

        status = libpq.PQresultStatus(pgres)
        if status in (libpq.PGRES_COPY_IN, libpq.PGRES_COPY_OUT, libpq.PGRES_COPY_BOTH):
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

    return results
