"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020-2021 The Psycopg Team

from cpython.object cimport PyObject_CallFunctionObjArgs

import logging
from typing import List

from psycopg import errors as e
from psycopg.pq import abc, error_message
from psycopg.abc import PQGen
from psycopg.waiting import Wait, Ready

cdef object WAIT_W = Wait.W
cdef object WAIT_R = Wait.R
cdef object WAIT_RW = Wait.RW
cdef int READY_R = Ready.R

def connect(conninfo: str) -> PQGenConn[abc.PGconn]:
    """
    Generator to create a database connection without blocking.

    """
    cdef pq.PGconn conn = pq.PGconn.connect_start(conninfo.encode())
    logger.debug("connection started, status %s", conn.status)
    cdef libpq.PGconn *pgconn_ptr = conn._pgconn_ptr
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


def execute(pq.PGconn pgconn) -> PQGen[List[abc.PGresult]]:
    """
    Generator sending a query and returning results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Return the list of results returned by the database (whether success
    or error).
    """
    yield from send(pgconn)
    rv = yield from fetch_many(pgconn)
    return rv


def send(pq.PGconn pgconn) -> PQGen[None]:
    """
    Generator to send a query to the server without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    After this generator has finished you may want to cycle using `fetch()`
    to retrieve the results available.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int status
    cdef int cires

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
                raise e.OperationalError(
                    f"consuming input failed: {error_message(pgconn)}")


def fetch_many(pq.PGconn pgconn) -> PQGen[List[PGresult]]:
    """
    Generator retrieving results from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0 or pgconn.pipeline_sync() been successfully called.

    Return the list of results returned by the database (whether success
    or error).
    """
    cdef list results = []
    cdef int status
    cdef pq.PGresult result
    cdef libpq.PGresult *pgres

    while 1:
        result = yield from fetch(pgconn)
        if result is None:
            break
        results.append(result)
        pgres = result._pgresult_ptr

        status = libpq.PQresultStatus(pgres)
        if status in (libpq.PGRES_COPY_IN, libpq.PGRES_COPY_OUT, libpq.PGRES_COPY_BOTH):
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

    return results


def fetch(pq.PGconn pgconn) -> PQGen[Optional[PGresult]]:
    """
    Generator retrieving a single result from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0 or pgconn.pipeline_sync() been successfully called.

    Return a result from the database (whether success or error).
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef libpq.PGnotify *notify
    cdef int cires, ibres
    cdef object notify_handler = pgconn.notify_handler
    cdef libpq.PGresult *pgres

    while 1:
        with nogil:
            cires = libpq.PQconsumeInput(pgconn_ptr)
            if cires == 1:
                ibres = libpq.PQisBusy(pgconn_ptr)

        if 1 != cires:
            raise e.OperationalError(
                f"consuming input failed: {error_message(pgconn)}")
        if not ibres:
            break
        yield WAIT_R

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
        return None
    return pq.PGresult._from_ptr(pgres)
