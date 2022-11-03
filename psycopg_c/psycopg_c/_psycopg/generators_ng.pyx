"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.object cimport PyObject_CallFunctionObjArgs

from typing import List

from psycopg import errors as e
from psycopg.pq import abc, error_message
from psycopg.abc import PipelineCommand
from psycopg._enums import Wait, Ready
from psycopg._compat import Deque
from psycopg._encodings import conninfo_encoding

cdef int CWAIT_W = Wait.W
cdef int CWAIT_R = Wait.R
cdef int CWAIT_RW = Wait.RW

cpdef pq.PGconn connect_ng(conninfo: str):
    """
    Generator to create a database connection without blocking.

    """
    cdef pq.PGconn conn = pq.PGconn.connect_start(conninfo.encode())
    cdef libpq.PGconn *pgconn_ptr = conn._pgconn_ptr
    cdef int conn_status = libpq.PQstatus(pgconn_ptr)
    cdef int poll_status

    while True:
        if conn_status == libpq.CONNECTION_BAD:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection is bad: {error_message(conn, encoding=encoding)}",
                pgconn=conn
            )

        poll_status = libpq.PQconnectPoll(pgconn_ptr)
        if poll_status == libpq.PGRES_POLLING_OK:
            break
        elif poll_status == libpq.PGRES_POLLING_READING:
            wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_R)
        elif poll_status == libpq.PGRES_POLLING_WRITING:
            wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_W)
        elif poll_status == libpq.PGRES_POLLING_FAILED:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection failed: {error_message(conn, encoding=encoding)}",
                pgconn=conn
            )
        else:
            raise e.InternalError(
                f"unexpected poll status: {poll_status}", pgconn=conn
            )

    conn.nonblocking = 1
    return conn


cpdef list execute_ng(pq.PGconn pgconn):
    """
    Generator sending a query and returning results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Return the list of results returned by the database (whether success
    or error).
    """
    send_ng(pgconn)
    rv = fetch_many_ng(pgconn)
    return rv


cpdef send_ng(pq.PGconn pgconn):
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

    while True:
        if pgconn.flush() == 0:
            break

        status = wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_RW)
        if status & READY_R:
            with nogil:
                # This call may read notifies which will be saved in the
                # PGconn buffer and passed to Python later.
                cires = libpq.PQconsumeInput(pgconn_ptr)
            if 1 != cires:
                raise e.OperationalError(
                    f"consuming input failed: {error_message(pgconn)}")


cpdef list fetch_many_ng(pq.PGconn pgconn):
    """
    Generator retrieving results from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return the list of results returned by the database (whether success
    or error).
    """
    cdef list results = []
    cdef int status
    cdef pq.PGresult result
    cdef libpq.PGresult *pgres

    while True:
        result = fetch_ng(pgconn)
        if result is None:
            break
        results.append(result)
        pgres = result._pgresult_ptr

        status = libpq.PQresultStatus(pgres)
        if (
            status == libpq.PGRES_COPY_IN
            or status == libpq.PGRES_COPY_OUT
            or status == libpq.PGRES_COPY_BOTH
        ):
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

        if status == libpq.PGRES_PIPELINE_SYNC:
            # PIPELINE_SYNC is not followed by a NULL, but we return it alone
            # similarly to other result sets.
            assert len(results) == 1, results
            break

    return results


cpdef object fetch_ng(pq.PGconn pgconn):
    """
    Generator retrieving a single result from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return a result from the database (whether success or error).
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int fileno = libpq.PQsocket(pgconn_ptr)
    cdef int cires, ibres = 0
    cdef libpq.PGresult *pgres

    if libpq.PQisBusy(pgconn_ptr):
        wait_ng(fileno, CWAIT_R)
        while True:
            with nogil:
                cires = libpq.PQconsumeInput(pgconn_ptr)
                if cires == 1:
                    ibres = libpq.PQisBusy(pgconn_ptr)

            if 1 != cires:
                raise e.OperationalError(
                    f"consuming input failed: {error_message(pgconn)}")
            if not ibres:
                break
            wait_ng(fileno, CWAIT_R)

    _consume_notifies(pgconn)

    pgres = libpq.PQgetResult(pgconn_ptr)
    if pgres is NULL:
        return None
    return pq.PGresult._from_ptr(pgres)


cpdef list pipeline_communicate_ng(
    pq.PGconn pgconn, commands: Deque[PipelineCommand]
):
    """Generator to send queries from a connection in pipeline mode while also
    receiving results.

    Return a list results, including single PIPELINE_SYNC elements.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int fileno = libpq.PQsocket(pgconn_ptr)
    cdef int cires
    cdef libpq.PGresult *pgres
    cdef list res = []
    cdef list results = []
    cdef pq.PGresult r

    while True:
        ready = wait_ng(fileno, CWAIT_RW)

        if ready & READY_R:
            with nogil:
                cires = libpq.PQconsumeInput(pgconn_ptr)
            if 1 != cires:
                raise e.OperationalError(
                    f"consuming input failed: {error_message(pgconn)}")

            _consume_notifies(pgconn)

            res: List[PGresult] = []
            while not libpq.PQisBusy(pgconn_ptr):
                pgres = libpq.PQgetResult(pgconn_ptr)
                if pgres is NULL:
                    if not res:
                        break
                    results.append(res)
                    res = []
                else:
                    status = libpq.PQresultStatus(pgres)
                    r = pq.PGresult._from_ptr(pgres)
                    if status == libpq.PGRES_PIPELINE_SYNC:
                        results.append([r])
                        break
                    else:
                        res.append(r)

        if ready & READY_W:
            pgconn.flush()
            if not commands:
                break
            commands.popleft()()

    return results
