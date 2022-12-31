"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.object cimport PyObject_CallFunctionObjArgs

from typing import List

from psycopg import errors as e
from psycopg.pq import abc, error_message
from psycopg.abc import PipelineCommand, PQGen
from psycopg._enums import Wait, Ready
from psycopg._compat import Deque
from psycopg._encodings import conninfo_encoding

# TODO: give this some order NOMERGE
cdef object WAIT_W = Wait.W
cdef object WAIT_R = Wait.R
cdef object WAIT_RW = Wait.RW
cdef object PY_READY_R = Ready.R
cdef object PY_READY_W = Ready.W
cdef object PY_READY_RW = Ready.RW
cdef int READY_R = Ready.R
cdef int READY_W = Ready.W
cdef int READY_RW = Ready.RW
cdef int CWAIT_W = Wait.W
cdef int CWAIT_R = Wait.R
cdef int CWAIT_RW = Wait.RW

cpdef pq.PGconn connect(conninfo: str):
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


cpdef pq.PGresult execute_command(
    pq.PGconn pgconn, const char *command, int result_format = PQ_TEXT
):
    """
    Execute a command as a string and fetch the results back from the server.

    Always send the command using the extended protocol, even if it has no
    parameter.
    """
    if pgconn._pgconn_ptr is NULL:
        raise e.OperationalError("the connection is closed")

    cdef int rv
    cdef libpq.PGresult *pgres

    with nogil:
        if 0 == libpq.PQsendQueryParams(
            pgconn._pgconn_ptr, command, 0, NULL, NULL, NULL, NULL, result_format
        ):
            raise e.OperationalError(
                f"sending query and params failed: {error_message(pgconn)}"
            )

        if 0 > _flush(pgconn._pgconn_ptr):
            raise e.OperationalError(f"flushing failed: {error_message(pgconn)}")

    results = fetch_many(pgconn)
    return results[0]


cpdef list execute_query(
    pq.PGconn pgconn,
    query: PostgresQuery,
    int result_format = PQ_TEXT,
    force_extended: bool = False,
):
    """
    Execute a query and fetch the results back from the server.
    """
    if force_extended or query.params or result_format == PQ_BINARY:
        pgconn.send_query_params(
            query.query, query.params, query.types, query.formats, result_format
        )
    else:
        # If we can, let's use simple query protocol,
        # as it can execute more than one statement in a single query.
        pgconn.send_query(query.query)

    return flush_and_fetch(pgconn)


cpdef object prepare_query(pq.PGconn pgconn, const char *name, query):
    """
    Prepare a query for prepared statement execution.
    """
    pgconn.send_prepare(name, query.query, param_types=query.types)
    cdef list results = flush_and_fetch(pgconn)
    cdef pq.PGresult result = results[0]
    if result.status == libpq.PGRES_FATAL_ERROR:
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)


cpdef list execute_prepared_query(
    pq.PGconn pgconn, const char *name, query, int result_format = PQ_TEXT
):
    """
    Execute a prepared statement with given parameters and fetch the results.
    """
    pgconn.send_query_prepared(name, query.params, query.formats, result_format)
    return (flush_and_fetch(pgconn))


cpdef list flush_and_fetch(pq.PGconn pgconn):
    """
    Generator sending a query and returning results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Return the list of results returned by the database (whether success
    or error).
    """
    flush(pgconn)
    rv = fetch_many(pgconn)
    return rv


def flush(pq.PGconn pgconn):
    """
    Generator to send a query to the server without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    After this generator has finished you may want to cycle using `fetch()`
    to retrieve the results available.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr

    if pgconn_ptr == NULL:
        raise e.OperationalError(f"sending failed: the connection is closed")

    if 0 > _flush(pgconn_ptr):
        raise e.OperationalError(f"flushing failed: {error_message(pgconn)}")


cdef int _flush(libpq.PGconn *pgconn_ptr) nogil:
    """
    Internal implementation of flush. Can be called without GIL.

    Return 0 in case of success, otherwise < 0. You can find the error
    on the connection.
    """
    cdef int frv
    cdef int status

    while True:
        frv = libpq.PQflush(pgconn_ptr)
        if frv == 0:
            break
        elif frv < 0:
            return -1

        # TODO NOMERGE remove
        with gil:
            status = wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_RW)

        if status & READY_R:
            # This call may read notifies which will be saved in the
            # PGconn buffer and passed to Python later.
            if 1 != libpq.PQconsumeInput(pgconn_ptr):
                return -2

    return 0


cpdef list fetch_many(pq.PGconn pgconn):
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
        result = fetch(pgconn)
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
            break

    _consume_notifies(pgconn)

    return results


cpdef pq.PGresult fetch(pq.PGconn pgconn):
    """
    Generator retrieving a single result from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return a result from the database (whether success or error).
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int fileno = libpq.PQsocket(pgconn_ptr)
    cdef libpq.PGresult *pgres

    while True:
        if not libpq.PQisBusy(pgconn_ptr):
            break

        wait_ng(fileno, CWAIT_R)

        if 1 != libpq.PQconsumeInput(pgconn_ptr):
            raise e.OperationalError(
                f"consuming input failed: {error_message(pgconn)}")

    pgres = libpq.PQgetResult(pgconn_ptr)
    return pq.PGresult._from_ptr(pgres) if pgres is not NULL else None


cpdef list pipeline_communicate(
    pq.PGconn pgconn, commands: Deque[PipelineCommand]
):
    """Generator to send queries from a connection in pipeline mode while also
    receiving results.

    Return a list results, including single PIPELINE_SYNC elements.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int fileno = libpq.PQsocket(pgconn_ptr)
    cdef int status
    cdef int ready
    cdef libpq.PGresult *pgres
    cdef list res = []
    cdef list results = []
    cdef pq.PGresult r

    while True:
        ready = wait_ng(fileno, CWAIT_RW)

        if ready & READY_R:
            if 1 != libpq.PQconsumeInput(pgconn_ptr):
                raise e.OperationalError(
                    f"consuming input failed: {error_message(pgconn)}")

            _consume_notifies(pgconn)

            res: List[PGresult] = []
            while True:
                if libpq.PQisBusy(pgconn_ptr):
                    break

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


cdef int _consume_notifies(pq.PGconn pgconn) except -1:
    cdef object notify_handler = pgconn.notify_handler
    cdef libpq.PGconn *pgconn_ptr
    cdef libpq.PGnotify *notify

    if notify_handler is not None:
        while True:
            pynotify = pgconn.notifies()
            if pynotify is None:
                break
            PyObject_CallFunctionObjArgs(
                notify_handler, <PyObject *>pynotify, NULL
            )
    else:
        pgconn_ptr = pgconn._pgconn_ptr
        while True:
            notify = libpq.PQnotifies(pgconn_ptr)
            if notify is NULL:
                break
            libpq.PQfreemem(notify)

    return 0
