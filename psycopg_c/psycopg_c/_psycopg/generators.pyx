"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.object cimport PyObject_CallFunctionObjArgs

from time import monotonic

from psycopg import errors as e
from psycopg.pq import abc
from psycopg.abc import PipelineCommand, PQGen
from psycopg._enums import Wait, Ready
from psycopg._compat import Deque
from psycopg._encodings import conninfo_encoding

cdef object WAIT_W = Wait.W
cdef object WAIT_R = Wait.R
cdef object WAIT_RW = Wait.RW
cdef object PY_READY_NONE = Ready.NONE
cdef object PY_READY_R = Ready.R
cdef object PY_READY_W = Ready.W
cdef object PY_READY_RW = Ready.RW
cdef int READY_NONE = Ready.NONE
cdef int READY_R = Ready.R
cdef int READY_W = Ready.W
cdef int READY_RW = Ready.RW

def connect(conninfo: str, *, timeout: float = 0.0) -> PQGenConn[abc.PGconn]:
    """
    Generator to create a database connection without blocking.
    """
    cdef pq.PGconn conn = pq.PGconn.connect_start(conninfo.encode())
    cdef libpq.PGconn *pgconn_ptr = conn._pgconn_ptr
    cdef int conn_status = libpq.PQstatus(pgconn_ptr)
    cdef int poll_status
    cdef object wait, ready
    cdef float deadline = 0.0

    if timeout:
        deadline = monotonic() + timeout

    while True:
        if conn_status == libpq.CONNECTION_BAD:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection is bad: {conn.get_error_message(encoding)}",
                pgconn=conn
            )

        with nogil:
            poll_status = libpq.PQconnectPoll(pgconn_ptr)

        if poll_status == libpq.PGRES_POLLING_READING \
        or poll_status == libpq.PGRES_POLLING_WRITING:
            wait = WAIT_R if poll_status == libpq.PGRES_POLLING_READING else WAIT_W
            while True:
                ready = yield (libpq.PQsocket(pgconn_ptr), wait)
                if deadline and monotonic() > deadline:
                    raise e.ConnectionTimeout("connection timeout expired")
                if ready:
                    break

        elif poll_status == libpq.PGRES_POLLING_OK:
            break
        elif poll_status == libpq.PGRES_POLLING_FAILED:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection failed: {conn.get_error_message(encoding)}",
                pgconn=e.finish_pgconn(conn),
            )
        else:
            raise e.InternalError(
                f"unexpected poll status: {poll_status}",
                pgconn=e.finish_pgconn(conn),
            )

    conn.nonblocking = 1
    return conn


def cancel(pq.PGcancelConn cancel_conn, *, timeout: float = 0.0) -> PQGenConn[None]:
    cdef libpq.PGcancelConn *pgcancelconn_ptr = cancel_conn.pgcancelconn_ptr
    cdef int status
    cdef float deadline = 0.0

    if timeout:
        deadline = monotonic() + timeout

    while True:
        if deadline and monotonic() > deadline:
            raise e.CancellationTimeout("cancellation timeout expired")
        with nogil:
            status = libpq.PQcancelPoll(pgcancelconn_ptr)
        if status == libpq.PGRES_POLLING_OK:
            break
        elif status == libpq.PGRES_POLLING_READING:
            yield libpq.PQcancelSocket(pgcancelconn_ptr), WAIT_R
        elif status == libpq.PGRES_POLLING_WRITING:
            yield libpq.PQcancelSocket(pgcancelconn_ptr), WAIT_W
        elif status == libpq.PGRES_POLLING_FAILED:
            raise e.OperationalError(
                f"cancellation failed: {cancel_conn.get_error_message()}"
            )
        else:
            raise e.InternalError(f"unexpected poll status: {status}")


def execute(pq.PGconn pgconn) -> PQGen[list[abc.PGresult]]:
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
    cdef int ready
    cdef int cires

    while True:
        if pgconn.flush() == 0:
            break

        while True:
            ready = yield WAIT_RW
            if ready:
                break

        if ready & READY_R:
            with nogil:
                # This call may read notifies which will be saved in the
                # PGconn buffer and passed to Python later.
                cires = libpq.PQconsumeInput(pgconn_ptr)
            if 1 != cires:
                raise e.OperationalError(
                    f"consuming input failed: {pgconn.get_error_message()}")


def fetch_many(pq.PGconn pgconn) -> PQGen[list[PGresult]]:
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
        try:
            result = yield from fetch(pgconn)
        except e.DatabaseError:
            # What might have happened here is that a previuos error
            # disconnected the connection, for example a idle in transaction
            # timeout. Check if we had received an error before, and raise it
            # as exception, because it should contain more details. See #988.
            if any(result.status == libpq.PGRES_FATAL_ERROR for res in results):
                break
            else:
                raise

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

    return results


def fetch(pq.PGconn pgconn) -> PQGen[PGresult | None]:
    """
    Generator retrieving a single result from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return a result from the database (whether success or error).
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int cires, ibres
    cdef libpq.PGresult *pgres
    cdef object ready

    with nogil:
        ibres = libpq.PQisBusy(pgconn_ptr)
    if ibres:
        while True:
            ready = yield WAIT_R
            if ready:
                break

        while True:
            with nogil:
                cires = libpq.PQconsumeInput(pgconn_ptr)
                if cires == 1:
                    ibres = libpq.PQisBusy(pgconn_ptr)

            if 1 != cires:
                raise e.OperationalError(
                    f"consuming input failed: {pgconn.get_error_message()}")
            if not ibres:
                break
            while True:
                ready = yield WAIT_R
                if ready:
                    break

    _consume_notifies(pgconn)

    with nogil:
        pgres = libpq.PQgetResult(pgconn_ptr)
    if pgres is NULL:
        return None
    return pq.PGresult._from_ptr(pgres)


def pipeline_communicate(
    pq.PGconn pgconn, commands: Deque[PipelineCommand]
) -> PQGen[list[list[PGresult]]]:
    """Generator to send queries from a connection in pipeline mode while also
    receiving results.

    Return a list results, including single PIPELINE_SYNC elements.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    cdef int cires
    cdef int status
    cdef int ready
    cdef libpq.PGresult *pgres
    cdef list res = []
    cdef list results = []
    cdef pq.PGresult r

    while True:
        while True:
            ready = yield WAIT_RW
            if ready:
                break

        if ready & READY_R:
            with nogil:
                cires = libpq.PQconsumeInput(pgconn_ptr)
            if 1 != cires:
                raise e.OperationalError(
                    f"consuming input failed: {pgconn.get_error_message()}")

            _consume_notifies(pgconn)

            res: list[PGresult] = []
            while True:
                with nogil:
                    ibres = libpq.PQisBusy(pgconn_ptr)
                    if ibres:
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
                    elif (
                        status == libpq.PGRES_COPY_IN
                        or status == libpq.PGRES_COPY_OUT
                        or status == libpq.PGRES_COPY_BOTH
                    ):
                        # This shouldn't happen, but insisting hard enough, it will.
                        # For instance, in test_executemany_badquery(), with the COPY
                        # statement and the AsyncClientCursor, which disables
                        # prepared statements).
                        # Bail out from the resulting infinite loop.
                        raise e.NotSupportedError(
                            "COPY cannot be used in pipeline mode"
                        )
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
