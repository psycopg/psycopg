"""
C implementation of generators for the communication protocols with the libpq
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.exc cimport PyErr_Occurred
from cpython.object cimport PyObject_CallFunctionObjArgs
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.list cimport PyList_GET_SIZE, PyList_GET_ITEM
from cpython.tuple cimport PyTuple_GET_SIZE, PyTuple_GET_ITEM

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
    cdef int ready

    while True:
        if conn_status == libpq.CONNECTION_BAD:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection is bad: {error_message(conn, encoding=encoding)}",
                pgconn=conn
            )

        poll_status = libpq.PQconnectPoll(pgconn_ptr)
        ready = 0

        if poll_status == libpq.PGRES_POLLING_OK:
            break
        elif poll_status == libpq.PGRES_POLLING_READING:
            ready = wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_R)
        elif poll_status == libpq.PGRES_POLLING_WRITING:
            ready = wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_W)
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

        if ready < 0:
            wait_dummy_raise()

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
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    if pgconn_ptr is NULL:
        raise e.OperationalError("the connection is closed")

    cdef int rv
    cdef libpq.PGresult *pgres = NULL

    with nogil:
        while True:  # poor man's goto
            if 0 == libpq.PQsendQueryParams(
                pgconn_ptr, command, 0, NULL, NULL, NULL, NULL, result_format
            ):
                break

            if 0 > _flush(pgconn_ptr):
                break

            pgres = fetch_last(pgconn_ptr)
            break

    if pgres == NULL:
        _raise_current_or_from_conn(pgconn)
    else:
        return pq.PGresult._from_ptr(pgres)


cdef object _raise_current_or_from_conn(pq.PGconn pgconn):
    if PyErr_Occurred() == NULL:
        raise e.OperationalError(f"query: {error_message(pgconn)}")
    else:
        wait_dummy_raise()


cpdef list execute_query(
    pq.PGconn pgconn,
    query: PostgresQuery,
    int result_format = PQ_TEXT,
    force_extended: bool = False,
):
    """
    Execute a query and fetch the results back from the server.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    if pgconn_ptr is NULL:
        raise e.OperationalError("the connection is closed")

    cdef const char *command = query.query
    cdef int cnparams = -1
    cdef libpq.Oid *ctypes = NULL
    cdef const char *const *cvalues = NULL
    cdef int *clengths = NULL
    cdef int *cformats = NULL

    cdef libpq.PGresult *pgres = NULL

    # Choose whether to use the extended protocol or not.
    if query.params or force_extended or result_format == PQ_BINARY:
        cnparams = _query_params_args(
            query.params, query.types, query.formats,
            &ctypes, &cvalues, &clengths, &cformats)

    with nogil:
        while 1:
            if cnparams >= 0:
                if 0 == libpq.PQsendQueryParams(
                    pgconn_ptr, command, cnparams, ctypes,
                    cvalues, clengths, cformats, result_format
                ):
                    break
            else:
                if 0 == libpq.PQsendQuery(pgconn_ptr, command):
                    break

            if 0 > _flush(pgconn_ptr):
                break

            # TODO NOMERGE: handle more than one result
            pgres = fetch_last(pgconn_ptr)
            break

    if cnparams > 0:
        _clear_query_params(ctypes, cvalues, clengths, cformats)

    if pgres == NULL:
        _raise_current_or_from_conn(pgconn)
    else:
        return [pq.PGresult._from_ptr(pgres)]


cpdef object prepare_query(pq.PGconn pgconn, const char *name, query):
    """
    Prepare a query for prepared statement execution.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    if pgconn_ptr is NULL:
        raise e.OperationalError("the connection is closed")

    cdef const char *command = query.query
    cdef libpq.Oid *ctypes = NULL

    cdef int cnparams = _query_types_args(query.types, &ctypes)

    cdef libpq.PGresult *pgres = NULL
    with nogil:
        while 1:
            if 0 == libpq.PQsendPrepare(
                pgconn_ptr, name, command, cnparams, ctypes
            ):
                break

            if 0 > _flush(pgconn_ptr):
                break

            pgres = fetch_last(pgconn_ptr)
            break

    if cnparams > 0:
        PyMem_Free(ctypes)

    if pgres == NULL:
        _raise_current_or_from_conn(pgconn)

    # Create a result only if needed to raise an exception.
    cdef int status = libpq.PQresultStatus(pgres)
    if status == libpq.PGRES_FATAL_ERROR:
        # In this branch, ownership is passed to the `result` object, which
        # will delete the `pgres` on de..
        result = pq.PGresult._from_ptr(pgres)
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)
    else:
        # In this branch, no Python wrapper is created, so just delete the
        # result once we know there was no error.
        libpq.PQclear(pgres)


cpdef list execute_prepared_query(
    pq.PGconn pgconn, const char *name, query, int result_format = PQ_TEXT
):
    """
    Execute a prepared statement with given parameters and fetch the results.
    """
    cdef libpq.PGconn *pgconn_ptr = pgconn._pgconn_ptr
    if pgconn_ptr is NULL:
        raise e.OperationalError("the connection is closed")

    cdef int cnparams = -1
    cdef char *const *cvalues = NULL
    cdef int *clengths = NULL
    cdef int *cformats = NULL

    cdef libpq.PGresult *pgres = NULL

    # Choose whether to use the extended protocol or not.
    cnparams = _query_params_args(
        query.params, None, query.formats,
        NULL, &cvalues, &clengths, &cformats)

    with nogil:
        while 1:
            if 0 == libpq.PQsendQueryPrepared(
                pgconn_ptr, name, cnparams,
                cvalues, clengths, cformats, result_format
            ):
                break

            if 0 > _flush(pgconn_ptr):
                break

            pgres = fetch_last(pgconn_ptr)
            break

    if cnparams > 0:
        _clear_query_params(NULL, cvalues, clengths, cformats)

    if pgres == NULL:
        _raise_current_or_from_conn(pgconn)
    else:
        return [pq.PGresult._from_ptr(pgres)]


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
    cdef int ready

    while True:
        frv = libpq.PQflush(pgconn_ptr)
        if frv == 0:
            break
        elif frv < 0:
            return -1

        ready = wait_ng(libpq.PQsocket(pgconn_ptr), CWAIT_RW)
        if ready < 0:
            return ready

        if ready & READY_R:
            # This call may read notifies which will be saved in the
            # PGconn buffer and passed to Python later.
            if 1 != libpq.PQconsumeInput(pgconn_ptr):
                return -1

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
    cdef libpq.PGresult *pgres = NULL
    cdef int rv

    with nogil:
        rv = _fetch(pgconn_ptr, &pgres)

    if rv < 0:
        _raise_current_or_from_conn(pgconn)

    return pq.PGresult._from_ptr(pgres) if pgres is not NULL else None


cdef int _fetch(libpq.PGconn *pgconn_ptr, libpq.PGresult **res) nogil:
    cdef int fileno = libpq.PQsocket(pgconn_ptr)
    cdef libpq.PGresult *pgres

    while True:
        if not libpq.PQisBusy(pgconn_ptr):
            break

        if wait_ng(fileno, CWAIT_R) < 0:
            return -1

        if 1 != libpq.PQconsumeInput(pgconn_ptr):
            return -1

    res[0] = libpq.PQgetResult(pgconn_ptr)
    return 0


cdef libpq.PGresult *fetch_last(libpq.PGconn *pgconn_ptr) nogil:
    """
    Return the last result from a query.
    """
    cdef libpq.PGresult *pgres = NULL
    cdef libpq.PGresult *rv = NULL

    while True:
        if _fetch(pgconn_ptr, &pgres) < 0:
            if rv != NULL:
                libpq.PQclear(rv)
            return NULL

        if pgres == NULL:
            # All queries returned
            break

        if rv != NULL:
            libpq.PQclear(rv)
        rv = pgres
        pgres = NULL

        status = libpq.PQresultStatus(rv)
        if (
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            status == libpq.PGRES_COPY_IN
            or status == libpq.PGRES_COPY_OUT
            or status == libpq.PGRES_COPY_BOTH
            # PIPELINE_SYNC is not followed by a NULL, but we return it alone
            # similarly to other result sets.
            or status == libpq.PGRES_PIPELINE_SYNC
        ):
            break

    return rv



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
        if ready < 0:
            wait_dummy_raise()

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


cdef int _query_params_args(
    param_values: Optional[Sequence[Optional[bytes]]],
    param_types: Optional[Sequence[int]],
    param_formats: Optional[Sequence[int]],
    libpq.Oid **ctypes,
    char ***cvalues,
    int **clengths,
    int **cformats
) except -1:
    cdef Py_ssize_t nparams

    if param_values is None:
        nparams = 0
    else:
        if not isinstance(param_values, list):
            param_values = list(param_values)
        nparams = PyList_GET_SIZE(param_values)

    if nparams == 0:
        return nparams

    if param_types is not None:
        if not isinstance(param_types, tuple):
            param_types = tuple(param_types)
        if PyTuple_GET_SIZE(param_types) != nparams:
            raise ValueError(
                "got %d param_values but %d param_types"
                % (nparams, len(param_types))
            )

    if param_formats is not None:
        if not isinstance(param_formats, list):
            param_types = list(param_formats)

        if PyList_GET_SIZE(param_formats) != nparams:
            raise ValueError(
                "got %d param_values but %d param_formats"
                % (nparams, len(param_formats))
            )

    cvalues[0] = <char **>PyMem_Malloc(nparams * sizeof(char *))
    clengths[0] = <int *>PyMem_Malloc(nparams * sizeof(int))

    cdef int i
    cdef PyObject *obj
    cdef char *ptr
    cdef Py_ssize_t length

    for i in range(nparams):
        obj = PyList_GET_ITEM(param_values, i)
        if <object>obj is None:
            cvalues[0][i] = NULL
            clengths[0][i] = 0
        else:
            _buffer_as_string_and_size(<object>obj, &ptr, &length)
            cvalues[0][i] = ptr
            clengths[0][i] = <int>length

    if param_types is not None:
        ctypes[0] = <libpq.Oid *>PyMem_Malloc(nparams * sizeof(libpq.Oid))
        for i in range(nparams):
            ctypes[0][i] = <libpq.Oid><object>PyTuple_GET_ITEM(param_types, i)

    if param_formats is not None:
        cformats[0] = <int *>PyMem_Malloc(nparams * sizeof(int *))
        for i in range(nparams):
            cformats[0][i] = <int><object>PyList_GET_ITEM(param_formats, i)

    return nparams


cdef int _query_types_args(
    param_types: Optional[Sequence[int]], libpq.Oid **ctypes
) except -1:
    cdef Py_ssize_t nparams

    if param_types is None:
        nparams = 0
    else:
        if not isinstance(param_types, tuple):
            param_types = tuple(param_types)
        nparams = PyTuple_GET_SIZE(param_types)

    if nparams == 0:
        return nparams

    ctypes[0] = <libpq.Oid *>PyMem_Malloc(nparams * sizeof(libpq.Oid))
    for i in range(nparams):
        ctypes[0][i] = <libpq.Oid><object>PyTuple_GET_ITEM(param_types, i)

    return nparams


cdef void _clear_query_params(
    libpq.Oid *ctypes, char *const *cvalues, int *clenghst, int *cformats
):
    PyMem_Free(ctypes)
    PyMem_Free(<char **>cvalues)
    PyMem_Free(clenghst)
    PyMem_Free(cformats)
