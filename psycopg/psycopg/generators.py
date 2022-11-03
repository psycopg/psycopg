"""
Generators implementing communication protocols with the libpq

Certain operations (connection, querying) are an interleave of libpq calls and
waiting for the socket to be ready. This module contains the code to execute
the operations, yielding a polling state whenever there is to wait. The
functions in the `waiting` module are the ones who wait more or less
cooperatively for the socket to be ready and make these generators continue.

All these generators yield pairs (fileno, `Wait`) whenever an operation would
block. The generator can be restarted sending the appropriate `Ready` state
when the file descriptor is ready.

"""

# Copyright (C) 2020 The Psycopg Team

import logging
from typing import List, Optional, Union

from . import pq
from . import errors as e
from .abc import Buffer, PipelineCommand, PQGen, PQGenConn
from .pq.abc import PGconn, PGresult
from .waiting import Wait, Ready
from ._compat import Deque
from ._cmodule import _psycopg
from ._queries import PostgresQuery
from ._encodings import pgconn_encoding, conninfo_encoding

TEXT = pq.Format.TEXT
BINARY = pq.Format.BINARY

OK = pq.ConnStatus.OK
BAD = pq.ConnStatus.BAD

POLL_OK = pq.PollingStatus.OK
POLL_READING = pq.PollingStatus.READING
POLL_WRITING = pq.PollingStatus.WRITING
POLL_FAILED = pq.PollingStatus.FAILED

COMMAND_OK = pq.ExecStatus.COMMAND_OK
FATAL_ERROR = pq.ExecStatus.FATAL_ERROR
COPY_OUT = pq.ExecStatus.COPY_OUT
COPY_IN = pq.ExecStatus.COPY_IN
COPY_BOTH = pq.ExecStatus.COPY_BOTH
PIPELINE_SYNC = pq.ExecStatus.PIPELINE_SYNC

WAIT_R = Wait.R
WAIT_W = Wait.W
WAIT_RW = Wait.RW
READY_R = Ready.R
READY_W = Ready.W
READY_RW = Ready.RW

logger = logging.getLogger(__name__)


def _connect(conninfo: str) -> PQGenConn[PGconn]:
    """
    Generator to create a database connection without blocking.

    """
    conn = pq.PGconn.connect_start(conninfo.encode())
    while True:
        if conn.status == BAD:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection is bad: {pq.error_message(conn, encoding=encoding)}",
                pgconn=conn,
            )

        status = conn.connect_poll()
        if status == POLL_OK:
            break
        elif status == POLL_READING:
            yield conn.socket, WAIT_R
        elif status == POLL_WRITING:
            yield conn.socket, WAIT_W
        elif status == POLL_FAILED:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection failed: {pq.error_message(conn, encoding=encoding)}",
                pgconn=conn,
            )
        else:
            raise e.InternalError(f"unexpected poll status: {status}", pgconn=conn)

    conn.nonblocking = 1
    return conn


def _execute_command(
    pgconn: PGconn, command: bytes, *, result_format: pq.Format = TEXT
) -> PQGen[PGresult]:
    """
    Execute a command as a string and fetch the results back from the server.

    Always send the command using the extended protocol, even if it has no
    parameter.
    """
    pgconn.send_query_params(command, None, result_format=result_format)
    yield from flush(pgconn)
    results = yield from fetch_many(pgconn)
    return results[0]


def _execute_query(
    pgconn: PGconn,
    query: PostgresQuery,
    *,
    result_format: pq.Format = TEXT,
    force_extended: bool = False,
) -> PQGen[List[PGresult]]:
    """
    Execute a query and fetch the results back from the server.
    """
    if force_extended or query.params or result_format == BINARY:
        pgconn.send_query_params(
            query.query,
            query.params,
            param_formats=query.formats,
            param_types=query.types,
            result_format=result_format,
        )
    else:
        # If we can, let's use simple query protocol,
        # as it can execute more than one statement in a single query.
        pgconn.send_query(query.query)

    return (yield from _flush_and_fetch(pgconn))


def _prepare_query(pgconn: PGconn, name: bytes, query: PostgresQuery) -> PQGen[None]:
    """
    Prepare a query for prepared statement execution.
    """
    pgconn.send_prepare(name, query.query, param_types=query.types)
    (result,) = yield from _flush_and_fetch(pgconn)
    if result.status == FATAL_ERROR:
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)


def _execute_prepared_query(
    pgconn: PGconn,
    name: bytes,
    query: PostgresQuery,
    *,
    result_format: pq.Format = TEXT,
) -> PQGen[List[PGresult]]:
    """
    Execute a prepared statement with given parameters and fetch the results.
    """
    pgconn.send_query_prepared(
        name, query.params, param_formats=query.formats, result_format=result_format
    )
    return (yield from _flush_and_fetch(pgconn))


def describe_portal(pgconn: PGconn, name: bytes) -> PQGen[List[PGresult]]:
    """
    Describe a portal fetch the result from the server.
    """
    pgconn.send_describe_portal(name)
    return (yield from _flush_and_fetch(pgconn))


def send_single_row(
    pgconn: PGconn, query: PostgresQuery, *, result_format: pq.Format = TEXT
) -> PQGen[None]:
    """
    Send a query to the server for consumption in single-row mode.
    """
    pgconn.send_query_params(
        query.query,
        query.params,
        param_formats=query.formats,
        param_types=query.types,
        result_format=result_format,
    )
    pgconn.set_single_row_mode()
    yield from _flush(pgconn)


def _flush_and_fetch(pgconn: PGconn) -> PQGen[List[PGresult]]:
    """
    Generator sending a query and returning results without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    Return the list of results returned by the database (whether success
    or error).
    """
    yield from flush(pgconn)
    rv = yield from fetch_many(pgconn)
    return rv


def _flush(pgconn: PGconn) -> PQGen[None]:
    """
    Generator to send a query to the server without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    After this generator has finished you may want to cycle using `fetch()`
    to retrieve the results available.
    """
    while True:
        if pgconn.flush() == 0:
            break

        ready = yield WAIT_RW
        if ready & READY_R:
            # This call may read notifies: they will be saved in the
            # PGconn buffer and passed to Python later, in `fetch()`.
            pgconn.consume_input()


def _fetch_many(pgconn: PGconn) -> PQGen[List[PGresult]]:
    """
    Generator retrieving results from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return the list of results returned by the database (whether success
    or error).
    """
    results: List[PGresult] = []
    while True:
        res = yield from _fetch(pgconn)
        if not res:
            break

        results.append(res)
        status = res.status
        if status == COPY_IN or status == COPY_OUT or status == COPY_BOTH:
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

        if status == PIPELINE_SYNC:
            # PIPELINE_SYNC is not followed by a NULL, but we return it alone
            # similarly to other result sets.
            assert len(results) == 1, results
            break

    _consume_notifies(pgconn)

    return results


def _fetch(pgconn: PGconn) -> PQGen[Optional[PGresult]]:
    """
    Generator retrieving a single result from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return a result from the database (whether success or error).
    """
    while True:
        if not pgconn.is_busy():
            break
        yield WAIT_R
        pgconn.consume_input()

    return pgconn.get_result()


def _pipeline_communicate(
    pgconn: PGconn, commands: Deque[PipelineCommand]
) -> PQGen[List[List[PGresult]]]:
    """Generator to send queries from a connection in pipeline mode while also
    receiving results.

    Return a list results, including single PIPELINE_SYNC elements.
    """
    results = []

    while True:
        ready = yield WAIT_RW

        if ready & READY_R:
            pgconn.consume_input()
            _consume_notifies(pgconn)

            res: List[PGresult] = []
            while not pgconn.is_busy():
                r = pgconn.get_result()
                if r is None:
                    if not res:
                        break
                    results.append(res)
                    res = []
                elif r.status == PIPELINE_SYNC:
                    assert not res
                    results.append([r])
                else:
                    res.append(r)

        if ready & READY_W:
            pgconn.flush()
            if not commands:
                break
            commands.popleft()()

    return results


def _consume_notifies(pgconn: PGconn) -> None:
    # Consume notifies
    while True:
        n = pgconn.notifies()
        if not n:
            break
        if pgconn.notify_handler:
            pgconn.notify_handler(n)


def notifies(pgconn: PGconn) -> PQGen[List[pq.PGnotify]]:
    yield WAIT_R
    pgconn.consume_input()

    ns = []
    while True:
        n = pgconn.notifies()
        if n:
            ns.append(n)
        else:
            break

    return ns


def notifies_ng(pgconn: PGconn) -> List[pq.PGnotify]:
    wait_ng_c(pgconn.socket, WAIT_R)
    pgconn.consume_input()

    ns = []
    while True:
        n = pgconn.notifies()
        if n:
            ns.append(n)
        else:
            break

    return ns


def copy_from(pgconn: PGconn) -> PQGen[Union[memoryview, PGresult]]:
    while True:
        nbytes, data = pgconn.get_copy_data(1)
        if nbytes != 0:
            break

        # would block
        yield WAIT_R
        pgconn.consume_input()

    if nbytes > 0:
        # some data
        return data

    # Retrieve the final result of copy
    results = yield from _fetch_many(pgconn)
    if len(results) > 1:
        # TODO: too brutal? Copy worked.
        raise e.ProgrammingError("you cannot mix COPY with other operations")
    result = results[0]
    if result.status != COMMAND_OK:
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)

    return result


def copy_to(pgconn: PGconn, buffer: Buffer) -> PQGen[None]:
    # Retry enqueuing data until successful.
    #
    # WARNING! This can cause an infinite loop if the buffer is too large. (see
    # ticket #255). We avoid it in the Copy object by splitting a large buffer
    # into smaller ones. We prefer to do it there instead of here in order to
    # do it upstream the queue decoupling the writer task from the producer one.
    while pgconn.put_copy_data(buffer) == 0:
        yield WAIT_W


def copy_end(pgconn: PGconn, error: Optional[bytes]) -> PQGen[PGresult]:
    # Retry enqueuing end copy message until successful
    while pgconn.put_copy_end(error) == 0:
        yield WAIT_W

    # Repeat until it the message is flushed to the server
    while True:
        yield WAIT_W
        if pgconn.flush() == 0:
            break

    # Retrieve the final result of copy
    (result,) = yield from _fetch_many(pgconn)
    if result.status != COMMAND_OK:
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)

    return result


# Override functions with fast versions if available
if _psycopg:
    connect = _psycopg.connect
    execute_command = _psycopg.execute_command
    execute_query = _psycopg.execute_query
    prepare_query = _psycopg.prepare_query
    execute_prepared_query = _psycopg.execute_prepared_query
    flush_and_fetch = _psycopg.flush_and_fetch
    flush = _psycopg.flush
    fetch_many = _psycopg.fetch_many
    fetch = _psycopg.fetch
    pipeline_communicate = _psycopg.pipeline_communicate

else:
    connect = _connect
    execute_command = _execute_command
    execute_query = _execute_query
    prepare_query = _prepare_query
    execute_prepared_query = _execute_prepared_query
    flush_and_fetch = _flush_and_fetch
    flush = _flush
    fetch_many = _fetch_many
    fetch = _fetch
    pipeline_communicate = _pipeline_communicate

assert _psycopg
wait_ng_c = _psycopg.wait_ng_c
connect_ng = _psycopg.connect_ng
execute_ng = _psycopg.execute_ng
send_ng = _psycopg.send_ng
fetch_many_ng = _psycopg.fetch_many_ng
fetch_ng = _psycopg.fetch_ng
pipeline_communicate_ng = _psycopg.pipeline_communicate_ng
