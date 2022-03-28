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
from .pq import ConnStatus, PollingStatus, ExecStatus
from .abc import Command, PQGen, PQGenConn
from .pq.abc import PGconn, PGresult
from .waiting import Wait, Ready
from ._compat import Deque
from ._encodings import pgconn_encoding, conninfo_encoding

logger = logging.getLogger(__name__)


def connect(conninfo: str) -> PQGenConn[PGconn]:
    """
    Generator to create a database connection without blocking.

    """
    conn = pq.PGconn.connect_start(conninfo.encode())
    while 1:
        if conn.status == ConnStatus.BAD:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection is bad: {pq.error_message(conn, encoding=encoding)}",
                pgconn=conn,
            )

        status = conn.connect_poll()
        if status == PollingStatus.OK:
            break
        elif status == PollingStatus.READING:
            yield conn.socket, Wait.R
        elif status == PollingStatus.WRITING:
            yield conn.socket, Wait.W
        elif status == PollingStatus.FAILED:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection failed: {pq.error_message(conn, encoding=encoding)}",
                pgconn=conn,
            )
        else:
            raise e.InternalError(f"unexpected poll status: {status}", pgconn=conn)

    conn.nonblocking = 1
    return conn


def execute(pgconn: PGconn) -> PQGen[List[PGresult]]:
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


def send(pgconn: PGconn) -> PQGen[None]:
    """
    Generator to send a query to the server without blocking.

    The query must have already been sent using `pgconn.send_query()` or
    similar. Flush the query and then return the result using nonblocking
    functions.

    After this generator has finished you may want to cycle using `fetch()`
    to retrieve the results available.
    """
    while 1:
        f = pgconn.flush()
        if f == 0:
            break

        ready = yield Wait.RW
        if ready & Ready.R:
            # This call may read notifies: they will be saved in the
            # PGconn buffer and passed to Python later, in `fetch()`.
            pgconn.consume_input()


def fetch_many(pgconn: PGconn) -> PQGen[List[PGresult]]:
    """
    Generator retrieving results from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return the list of results returned by the database (whether success
    or error).
    """
    results: List[PGresult] = []
    while 1:
        res = yield from fetch(pgconn)
        if not res:
            break

        results.append(res)
        if res.status in _copy_statuses:
            # After entering copy mode the libpq will create a phony result
            # for every request so let's break the endless loop.
            break

        if res.status == pq.ExecStatus.PIPELINE_SYNC:
            # PIPELINE_SYNC is not followed by a NULL, but we return it alone
            # similarly to other result sets.
            assert len(results) == 1, results
            break

    return results


def fetch(pgconn: PGconn) -> PQGen[Optional[PGresult]]:
    """
    Generator retrieving a single result from the database without blocking.

    The query must have already been sent to the server, so pgconn.flush() has
    already returned 0.

    Return a result from the database (whether success or error).
    """
    while 1:
        pgconn.consume_input()
        if not pgconn.is_busy():
            break
        yield Wait.R

    # Consume notifies
    while 1:
        n = pgconn.notifies()
        if not n:
            break
        if pgconn.notify_handler:
            pgconn.notify_handler(n)

    return pgconn.get_result()


def pipeline_communicate(
    pgconn: PGconn, commands: Deque[Command]
) -> PQGen[List[List[PGresult]]]:
    """Generator to send queries from a connection in pipeline mode while also
    receiving results.

    Return a list results, including single PIPELINE_SYNC elements.
    """
    results = []

    while True:
        ready = yield Wait.RW

        if ready & Ready.R:
            pgconn.consume_input()
            while True:
                n = pgconn.notifies()
                if not n:
                    break
                if pgconn.notify_handler:
                    pgconn.notify_handler(n)

            res: List[PGresult] = []
            while not pgconn.is_busy():
                r = pgconn.get_result()
                if r is None:
                    if not res:
                        break
                    results.append(res)
                    res = []
                elif r.status == pq.ExecStatus.PIPELINE_SYNC:
                    assert not res
                    results.append([r])
                else:
                    res.append(r)

        if ready & Ready.W:
            pgconn.flush()
            if not commands:
                break
            commands.popleft()()

    return results


_copy_statuses = (
    ExecStatus.COPY_IN,
    ExecStatus.COPY_OUT,
    ExecStatus.COPY_BOTH,
)


def notifies(pgconn: PGconn) -> PQGen[List[pq.PGnotify]]:
    yield Wait.R
    pgconn.consume_input()

    ns = []
    while 1:
        n = pgconn.notifies()
        if n:
            ns.append(n)
        else:
            break

    return ns


def copy_from(pgconn: PGconn) -> PQGen[Union[memoryview, PGresult]]:
    while 1:
        nbytes, data = pgconn.get_copy_data(1)
        if nbytes != 0:
            break

        # would block
        yield Wait.R
        pgconn.consume_input()

    if nbytes > 0:
        # some data
        return data

    # Retrieve the final result of copy
    results = yield from fetch_many(pgconn)
    if len(results) > 1:
        # TODO: too brutal? Copy worked.
        raise e.ProgrammingError("you cannot mix COPY with other operations")
    result = results[0]
    if result.status != ExecStatus.COMMAND_OK:
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)

    return result


def copy_to(pgconn: PGconn, buffer: bytes) -> PQGen[None]:
    # Retry enqueuing data until successful.
    #
    # WARNING! This can cause an infinite loop if the buffer is too large. (see
    # ticket #255). We avoid it in the Copy object by splitting a large buffer
    # into smaller ones. We prefer to do it there instead of here in order to
    # do it upstream the queue decoupling the writer task from the producer one.
    while pgconn.put_copy_data(buffer) == 0:
        yield Wait.W


def copy_end(pgconn: PGconn, error: Optional[bytes]) -> PQGen[PGresult]:
    # Retry enqueuing end copy message until successful
    while pgconn.put_copy_end(error) == 0:
        yield Wait.W

    # Repeat until it the message is flushed to the server
    while 1:
        yield Wait.W
        f = pgconn.flush()
        if f == 0:
            break

    # Retrieve the final result of copy
    (result,) = yield from fetch_many(pgconn)
    if result.status != ExecStatus.COMMAND_OK:
        encoding = pgconn_encoding(pgconn)
        raise e.error_from_result(result, encoding=encoding)

    return result
