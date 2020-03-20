"""
psycopg3 connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import logging
from threading import Lock

from . import pq
from . import exceptions as exc
from .conninfo import make_conninfo
from .waiting import wait_select, wait_async, Wait, Ready

logger = logging.getLogger(__name__)


class BaseConnection:
    """
    Base class for different types of connections.

    Share common functionalities such as access to the wrapped PGconn, but
    allow different interfaces (sync/async).
    """

    def __init__(self, pgconn):
        self.pgconn = pgconn
        self.lock = Lock()

    @classmethod
    def _connect_gen(cls, conninfo):
        """
        Generator to create a database connection using without blocking.

        Yield pairs (fileno, `Wait`) whenever an operation would block. The
        generator can be restarted sending the appropriate `Ready` state when
        the file descriptor is ready.
        """
        conninfo = conninfo.encode("utf8")

        conn = pq.PGconn.connect_start(conninfo)
        logger.debug("connection started, status %s", conn.status.name)
        while 1:
            if conn.status == pq.ConnStatus.CONNECTION_BAD:
                raise exc.OperationalError(
                    f"connection is bad: {pq.error_message(conn)}"
                )

            status = conn.connect_poll()
            logger.debug("connection polled, status %s", conn.status.name)
            if status == pq.PollingStatus.PGRES_POLLING_OK:
                break
            elif status == pq.PollingStatus.PGRES_POLLING_READING:
                yield conn.socket, Wait.R
            elif status == pq.PollingStatus.PGRES_POLLING_WRITING:
                yield conn.socket, Wait.W
            elif status == pq.PollingStatus.PGRES_POLLING_FAILED:
                raise exc.OperationalError(
                    f"connection failed: {pq.error_message(conn)}"
                )
            else:
                raise exc.InternalError(f"unexpected poll status: {status}")

        conn.nonblocking = 1
        return conn

    @classmethod
    def _exec_gen(cls, pgconn):
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
        results = []

        while 1:
            f = pgconn.flush()
            if f == 0:
                break

            ready = yield pgconn.socket, Wait.RW
            if ready is Ready.R:
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

        return results


class Connection(BaseConnection):
    """
    Wrap a connection to the database.

    This class implements a DBAPI-compliant interface.
    """

    @classmethod
    def connect(cls, conninfo, connection_factory=None, **kwargs):
        if connection_factory is not None:
            raise NotImplementedError()
        conninfo = make_conninfo(conninfo, **kwargs)
        gen = cls._connect_gen(conninfo)
        pgconn = wait_select(gen)
        return cls(pgconn)

    def commit(self):
        self._exec_commit_rollback(b"commit")

    def rollback(self):
        self._exec_commit_rollback(b"rollback")

    def _exec_commit_rollback(self, command):
        with self.lock:
            status = self.pgconn.transaction_status
            if status == pq.TransactionStatus.PQTRANS_IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = wait_select(self._exec_gen(self.pgconn))
            if pgres.status != pq.ExecStatus.PGRES_COMMAND_OK:
                raise exc.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )


class AsyncConnection(BaseConnection):
    """
    Wrap an asynchronous connection to the database.

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines.
    """

    @classmethod
    async def connect(cls, conninfo, **kwargs):
        conninfo = make_conninfo(conninfo, **kwargs)
        gen = cls._connect_gen(conninfo)
        pgconn = await wait_async(gen)
        return cls(pgconn)

    async def commit(self):
        await self._exec_commit_rollback(b"commit")

    async def rollback(self):
        await self._exec_commit_rollback(b"rollback")

    async def _exec_commit_rollback(self, command):
        with self.lock:
            status = self.pgconn.transaction_status
            if status == pq.TransactionStatus.PQTRANS_IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = await wait_async(self._exec_gen(self.pgconn))
            if pgres.status != pq.ExecStatus.PGRES_COMMAND_OK:
                raise exc.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )
