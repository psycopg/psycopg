"""
psycopg3 connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import logging

from . import pq
from . import exceptions as exc
from .conninfo import make_conninfo
from .waiting import wait_select, wait_async, WAIT_R, WAIT_W

logger = logging.getLogger(__name__)


class BaseConnection:
    """
    Base class for different types of connections.

    Share common functionalities such as access to the wrapped PGconn, but
    allow different interfaces (sync/async).
    """

    def __init__(self, pgconn):
        self.pgconn = pgconn

    @classmethod
    def _connect_gen(cls, conninfo):
        """
        Generator yielding connection states and returning a done connection.
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
                yield conn.socket, WAIT_R
            elif status == pq.PollingStatus.PGRES_POLLING_WRITING:
                yield conn.socket, WAIT_W
            elif status == pq.PollingStatus.PGRES_POLLING_FAILED:
                raise exc.OperationalError(
                    f"connection failed: {pq.error_message(conn)}"
                )
            else:
                raise exc.InternalError(f"unexpected poll status: {status}")

        conn.nonblocking = 1
        return conn


class Connection(BaseConnection):
    """
    Wrap a connection to the database.

    This class implements a DBAPI-compliant interface.
    """

    @classmethod
    def connect(cls, conninfo, **kwargs):
        conninfo = make_conninfo(conninfo, **kwargs)
        gen = cls._connect_gen(conninfo)
        pgconn = wait_select(gen)
        return cls(pgconn)


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
