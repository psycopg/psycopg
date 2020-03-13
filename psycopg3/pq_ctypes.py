"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from .pq_enums import ConnStatus, PostgresPollingStatus

from . import _pq_ctypes as impl


class PGconn:
    __slots__ = ("pgconn_ptr",)

    def __init__(self, pgconn_ptr):
        self.pgconn_ptr = pgconn_ptr

    @classmethod
    def connectdb(cls, conninfo):
        if isinstance(conninfo, str):
            conninfo = conninfo.encode("utf8")

        if not isinstance(conninfo, bytes):
            raise TypeError("bytes expected, got %r instead" % conninfo)

        pgconn_ptr = impl.PQconnectdb(conninfo)
        return cls(pgconn_ptr)

    @classmethod
    def connect_start(cls, conninfo):
        if isinstance(conninfo, str):
            conninfo = conninfo.encode("utf8")

        if not isinstance(conninfo, bytes):
            raise TypeError("bytes expected, got %r instead" % conninfo)

        pgconn_ptr = impl.PQconnectStart(conninfo)
        return cls(pgconn_ptr)

    def connect_poll(self):
        rv = impl.PQconnectPoll(self.pgconn_ptr)
        return PostgresPollingStatus(rv)

    @property
    def status(self):
        rv = impl.PQstatus(self.pgconn_ptr)
        return ConnStatus(rv)

    @property
    def error_message(self):
        # TODO: decode
        return impl.PQerrorMessage(self.pgconn_ptr)

    @property
    def socket(self):
        return impl.PQsocket(self.pgconn_ptr)
