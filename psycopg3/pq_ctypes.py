"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from collections import namedtuple

from .pq_enums import ConnStatus, PostgresPollingStatus
from . import _pq_ctypes as impl


class PGconn:
    __slots__ = ("pgconn_ptr",)

    def __init__(self, pgconn_ptr):
        self.pgconn_ptr = pgconn_ptr

    def __del__(self):
        self.pgconn_ptr, p = None, self.pgconn_ptr
        if p is not None:
            impl.PQfinish(p)

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

    @classmethod
    def get_defaults(cls):
        opts = impl.PQconndefaults()
        if not opts:
            raise MemoryError("couldn't allocate connection defaults")
        try:
            return _conninfoopts_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    @property
    def info(self):
        opts = impl.PQconninfo(self.pgconn_ptr)
        if not opts:
            raise MemoryError("couldn't allocate connection info")
        try:
            return _conninfoopts_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

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


ConninfoOption = namedtuple(
    "ConninfoOption", "keyword envvar compiled val label dispatcher dispsize"
)


def _conninfoopts_from_array(opts):
    def gets(opt, kw):
        rv = getattr(opt, kw)
        if rv is not None:
            rv = rv.decode("utf8", "replace")
        return rv

    rv = []
    skws = "keyword envvar compiled val label dispatcher".split()
    for opt in opts:
        if not opt.keyword:
            break
        d = {kw: gets(opt, kw) for kw in skws}
        d["dispsize"] = opt.dispsize
        rv.append(ConninfoOption(**d))

    return rv
