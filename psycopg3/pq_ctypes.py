"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from collections import namedtuple
from ctypes import c_char_p, pointer

from .pq_enums import (
    ConnStatus,
    PostgresPollingStatus,
    TransactionStatus,
    PGPing,
)
from . import _pq_ctypes as impl


class PQerror(Exception):
    pass


class PGconn:
    __slots__ = ("pgconn_ptr",)

    def __init__(self, pgconn_ptr):
        self.pgconn_ptr = pgconn_ptr

    def __del__(self):
        self.finish()

    @classmethod
    def connect(cls, conninfo):
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

    def finish(self):
        self.pgconn_ptr, p = None, self.pgconn_ptr
        if p is not None:
            impl.PQfinish(p)

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

    @classmethod
    def parse_conninfo(cls, conninfo):
        if isinstance(conninfo, str):
            conninfo = conninfo.encode("utf8")
        if not isinstance(conninfo, bytes):
            raise TypeError("bytes expected, got %r instead" % conninfo)

        errmsg = c_char_p()
        rv = impl.PQconninfoParse(conninfo, pointer(errmsg))
        if not rv:
            if not errmsg:
                raise MemoryError("couldn't allocate on conninfo parse")
            else:
                exc = PQerror(errmsg.value.decode("utf8", "replace"))
                impl.PQfreemem(errmsg)
                raise exc

        try:
            return _conninfoopts_from_array(rv)
        finally:
            impl.PQconninfoFree(rv)

    def reset(self):
        impl.PQreset(self.pgconn_ptr)

    def reset_start(self):
        rv = impl.PQresetStart(self.pgconn_ptr)
        if rv == 0:
            raise PQerror("couldn't reset connection")

    def reset_poll(self):
        rv = impl.PQresetPoll(self.pgconn_ptr)
        return PostgresPollingStatus(rv)

    @classmethod
    def ping(self, conninfo):
        if isinstance(conninfo, str):
            conninfo = conninfo.encode("utf8")
        if not isinstance(conninfo, bytes):
            raise TypeError("bytes expected, got %r instead" % conninfo)

        rv = impl.PQping(conninfo)
        return PGPing(rv)

    @property
    def db(self):
        return self._decode(impl.PQdb(self.pgconn_ptr))

    @property
    def user(self):
        return self._decode(impl.PQuser(self.pgconn_ptr))

    @property
    def password(self):
        return self._decode(impl.PQpass(self.pgconn_ptr))

    @property
    def host(self):
        return self._decode(impl.PQhost(self.pgconn_ptr))

    @property
    def hostaddr(self):
        return self._decode(impl.PQhostaddr(self.pgconn_ptr))

    @property
    def port(self):
        return self._decode(impl.PQport(self.pgconn_ptr))

    @property
    def tty(self):
        return self._decode(impl.PQtty(self.pgconn_ptr))

    @property
    def options(self):
        return self._decode(impl.PQoptions(self.pgconn_ptr))

    @property
    def status(self):
        rv = impl.PQstatus(self.pgconn_ptr)
        return ConnStatus(rv)

    @property
    def transaction_status(self):
        rv = impl.PQtransactionStatus(self.pgconn_ptr)
        return TransactionStatus(rv)

    @property
    def error_message(self):
        return self._decode(impl.PQerrorMessage(self.pgconn_ptr))

    @property
    def socket(self):
        return impl.PQsocket(self.pgconn_ptr)

    def _encode(self, s):
        # TODO: encode in client encoding
        return s.encode("utf8")

    def _decode(self, b):
        if b is None:
            return None
        # TODO: decode in client encoding
        return b.decode("utf8", "replace")


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
