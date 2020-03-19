"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from collections import namedtuple
from ctypes import string_at
from ctypes import c_char_p, c_int, pointer

from .enums import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
)
from .misc import error_message
from . import _pq_ctypes as impl
from ..exceptions import OperationalError


class PQerror(OperationalError):
    pass


class PGconn:
    __slots__ = ("pgconn_ptr",)

    def __init__(self, pgconn_ptr):
        self.pgconn_ptr = pgconn_ptr

    def __del__(self):
        self.finish()

    @classmethod
    def connect(cls, conninfo):
        if not isinstance(conninfo, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(conninfo).__name__
            )

        pgconn_ptr = impl.PQconnectdb(conninfo)
        if not pgconn_ptr:
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    @classmethod
    def connect_start(cls, conninfo):
        if not isinstance(conninfo, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(conninfo).__name__
            )

        pgconn_ptr = impl.PQconnectStart(conninfo)
        if not pgconn_ptr:
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    def connect_poll(self):
        rv = impl.PQconnectPoll(self.pgconn_ptr)
        return PollingStatus(rv)

    def finish(self):
        self.pgconn_ptr, p = None, self.pgconn_ptr
        if p is not None:
            impl.PQfinish(p)

    @property
    def info(self):
        opts = impl.PQconninfo(self.pgconn_ptr)
        if not opts:
            raise MemoryError("couldn't allocate connection info")
        try:
            return Conninfo._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    def reset(self):
        impl.PQreset(self.pgconn_ptr)

    def reset_start(self):
        rv = impl.PQresetStart(self.pgconn_ptr)
        if rv == 0:
            raise PQerror("couldn't reset connection")

    def reset_poll(self):
        rv = impl.PQresetPoll(self.pgconn_ptr)
        return PollingStatus(rv)

    @classmethod
    def ping(self, conninfo):
        if not isinstance(conninfo, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(conninfo).__name__
            )

        rv = impl.PQping(conninfo)
        return Ping(rv)

    @property
    def db(self):
        return impl.PQdb(self.pgconn_ptr)

    @property
    def user(self):
        return impl.PQuser(self.pgconn_ptr)

    @property
    def password(self):
        return impl.PQpass(self.pgconn_ptr)

    @property
    def host(self):
        return impl.PQhost(self.pgconn_ptr)

    @property
    def hostaddr(self):
        return impl.PQhostaddr(self.pgconn_ptr)

    @property
    def port(self):
        return impl.PQport(self.pgconn_ptr)

    @property
    def tty(self):
        return impl.PQtty(self.pgconn_ptr)

    @property
    def options(self):
        return impl.PQoptions(self.pgconn_ptr)

    @property
    def status(self):
        rv = impl.PQstatus(self.pgconn_ptr)
        return ConnStatus(rv)

    @property
    def transaction_status(self):
        rv = impl.PQtransactionStatus(self.pgconn_ptr)
        return TransactionStatus(rv)

    def parameter_status(self, name):
        return impl.PQparameterStatus(self.pgconn_ptr, name)

    @property
    def protocol_version(self):
        return impl.PQprotocolVersion(self.pgconn_ptr)

    @property
    def server_version(self):
        return impl.PQserverVersion(self.pgconn_ptr)

    @property
    def error_message(self):
        return impl.PQerrorMessage(self.pgconn_ptr)

    @property
    def socket(self):
        return impl.PQsocket(self.pgconn_ptr)

    @property
    def backend_pid(self):
        return impl.PQbackendPID(self.pgconn_ptr)

    @property
    def needs_password(self):
        return bool(impl.PQconnectionNeedsPassword(self.pgconn_ptr))

    @property
    def used_password(self):
        return bool(impl.PQconnectionUsedPassword(self.pgconn_ptr))

    @property
    def ssl_in_use(self):
        return bool(impl.PQsslInUse(self.pgconn_ptr))

    def exec_(self, command):
        if not isinstance(command, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(command).__name__
            )
        rv = impl.PQexec(self.pgconn_ptr, command)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query(self, command):
        if not isinstance(command, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(command).__name__
            )
        return impl.PQsendQuery(self.pgconn_ptr, command)

    def exec_params(
        self,
        command,
        param_values,
        param_types=None,
        param_formats=None,
        result_format=0,
    ):
        if not isinstance(command, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(command).__name__
            )

        nparams = len(param_values)
        if nparams:
            aparams = (c_char_p * nparams)(*param_values)
            alenghts = (c_int * nparams)(
                *(len(p) if p is not None else 0 for p in param_values)
            )
        else:
            aparams = alenghts = None

        if param_types is None:
            atypes = None
        else:
            if len(param_types) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_types))
                )
            atypes = (impl.Oid * nparams)(*param_types)

        if param_formats is None:
            aformats = None
        else:
            if len(param_formats) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_formats))
                )
            aformats = (c_int * nparams)(*param_formats)

        rv = impl.PQexecParams(
            self.pgconn_ptr,
            command,
            nparams,
            atypes,
            aparams,
            alenghts,
            aformats,
            result_format,
        )
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def prepare(self, name, command, param_types=None):
        if not isinstance(name, bytes):
            raise TypeError(
                "'name' must be bytes, got %s instead" % type(name).__name__
            )

        if not isinstance(command, bytes):
            raise TypeError(
                "'command' must be bytes, got %s instead"
                % type(command).__name__
            )

        if param_types is None:
            nparams = 0
            atypes = None
        else:
            nparams = len(param_types)
            atypes = (impl.Oid * nparams)(*param_types)

        rv = impl.PQprepare(self.pgconn_ptr, name, command, nparams, atypes)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def exec_prepared(
        self, name, param_values, param_formats=None, result_format=0
    ):
        if not isinstance(name, bytes):
            raise TypeError(
                "'name' must be bytes, got %s instead" % type(name).__name__
            )

        nparams = len(param_values)
        if nparams:
            aparams = (c_char_p * nparams)(*param_values)
            alenghts = (c_int * nparams)(
                *(len(p) if p is not None else 0 for p in param_values)
            )
        else:
            aparams = alenghts = None

        if param_formats is None:
            aformats = None
        else:
            if len(param_formats) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_formats))
                )
            aformats = (c_int * nparams)(*param_formats)

        rv = impl.PQexecPrepared(
            self.pgconn_ptr,
            name,
            nparams,
            aparams,
            alenghts,
            aformats,
            result_format,
        )
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def describe_prepared(self, name):
        if not isinstance(name, bytes):
            raise TypeError(
                "'name' must be bytes, got %s instead" % type(name).__name__
            )
        rv = impl.PQdescribePrepared(self.pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def describe_portal(self, name):
        if not isinstance(name, bytes):
            raise TypeError(
                "'name' must be bytes, got %s instead" % type(name).__name__
            )
        rv = impl.PQdescribePortal(self.pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def get_result(self):
        rv = impl.PQgetResult(self.pgconn_ptr)
        return PGresult(rv) if rv else None

    def consume_input(self):
        if 1 != impl.PQconsumeInput(self.pgconn_ptr):
            raise PQerror(f"consuming input failed: {error_message(self)}")

    def is_busy(self):
        return impl.PQisBusy(self.pgconn_ptr)

    @property
    def nonblocking(self):
        return impl.PQisnonblocking(self.pgconn_ptr)

    @nonblocking.setter
    def nonblocking(self, arg):
        if 0 > impl.PQsetnonblocking(self.pgconn_ptr, arg):
            raise PQerror(f"setting nonblocking failed: {error_message(self)}")

    def flush(self):
        rv = impl.PQflush(self.pgconn_ptr)
        if rv < 0:
            raise PQerror(f"flushing failed: {error_message(self)}")
        return rv

    def make_empty_result(self, exec_status):
        rv = impl.PQmakeEmptyPGresult(self.pgconn_ptr, exec_status)
        if not rv:
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult(rv)


class PGresult:
    __slots__ = ("pgresult_ptr",)

    def __init__(self, pgresult_ptr):
        self.pgresult_ptr = pgresult_ptr

    def __del__(self):
        self.clear()

    def clear(self):
        self.pgresult_ptr, p = None, self.pgresult_ptr
        if p is not None:
            impl.PQclear(p)

    @property
    def status(self):
        rv = impl.PQresultStatus(self.pgresult_ptr)
        return ExecStatus(rv)

    @property
    def error_message(self):
        return impl.PQresultErrorMessage(self.pgresult_ptr)

    def error_field(self, fieldcode):
        return impl.PQresultErrorField(self.pgresult_ptr, fieldcode)

    @property
    def ntuples(self):
        return impl.PQntuples(self.pgresult_ptr)

    @property
    def nfields(self):
        return impl.PQnfields(self.pgresult_ptr)

    def fname(self, column_number):
        return impl.PQfname(self.pgresult_ptr, column_number)

    def ftable(self, column_number):
        return impl.PQftable(self.pgresult_ptr, column_number)

    def ftablecol(self, column_number):
        return impl.PQftablecol(self.pgresult_ptr, column_number)

    def fformat(self, column_number):
        return impl.PQfformat(self.pgresult_ptr, column_number)

    def ftype(self, column_number):
        return impl.PQftype(self.pgresult_ptr, column_number)

    def fmod(self, column_number):
        return impl.PQfmod(self.pgresult_ptr, column_number)

    def fsize(self, column_number):
        return impl.PQfsize(self.pgresult_ptr, column_number)

    @property
    def binary_tuples(self):
        return impl.PQbinaryTuples(self.pgresult_ptr)

    def get_value(self, row_number, column_number):
        length = impl.PQgetlength(self.pgresult_ptr, row_number, column_number)
        if length:
            v = impl.PQgetvalue(self.pgresult_ptr, row_number, column_number)
            return string_at(v, length)
        else:
            if impl.PQgetisnull(self.pgresult_ptr, row_number, column_number):
                return None
            else:
                return b""

    @property
    def nparams(self):
        return impl.PQnparams(self.pgresult_ptr)

    def param_type(self, param_number):
        return impl.PQparamtype(self.pgresult_ptr, param_number)


ConninfoOption = namedtuple(
    "ConninfoOption", "keyword envvar compiled val label dispatcher dispsize"
)


class Conninfo:
    @classmethod
    def get_defaults(cls):
        opts = impl.PQconndefaults()
        if not opts:
            raise MemoryError("couldn't allocate connection defaults")
        try:
            return cls._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    @classmethod
    def parse(cls, conninfo):
        if not isinstance(conninfo, bytes):
            raise TypeError(
                "bytes expected, got %s instead" % type(conninfo).__name__
            )

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
            return cls._options_from_array(rv)
        finally:
            impl.PQconninfoFree(rv)

    @classmethod
    def _options_from_array(cls, opts):
        rv = []
        skws = "keyword envvar compiled val label dispatcher".split()
        for opt in opts:
            if not opt.keyword:
                break
            d = {kw: getattr(opt, kw) for kw in skws}
            d["dispsize"] = opt.dispsize
            rv.append(ConninfoOption(**d))

        return rv
