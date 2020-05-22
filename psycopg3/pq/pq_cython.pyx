"""
libpq Python wrapper using cython bindings.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.mem cimport PyMem_Malloc, PyMem_Free

import logging
from typing import List, Optional, Sequence

from psycopg3.pq cimport libpq as impl
from psycopg3.pq.libpq cimport Oid
from psycopg3.errors import OperationalError

from psycopg3.pq.misc import error_message, ConninfoOption, PQerror
from psycopg3.pq.enums import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)


__impl__ = 'c'

logger = logging.getLogger('psycopg3')


def version():
    return impl.PQlibVersion()


cdef void notice_receiver(void *arg, const impl.PGresult *res_ptr):
    cdef PGconn pgconn = <object>arg
    if pgconn.notice_handler is None:
        return

    cdef PGresult res = PGresult._from_ptr(<impl.PGresult *>res_ptr)
    try:
        pgconn.notice_handler(res)
    except Exception as e:
        logger.exception("error in notice receiver: %s", e)

    res.pgresult_ptr = NULL  # avoid destroying the pgresult_ptr


cdef class PGconn:
    @staticmethod
    cdef PGconn _from_ptr(impl.PGconn *ptr):
        cdef PGconn rv = PGconn.__new__(PGconn)
        rv.pgconn_ptr = ptr

        impl.PQsetNoticeReceiver(ptr, notice_receiver, <void *>rv)
        return rv

    def __cinit__(self):
        self.pgconn_ptr = NULL

    def __dealloc__(self):
        self.finish()

    @classmethod
    def connect(cls, conninfo: bytes) -> PGconn:
        return _connect(conninfo)

    @classmethod
    def connect_start(cls, conninfo: bytes) -> PGconn:
        return _connect_start(conninfo)

    def connect_poll(self) -> PollingStatus:
        cdef int rv = self._call_int(<conn_int_f>impl.PQconnectPoll)
        return PollingStatus(rv)

    def finish(self) -> None:
        if self.pgconn_ptr is not NULL:
            impl.PQfinish(self.pgconn_ptr)
            self.pgconn_ptr = NULL

    @property
    def info(self) -> List["ConninfoOption"]:
        self._ensure_pgconn()
        cdef impl.PQconninfoOption *opts = impl.PQconninfo(self.pgconn_ptr)
        if opts is NULL:
            raise MemoryError("couldn't allocate connection info")
        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    def reset(self) -> None:
        self._ensure_pgconn()
        impl.PQreset(self.pgconn_ptr)

    def reset_start(self) -> None:
        if not impl.PQresetStart(self.pgconn_ptr):
            raise PQerror("couldn't reset connection")

    def reset_poll(self) -> PollingStatus:
        cdef int rv = self._call_int(<conn_int_f>impl.PQresetPoll)
        return PollingStatus(rv)

    @classmethod
    def ping(self, conninfo: bytes) -> Ping:
        cdef int rv = impl.PQping(conninfo)
        return Ping(rv)

    @property
    def db(self) -> bytes:
        return self._call_bytes(impl.PQdb)

    @property
    def user(self) -> bytes:
        return self._call_bytes(impl.PQuser)

    @property
    def password(self) -> bytes:
        return self._call_bytes(impl.PQpass)

    @property
    def host(self) -> bytes:
        return self._call_bytes(impl.PQhost)

    @property
    def hostaddr(self) -> bytes:
        return b'TODO'

    @property
    def port(self) -> bytes:
        return self._call_bytes(impl.PQport)

    @property
    def tty(self) -> bytes:
        return self._call_bytes(impl.PQtty)

    @property
    def options(self) -> bytes:
        return self._call_bytes(impl.PQoptions)

    @property
    def status(self) -> ConnStatus:
        cdef int rv = impl.PQstatus(self.pgconn_ptr)
        return ConnStatus(rv)

    @property
    def transaction_status(self) -> TransactionStatus:
        cdef int rv = impl.PQtransactionStatus(self.pgconn_ptr)
        return TransactionStatus(rv)

    def parameter_status(self, name: bytes) -> Optional[bytes]:
        self._ensure_pgconn()
        cdef const char *rv = impl.PQparameterStatus(self.pgconn_ptr, name)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def error_message(self) -> bytes:
        return impl.PQerrorMessage(self.pgconn_ptr)

    @property
    def protocol_version(self) -> int:
        return self._call_int(impl.PQprotocolVersion)

    @property
    def server_version(self) -> int:
        return self._call_int(impl.PQserverVersion)

    @property
    def socket(self) -> int:
        return self._call_int(impl.PQsocket)

    @property
    def backend_pid(self) -> int:
        return self._call_int(impl.PQbackendPID)

    @property
    def needs_password(self) -> bool:
        return bool(self._call_int(impl.PQconnectionNeedsPassword))

    @property
    def used_password(self) -> bool:
        return bool(self._call_int(impl.PQconnectionUsedPassword))

    @property
    def ssl_in_use(self) -> bool:
        return bool(self._call_int(<conn_int_f>impl.PQsslInUse))

    def exec_(self, command: bytes) -> PGresult:
        self._ensure_pgconn()
        cdef impl.PGresult *pgresult = impl.PQexec(self.pgconn_ptr, command)
        if pgresult is NULL:
            raise MemoryError("couldn't allocate PGresult")

        return PGresult._from_ptr(pgresult)

    def send_query(self, command: bytes) -> None:
        self._ensure_pgconn()
        if not impl.PQsendQuery(self.pgconn_ptr, command):
            raise PQerror(
                "sending query failed:"
                f" {error_message(self)}"
            )

    def exec_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> PGresult:
        self._ensure_pgconn()

        cdef int cnparams
        cdef Oid *ctypes
        cdef char *const *cvalues
        cdef int *clenghts
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, param_types, param_formats)

        cdef impl.PGresult *pgresult = impl.PQexecParams(
            self.pgconn_ptr, command, cnparams, ctypes,
            <const char *const *>cvalues, clengths, cformats, result_format)
        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if pgresult is NULL:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult._from_ptr(pgresult)

    def send_query_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        self._ensure_pgconn()

        cdef int cnparams
        cdef Oid *ctypes
        cdef char *const *cvalues
        cdef int *clenghts
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, param_types, param_formats)

        cdef int rv = impl.PQsendQueryParams(
            self.pgconn_ptr, command, cnparams, ctypes,
            <const char *const *>cvalues,
            clengths, cformats, result_format)
        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if not rv:
            raise PQerror(
                "sending query and params failed:"
                f" {error_message(self)}"
            )

    def send_prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> None:
        self._ensure_pgconn()

        cdef int i
        cdef int nparams = len(param_types) if param_types else 0
        cdef Oid *atypes = NULL
        if nparams:
            atypes = <Oid *>PyMem_Malloc(nparams * sizeof(Oid))
            for i in range(nparams):
                atypes[i] = param_types[i]

        cdef int rv = impl.PQsendPrepare(
            self.pgconn_ptr, name, command, nparams, atypes
        )
        PyMem_Free(atypes)
        if not rv:
            raise PQerror(
                "sending query and params failed:"
                f" {error_message(self)}"
            )

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        self._ensure_pgconn()

        cdef int cnparams
        cdef Oid *ctypes
        cdef char *const *cvalues
        cdef int *clenghts
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, None, param_formats)

        cdef int rv = impl.PQsendQueryPrepared(
            self.pgconn_ptr, name, cnparams,
            <const char *const *>cvalues,
            clengths, cformats, result_format)
        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if not rv:
            raise PQerror(
                "sending prepared query failed:"
                f" {error_message(self)}"
            )

    def prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> PGresult:
        self._ensure_pgconn()

        cdef int i
        cdef int nparams = len(param_types) if param_types else 0
        cdef Oid *atypes = NULL
        if nparams:
            atypes = <Oid *>PyMem_Malloc(nparams * sizeof(Oid))
            for i in range(nparams):
                atypes[i] = param_types[i]

        cdef impl.PGresult *rv = impl.PQprepare(
            self.pgconn_ptr, name, command, nparams, atypes)
        PyMem_Free(atypes)
        if rv is NULL:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult._from_ptr(rv)

    def exec_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[bytes]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = 0,
    ) -> PGresult:
        self._ensure_pgconn()

        cdef int cnparams
        cdef Oid *ctypes
        cdef char *const *cvalues
        cdef int *clenghts
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, None, param_formats)

        cdef impl.PGresult *rv = impl.PQexecPrepared(
            self.pgconn_ptr, name, cnparams,
            <const char *const *>cvalues,
            clengths, cformats, result_format)

        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if rv is NULL:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult._from_ptr(rv)

    def describe_prepared(self, name: bytes) -> PGresult:
        self._ensure_pgconn()
        cdef impl.PGresult *rv = impl.PQdescribePrepared(self.pgconn_ptr, name)
        if rv is NULL:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult._from_ptr(rv)

    def describe_portal(self, name: bytes) -> PGresult:
        self._ensure_pgconn()
        cdef impl.PGresult *rv = impl.PQdescribePortal(self.pgconn_ptr, name)
        if rv is NULL:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult._from_ptr(rv)

    def get_result(self) -> Optional["PGresult"]:
        cdef impl.PGresult *pgresult = impl.PQgetResult(self.pgconn_ptr)
        if pgresult is NULL:
            return None
        return PGresult._from_ptr(pgresult)

    def consume_input(self) -> None:
        if 1 != impl.PQconsumeInput(self.pgconn_ptr):
            raise PQerror(
                "consuming input failed:"
                f" {error_message(self)}"
            )

    def is_busy(self) -> int:
        return impl.PQisBusy(self.pgconn_ptr)

    @property
    def nonblocking(self) -> int:
        return impl.PQisnonblocking(self.pgconn_ptr)

    @nonblocking.setter
    def nonblocking(self, arg: int) -> None:
        if 0 > impl.PQsetnonblocking(self.pgconn_ptr, arg):
            raise PQerror(
                f"setting nonblocking failed:"
                f" {error_message(self)}"
            )

    def flush(self) -> int:
        cdef int rv = impl.PQflush(self.pgconn_ptr)
        if rv < 0:
            raise PQerror(
                f"flushing failed:{error_message(self)}"
            )
        return rv

    def make_empty_result(self, exec_status: ExecStatus) -> PGresult:
        cdef impl.PGresult *rv = impl.PQmakeEmptyPGresult(
            self.pgconn_ptr, exec_status)
        if not rv:
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult._from_ptr(rv)

    cdef int _ensure_pgconn(self) except 0:
        if self.pgconn_ptr is not NULL:
            return 1

        raise PQerror("the connection is closed")

    cdef char *_call_bytes(self, conn_bytes_f func) except NULL:
        """
        Call one of the pgconn libpq functions returning a bytes pointer.
        """
        if not self._ensure_pgconn():
            return NULL
        cdef char *rv = func(self.pgconn_ptr)
        assert rv is not NULL
        return rv

    cdef int _call_int(self, conn_int_f func) except -1:
        """
        Call one of the pgconn libpq functions returning an int.
        """
        if not self._ensure_pgconn():
            return -1
        return func(self.pgconn_ptr)


cdef PGconn _connect(const char *conninfo):
    cdef impl.PGconn* pgconn = impl.PQconnectdb(conninfo)
    if not pgconn:
        raise MemoryError("couldn't allocate PGconn")

    return PGconn._from_ptr(pgconn)


cdef PGconn _connect_start(const char *conninfo):
    cdef impl.PGconn* pgconn = impl.PQconnectStart(conninfo)
    if not pgconn:
        raise MemoryError("couldn't allocate PGconn")

    return PGconn._from_ptr(pgconn)


cdef (int, Oid *, char * const*, int *, int *) _query_params_args(
    param_values: Optional[Sequence[Optional[bytes]]],
    param_types: Optional[Sequence[int]],
    param_formats: Optional[Sequence[Format]],
) except *:
    cdef int i

    cdef int nparams = len(param_values) if param_values else 0
    if param_types is not None and len(param_types) != nparams:
        raise ValueError(
            "got %d param_values but %d param_types"
            % (nparams, len(param_types))
        )
    if param_formats is not None and len(param_formats) != nparams:
        raise ValueError(
            "got %d param_values but %d param_formats"
            % (nparams, len(param_formats))
        )

    cdef char **aparams = NULL
    cdef int *alenghts = NULL
    if nparams:
        aparams = <char **>PyMem_Malloc(nparams * sizeof(char *))
        alenghts = <int *>PyMem_Malloc(nparams * sizeof(int))
        for i in range(nparams):
            if param_values[i] is not None:
                aparams[i] = param_values[i]
                alenghts[i] = len(param_values[i])
            else:
                aparams[i] = NULL
                alenghts[i] = 0

    cdef Oid *atypes = NULL
    if param_types is not None:
        atypes = <Oid *>PyMem_Malloc(nparams * sizeof(Oid))
        for i in range(nparams):
            atypes[i] = param_types[i]

    cdef int *aformats = NULL
    if param_formats is not None:
        aformats = <int *>PyMem_Malloc(nparams * sizeof(int *))
        for i in range(nparams):
            aformats[i] = param_formats[i]

    return (nparams, atypes, aparams, alenghts, aformats)


cdef void _clear_query_params(
    Oid *ctypes, char *const *cvalues, int *clenghts, int *cformats
):
    PyMem_Free(ctypes)
    PyMem_Free(<char **>cvalues)
    PyMem_Free(clenghts)
    PyMem_Free(cformats)


cdef _options_from_array(impl.PQconninfoOption *opts):
    rv = []
    cdef int i = 0
    cdef impl.PQconninfoOption* opt
    while 1:
        opt = opts + i
        if opt.keyword is NULL:
            break
        rv.append(
            ConninfoOption(
                keyword=opt.keyword,
                envvar=opt.envvar if opt.envvar is not NULL else None,
                compiled=opt.compiled if opt.compiled is not NULL else None,
                val=opt.val if opt.val is not NULL else None,
                label=opt.label if opt.label is not NULL else None,
                dispchar=opt.dispchar if opt.dispchar is not NULL else None,
                dispsize=opt.dispsize,
            )
        )
        i += 1

    return rv


cdef class PGresult:
    def __cinit__(self):
        self.pgresult_ptr = NULL

    @staticmethod
    cdef PGresult _from_ptr(impl.PGresult *ptr):
        cdef PGresult rv = PGresult.__new__(PGresult)
        rv.pgresult_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.clear()

    def clear(self) -> None:
        if self.pgresult_ptr is not NULL:
            impl.PQclear(self.pgresult_ptr)
            self.pgresult_ptr = NULL

    @property
    def status(self) -> ExecStatus:
        cdef int rv = impl.PQresultStatus(self.pgresult_ptr)
        return ExecStatus(rv)

    @property
    def error_message(self) -> bytes:
        return impl.PQresultErrorMessage(self.pgresult_ptr)

    def error_field(self, fieldcode: DiagnosticField) -> Optional[bytes]:
        cdef char * rv = impl.PQresultErrorField(self.pgresult_ptr, fieldcode)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def ntuples(self) -> int:
        return impl.PQntuples(self.pgresult_ptr)

    @property
    def nfields(self) -> int:
        return impl.PQnfields(self.pgresult_ptr)

    def fname(self, column_number: int) -> Optional[bytes]:
        cdef char *rv = impl.PQfname(self.pgresult_ptr, column_number)
        if rv is not NULL:
            return rv
        else:
            return None

    def ftable(self, column_number: int) -> int:
        return impl.PQftable(self.pgresult_ptr, column_number)

    def ftablecol(self, column_number: int) -> int:
        return impl.PQftablecol(self.pgresult_ptr, column_number)

    def fformat(self, column_number: int) -> Format:
        return Format(impl.PQfformat(self.pgresult_ptr, column_number))

    def ftype(self, column_number: int) -> int:
        return impl.PQftype(self.pgresult_ptr, column_number)

    def fmod(self, column_number: int) -> int:
        return impl.PQfmod(self.pgresult_ptr, column_number)

    def fsize(self, column_number: int) -> int:
        return impl.PQfsize(self.pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> Format:
        return Format(impl.PQbinaryTuples(self.pgresult_ptr))

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        cdef int crow = row_number
        cdef int ccol = column_number
        cdef int length = impl.PQgetlength(self.pgresult_ptr, crow, ccol)
        cdef char *v;
        if length:
            v = impl.PQgetvalue(self.pgresult_ptr, crow, ccol)
            # TODO: avoid copy
            return v[:length]
        else:
            if impl.PQgetisnull(self.pgresult_ptr, crow, ccol):
                return None
            else:
                return b""

    @property
    def nparams(self) -> int:
        return impl.PQnparams(self.pgresult_ptr)

    def param_type(self, param_number: int) -> int:
        return impl.PQparamtype(self.pgresult_ptr, param_number)

    @property
    def command_status(self) -> Optional[bytes]:
        cdef char *rv = impl.PQcmdStatus(self.pgresult_ptr)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def command_tuples(self) -> Optional[int]:
        cdef char *rv = impl.PQcmdTuples(self.pgresult_ptr)
        if rv is NULL:
            return None
        cdef bytes brv = rv
        return int(brv) if brv else None

    @property
    def oid_value(self) -> int:
        return impl.PQoidValue(self.pgresult_ptr)


class Conninfo:
    @classmethod
    def get_defaults(cls) -> List[ConninfoOption]:
        cdef impl.PQconninfoOption *opts = impl.PQconndefaults()
        if opts is NULL :
            raise MemoryError("couldn't allocate connection defaults")
        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    @classmethod
    def parse(cls, conninfo: bytes) -> List[ConninfoOption]:
        cdef char *errmsg = NULL
        cdef impl.PQconninfoOption *opts = impl.PQconninfoParse(conninfo, &errmsg)
        if opts is NULL:
            if errmsg is NULL:
                raise MemoryError("couldn't allocate on conninfo parse")
            else:
                exc = PQerror(errmsg.decode("utf8", "replace"))
                impl.PQfreemem(errmsg)
                raise exc

        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    def __repr__(self):
        return f"<{type(self).__name__} ({self.keyword.decode('ascii')})>"


cdef class Escaping:
    cdef PGconn conn

    def __init__(self, conn: Optional[PGconn] = None):
        self.conn = conn

    def escape_bytea(self, data: bytes) -> bytes:
        cdef size_t len_out
        cdef unsigned char *out
        if self.conn is not None:
            if self.conn.pgconn_ptr is NULL:
                raise PQerror("the connection is closed")
            out = impl.PQescapeByteaConn(
                self.conn.pgconn_ptr, data, len(data), &len_out)
        else:
            out = impl.PQescapeBytea(data, len(data), &len_out)
        if out is NULL:
            raise MemoryError(
                f"couldn't allocate for escape_bytea of {len(data)} bytes"
            )

        # TODO: without copy?
        rv = out[:len_out - 1]  # out includes final 0
        impl.PQfreemem(out)
        return rv

    def unescape_bytea(self, data: bytes) -> bytes:
        # not needed, but let's keep it symmetric with the escaping:
        # if a connection is passed in, it must be valid.
        if self.conn is not None:
            if self.conn.pgconn_ptr is NULL:
                raise PQerror("the connection is closed")

        cdef size_t len_out
        cdef unsigned char *out = impl.PQunescapeBytea(data, &len_out)
        if out is NULL:
            raise MemoryError(
                f"couldn't allocate for unescape_bytea of {len(data)} bytes"
            )

        rv = out[:len_out]
        impl.PQfreemem(out)
        return rv
