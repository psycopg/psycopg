"""
psycopg_c.pq.PGconn object implementation.
"""

# Copyright (C) 2020 The Psycopg Team

cdef extern from * nogil:
    """
#if defined(_WIN32) || defined(WIN32) || defined(MS_WINDOWS)
    /* We don't need a real definition for this because Windows is not affected
     * by the issue caused by closing the fds after fork.
     */
    #define getpid() (0)
#else
    #include <unistd.h>
#endif
    """
    pid_t getpid()

from libc.stdio cimport fdopen
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from cpython.bytes cimport PyBytes_AsString
from cpython.memoryview cimport PyMemoryView_FromObject

import sys

from psycopg.pq import Format as PqFormat
from psycopg.pq import Trace, version_pretty
from psycopg.pq.misc import PGnotify, _clean_error_message, connection_summary
from psycopg.pq._enums import ExecStatus
from psycopg._encodings import pg2pyenc

from psycopg_c.pq cimport PQBuffer


cdef object _check_supported(fname, int pgversion):
    if libpq.PG_VERSION_NUM < pgversion:
        raise e.NotSupportedError(
            f"{fname} requires libpq from PostgreSQL {version_pretty(pgversion)}"
            f" on the client; version {version_pretty(libpq.PG_VERSION_NUM)}"
            " available instead"
        )

cdef class PGconn:
    @staticmethod
    cdef PGconn _from_ptr(libpq.PGconn *ptr):
        cdef PGconn rv = PGconn.__new__(PGconn)
        rv._pgconn_ptr = ptr

        libpq.PQsetNoticeReceiver(
            ptr, <libpq.PQnoticeReceiver>notice_receiver, <void *>rv)
        return rv

    def __cinit__(self):
        self._pgconn_ptr = NULL
        self._procpid = getpid()

    def __dealloc__(self):
        # Close the connection only if it was created in this process,
        # not if this object is being GC'd after fork.
        if self._procpid == getpid():
            self.finish()

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = connection_summary(self)
        return f"<{cls} {info} at 0x{id(self):x}>"

    @classmethod
    def connect(cls, const char *conninfo) -> PGconn:
        cdef libpq.PGconn* pgconn = libpq.PQconnectdb(conninfo)
        if not pgconn:
            raise MemoryError("couldn't allocate PGconn")

        return PGconn._from_ptr(pgconn)

    @classmethod
    def connect_start(cls, const char *conninfo) -> PGconn:
        cdef libpq.PGconn* pgconn = libpq.PQconnectStart(conninfo)
        if not pgconn:
            raise MemoryError("couldn't allocate PGconn")

        return PGconn._from_ptr(pgconn)

    def connect_poll(self) -> int:
        return _call_int(self, <conn_int_f>libpq.PQconnectPoll)

    def finish(self) -> None:
        if self._pgconn_ptr is not NULL:
            libpq.PQfinish(self._pgconn_ptr)
            self._pgconn_ptr = NULL

    @property
    def pgconn_ptr(self) -> int | None:
        if self._pgconn_ptr:
            return <long long><void *>self._pgconn_ptr
        else:
            return None

    @property
    def info(self) -> list[ConninfoOption]:
        _ensure_pgconn(self)
        cdef libpq.PQconninfoOption *opts = libpq.PQconninfo(self._pgconn_ptr)
        if opts is NULL:
            raise MemoryError("couldn't allocate connection info")
        rv = _options_from_array(opts)
        libpq.PQconninfoFree(opts)
        return rv

    def reset(self) -> None:
        _ensure_pgconn(self)
        libpq.PQreset(self._pgconn_ptr)

    def reset_start(self) -> None:
        if not libpq.PQresetStart(self._pgconn_ptr):
            raise e.OperationalError("couldn't reset connection")

    def reset_poll(self) -> int:
        return _call_int(self, <conn_int_f>libpq.PQresetPoll)

    @classmethod
    def ping(self, const char *conninfo) -> int:
        return libpq.PQping(conninfo)

    @property
    def db(self) -> bytes:
        return _call_bytes(self, libpq.PQdb)

    @property
    def user(self) -> bytes:
        return _call_bytes(self, libpq.PQuser)

    @property
    def password(self) -> bytes:
        return _call_bytes(self, libpq.PQpass)

    @property
    def host(self) -> bytes:
        return _call_bytes(self, libpq.PQhost)

    @property
    def hostaddr(self) -> bytes:
        _check_supported("PQhostaddr", 120000)
        _ensure_pgconn(self)
        cdef char *rv = libpq.PQhostaddr(self._pgconn_ptr)
        assert rv is not NULL
        return rv

    @property
    def port(self) -> bytes:
        return _call_bytes(self, libpq.PQport)

    @property
    def tty(self) -> bytes:
        return _call_bytes(self, libpq.PQtty)

    @property
    def options(self) -> bytes:
        return _call_bytes(self, libpq.PQoptions)

    @property
    def status(self) -> int:
        return libpq.PQstatus(self._pgconn_ptr)

    @property
    def transaction_status(self) -> int:
        return libpq.PQtransactionStatus(self._pgconn_ptr)

    def parameter_status(self, const char *name) -> bytes | None:
        _ensure_pgconn(self)
        cdef const char *rv = libpq.PQparameterStatus(self._pgconn_ptr, name)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def error_message(self) -> bytes:
        return libpq.PQerrorMessage(self._pgconn_ptr)

    def get_error_message(self, encoding: str = "") -> str:
        return _clean_error_message(self.error_message, encoding or self._encoding)

    @property
    def _encoding(self) -> str:
        cdef const char *pgenc
        if libpq.PQstatus(self._pgconn_ptr) == libpq.CONNECTION_OK:
            pgenc = libpq.PQparameterStatus(self._pgconn_ptr, b"client_encoding")
            if pgenc is NULL:
                pgenc = b"UTF8"
            return pg2pyenc(pgenc)
        else:
            return "utf-8"

    @property
    def protocol_version(self) -> int:
        return _call_int(self, libpq.PQprotocolVersion)

    @property
    def server_version(self) -> int:
        return _call_int(self, libpq.PQserverVersion)

    @property
    def socket(self) -> int:
        rv = _call_int(self, libpq.PQsocket)
        if rv == -1:
            raise e.OperationalError("the connection is lost")
        return rv

    @property
    def backend_pid(self) -> int:
        return _call_int(self, libpq.PQbackendPID)

    @property
    def needs_password(self) -> bool:
        return bool(libpq.PQconnectionNeedsPassword(self._pgconn_ptr))

    @property
    def used_password(self) -> bool:
        return bool(libpq.PQconnectionUsedPassword(self._pgconn_ptr))

    @property
    def ssl_in_use(self) -> bool:
        return bool(_call_int(self, <conn_int_f>libpq.PQsslInUse))

    def exec_(self, const char *command) -> PGresult:
        _ensure_pgconn(self)
        cdef libpq.PGresult *pgresult
        with nogil:
            pgresult = libpq.PQexec(self._pgconn_ptr, command)
        if pgresult is NULL:
            raise e.OperationalError(f"executing query failed: {self.get_error_message()}")

        return PGresult._from_ptr(pgresult)

    def send_query(self, const char *command) -> None:
        _ensure_pgconn(self)
        cdef int rv
        with nogil:
            rv = libpq.PQsendQuery(self._pgconn_ptr, command)
        if not rv:
            raise e.OperationalError(f"sending query failed: {self.get_error_message()}")

    def exec_params(
        self,
        const char *command,
        param_values: Sequence[bytes | None] | None,
        param_types: Sequence[int] | None = None,
        param_formats: Sequence[int] | None = None,
        int result_format = PqFormat.TEXT,
    ) -> PGresult:
        _ensure_pgconn(self)

        cdef Py_ssize_t cnparams
        cdef libpq.Oid *ctypes
        cdef char *const *cvalues
        cdef int *clengths
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, param_types, param_formats)

        cdef libpq.PGresult *pgresult
        with nogil:
            pgresult = libpq.PQexecParams(
                self._pgconn_ptr, command, <int>cnparams, ctypes,
                <const char *const *>cvalues, clengths, cformats, result_format)
        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if pgresult is NULL:
            raise e.OperationalError(f"executing query failed: {self.get_error_message()}")
        return PGresult._from_ptr(pgresult)

    def send_query_params(
        self,
        const char *command,
        param_values: Sequence[bytes | None] | None,
        param_types: Sequence[int] | None = None,
        param_formats: Sequence[int] | None = None,
        int result_format = PqFormat.TEXT,
    ) -> None:
        _ensure_pgconn(self)

        cdef Py_ssize_t cnparams
        cdef libpq.Oid *ctypes
        cdef char *const *cvalues
        cdef int *clengths
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, param_types, param_formats)

        cdef int rv
        with nogil:
            rv = libpq.PQsendQueryParams(
                self._pgconn_ptr, command, <int>cnparams, ctypes,
                <const char *const *>cvalues, clengths, cformats, result_format)
        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if not rv:
            raise e.OperationalError(
                f"sending query and params failed: {self.get_error_message()}"
            )

    def send_prepare(
        self,
        const char *name,
        const char *command,
        param_types: Sequence[int] | None = None,
    ) -> None:
        _ensure_pgconn(self)

        cdef int i
        cdef Py_ssize_t nparams = len(param_types) if param_types else 0
        cdef libpq.Oid *atypes = NULL
        if nparams:
            atypes = <libpq.Oid *>PyMem_Malloc(nparams * sizeof(libpq.Oid))
            for i in range(nparams):
                atypes[i] = param_types[i]

        cdef int rv
        with nogil:
            rv = libpq.PQsendPrepare(
                self._pgconn_ptr, name, command, <int>nparams, atypes
            )
        PyMem_Free(atypes)
        if not rv:
            raise e.OperationalError(
                f"sending query and params failed: {self.get_error_message()}"
            )

    def send_query_prepared(
        self,
        const char *name,
        param_values: Sequence[bytes | None] | None,
        param_formats: Sequence[int] | None = None,
        int result_format = PqFormat.TEXT,
    ) -> None:
        _ensure_pgconn(self)

        cdef Py_ssize_t cnparams
        cdef libpq.Oid *ctypes
        cdef char *const *cvalues
        cdef int *clengths
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, None, param_formats)

        cdef int rv
        with nogil:
            rv = libpq.PQsendQueryPrepared(
                self._pgconn_ptr, name, <int>cnparams, <const char *const *>cvalues,
                clengths, cformats, result_format)
        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if not rv:
            raise e.OperationalError(
                f"sending prepared query failed: {self.get_error_message()}"
            )

    def prepare(
        self,
        const char *name,
        const char *command,
        param_types: Sequence[int] | None = None,
    ) -> PGresult:
        _ensure_pgconn(self)

        cdef int i
        cdef Py_ssize_t nparams = len(param_types) if param_types else 0
        cdef libpq.Oid *atypes = NULL
        if nparams:
            atypes = <libpq.Oid *>PyMem_Malloc(nparams * sizeof(libpq.Oid))
            for i in range(nparams):
                atypes[i] = param_types[i]

        cdef libpq.PGresult *rv
        with nogil:
            rv = libpq.PQprepare(
                self._pgconn_ptr, name, command, <int>nparams, atypes)
        PyMem_Free(atypes)
        if rv is NULL:
            raise e.OperationalError(f"preparing query failed: {self.get_error_message()}")
        return PGresult._from_ptr(rv)

    def exec_prepared(
        self,
        const char *name,
        param_values: Sequence[bytes] | None,
        param_formats: Sequence[int] | None = None,
        int result_format = PqFormat.TEXT,
    ) -> PGresult:
        _ensure_pgconn(self)

        cdef Py_ssize_t cnparams
        cdef libpq.Oid *ctypes
        cdef char *const *cvalues
        cdef int *clengths
        cdef int *cformats
        cnparams, ctypes, cvalues, clengths, cformats = _query_params_args(
            param_values, None, param_formats)

        cdef libpq.PGresult *rv
        with nogil:
            rv = libpq.PQexecPrepared(
                self._pgconn_ptr, name, <int>cnparams,
                <const char *const *>cvalues,
                clengths, cformats, result_format)

        _clear_query_params(ctypes, cvalues, clengths, cformats)
        if rv is NULL:
            raise e.OperationalError(
                f"executing prepared query failed: {self.get_error_message()}"
            )
        return PGresult._from_ptr(rv)

    def describe_prepared(self, const char *name) -> PGresult:
        _ensure_pgconn(self)
        cdef libpq.PGresult *rv = libpq.PQdescribePrepared(self._pgconn_ptr, name)
        if rv is NULL:
            raise e.OperationalError(
                f"describe prepared failed: {self.get_error_message()}"
            )
        return PGresult._from_ptr(rv)

    def send_describe_prepared(self, const char *name) -> None:
        _ensure_pgconn(self)
        cdef int rv = libpq.PQsendDescribePrepared(self._pgconn_ptr, name)
        if not rv:
            raise e.OperationalError(
                f"sending describe prepared failed: {self.get_error_message()}"
            )

    def describe_portal(self, const char *name) -> PGresult:
        _ensure_pgconn(self)
        cdef libpq.PGresult *rv = libpq.PQdescribePortal(self._pgconn_ptr, name)
        if rv is NULL:
            raise e.OperationalError(
                f"describe prepared failed: {self.get_error_message()}"
            )
        return PGresult._from_ptr(rv)

    def send_describe_portal(self, const char *name) -> None:
        _ensure_pgconn(self)
        cdef int rv = libpq.PQsendDescribePortal(self._pgconn_ptr, name)
        if not rv:
            raise e.OperationalError(
                f"sending describe prepared failed: {self.get_error_message()}"
            )

    def close_prepared(self, const char *name) -> PGresult:
        _check_supported("PQclosePrepared", 170000)
        _ensure_pgconn(self)
        cdef libpq.PGresult *rv = libpq.PQclosePrepared(self._pgconn_ptr, name)
        if rv is NULL:
            raise e.OperationalError(
                f"close prepared failed: {self.get_error_message()}"
            )
        return PGresult._from_ptr(rv)

    def send_close_prepared(self, const char *name) -> None:
        _check_supported("PQsendClosePrepared", 170000)
        _ensure_pgconn(self)
        cdef int rv = libpq.PQsendClosePrepared(self._pgconn_ptr, name)
        if not rv:
            raise e.OperationalError(
                f"sending close prepared failed: {self.get_error_message()}"
            )

    def close_portal(self, const char *name) -> PGresult:
        _check_supported("PQclosePortal", 170000)
        _ensure_pgconn(self)
        cdef libpq.PGresult *rv = libpq.PQclosePortal(self._pgconn_ptr, name)
        if rv is NULL:
            raise e.OperationalError(
                f"close prepared failed: {self.get_error_message()}"
            )
        return PGresult._from_ptr(rv)

    def send_close_portal(self, const char *name) -> None:
        _check_supported("PQsendClosePortal", 170000)
        _ensure_pgconn(self)
        cdef int rv = libpq.PQsendClosePortal(self._pgconn_ptr, name)
        if not rv:
            raise e.OperationalError(
                f"sending close prepared failed: {self.get_error_message()}"
            )

    def get_result(self) -> "PGresult" | None:
        cdef libpq.PGresult *pgresult = libpq.PQgetResult(self._pgconn_ptr)
        if pgresult is NULL:
            return None
        return PGresult._from_ptr(pgresult)

    def consume_input(self) -> None:
        if 1 != libpq.PQconsumeInput(self._pgconn_ptr):
            raise e.OperationalError(f"consuming input failed: {self.get_error_message()}")

    def is_busy(self) -> int:
        cdef int rv
        with nogil:
            rv = libpq.PQisBusy(self._pgconn_ptr)
        return rv

    @property
    def nonblocking(self) -> int:
        return libpq.PQisnonblocking(self._pgconn_ptr)

    @nonblocking.setter
    def nonblocking(self, int arg) -> None:
        if 0 > libpq.PQsetnonblocking(self._pgconn_ptr, arg):
            raise e.OperationalError(f"setting nonblocking failed: {self.get_error_message()}")

    cpdef int flush(self) except -1:
        if self._pgconn_ptr == NULL:
            raise e.OperationalError(f"flushing failed: the connection is closed")
        cdef int rv = libpq.PQflush(self._pgconn_ptr)
        if rv < 0:
            raise e.OperationalError(f"flushing failed: {self.get_error_message()}")
        return rv

    def set_single_row_mode(self) -> None:
        if not libpq.PQsetSingleRowMode(self._pgconn_ptr):
            raise e.OperationalError("setting single row mode failed")

    def set_chunked_rows_mode(self, size: int) -> None:
        if not libpq.PQsetChunkedRowsMode(self._pgconn_ptr, size):
            raise e.OperationalError("setting chunked rows mode failed")

    def cancel_conn(self) -> PGcancelConn:
        _check_supported("PQcancelCreate", 170000)
        cdef libpq.PGcancelConn *ptr = libpq.PQcancelCreate(self._pgconn_ptr)
        if not ptr:
            raise e.OperationalError("couldn't create cancelConn object")
        return PGcancelConn._from_ptr(ptr)

    def get_cancel(self) -> PGcancel:
        cdef libpq.PGcancel *ptr = libpq.PQgetCancel(self._pgconn_ptr)
        if not ptr:
            raise e.OperationalError("couldn't create cancel object")
        return PGcancel._from_ptr(ptr)

    cpdef object notifies(self):
        cdef libpq.PGnotify *ptr
        with nogil:
            ptr = libpq.PQnotifies(self._pgconn_ptr)
        if ptr:
            ret = PGnotify(ptr.relname, ptr.be_pid, ptr.extra)
            libpq.PQfreemem(ptr)
            return ret
        else:
            return None

    def put_copy_data(self, buffer) -> int:
        cdef int rv
        cdef char *cbuffer
        cdef Py_ssize_t length

        _buffer_as_string_and_size(buffer, &cbuffer, &length)
        rv = libpq.PQputCopyData(self._pgconn_ptr, cbuffer, <int>length)
        if rv < 0:
            raise e.OperationalError(f"sending copy data failed: {self.get_error_message()}")
        return rv

    def put_copy_end(self, error: bytes | None = None) -> int:
        cdef int rv
        cdef const char *cerr = NULL
        if error is not None:
            cerr = PyBytes_AsString(error)
        rv = libpq.PQputCopyEnd(self._pgconn_ptr, cerr)
        if rv < 0:
            raise e.OperationalError(f"sending copy end failed: {self.get_error_message()}")
        return rv

    def get_copy_data(self, int async_) -> tuple[int, memoryview]:
        cdef char *buffer_ptr = NULL
        cdef int nbytes
        nbytes = libpq.PQgetCopyData(self._pgconn_ptr, &buffer_ptr, async_)
        if nbytes == -2:
            raise e.OperationalError(f"receiving copy data failed: {self.get_error_message()}")
        if buffer_ptr is not NULL:
            data = PyMemoryView_FromObject(
                PQBuffer._from_buffer(<unsigned char *>buffer_ptr, nbytes))
            return nbytes, data
        else:
            return nbytes, b""  # won't parse it, doesn't really be memoryview

    def trace(self, fileno: int) -> None:
        if sys.platform != "linux":
            raise e.NotSupportedError("currently only supported on Linux")
        stream = fdopen(fileno, b"w")
        libpq.PQtrace(self._pgconn_ptr, stream)

    def set_trace_flags(self, flags: Trace) -> None:
        _check_supported("PQsetTraceFlags", 140000)
        libpq.PQsetTraceFlags(self._pgconn_ptr, flags)

    def untrace(self) -> None:
        libpq.PQuntrace(self._pgconn_ptr)

    def encrypt_password(
        self, const char *passwd, const char *user, algorithm = None
    ) -> bytes:
        _check_supported("PQencryptPasswordConn", 100000)

        cdef char *out
        cdef const char *calgo = NULL
        if algorithm:
            calgo = algorithm
        out = libpq.PQencryptPasswordConn(self._pgconn_ptr, passwd, user, calgo)
        if not out:
            raise e.OperationalError(
                f"password encryption failed: {self.get_error_message()}"
            )

        rv = bytes(out)
        libpq.PQfreemem(out)
        return rv

    def change_password(
        self, const char *user, const char *passwd
    ) -> None:
        _check_supported("PQchangePassword", 170000)

        cdef libpq.PGresult *res
        res = libpq.PQchangePassword(self._pgconn_ptr, user, passwd)
        if libpq.PQresultStatus(res) != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                f"password encryption failed: {self.get_error_message()}"
            )

    def make_empty_result(self, int exec_status) -> PGresult:
        cdef libpq.PGresult *rv = libpq.PQmakeEmptyPGresult(
            self._pgconn_ptr, <libpq.ExecStatusType>exec_status)
        if not rv:
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult._from_ptr(rv)

    @property
    def pipeline_status(self) -> int:
        """The current pipeline mode status.

        For libpq < 14.0, always return 0 (PQ_PIPELINE_OFF).
        """
        if libpq.PG_VERSION_NUM < 140000:
            return libpq.PQ_PIPELINE_OFF
        cdef int status = libpq.PQpipelineStatus(self._pgconn_ptr)
        return status

    def enter_pipeline_mode(self) -> None:
        """Enter pipeline mode.

        :raises ~e.OperationalError: in case of failure to enter the pipeline
            mode.
        """
        _check_supported("PQenterPipelineMode", 140000)
        if libpq.PQenterPipelineMode(self._pgconn_ptr) != 1:
            raise e.OperationalError("failed to enter pipeline mode")

    def exit_pipeline_mode(self) -> None:
        """Exit pipeline mode.

        :raises ~e.OperationalError: in case of failure to exit the pipeline
            mode.
        """
        _check_supported("PQexitPipelineMode", 140000)
        if libpq.PQexitPipelineMode(self._pgconn_ptr) != 1:
            raise e.OperationalError(self.get_error_message())

    def pipeline_sync(self) -> None:
        """Mark a synchronization point in a pipeline.

        :raises ~e.OperationalError: if the connection is not in pipeline mode
            or if sync failed.
        """
        _check_supported("PQpipelineSync", 140000)
        rv = libpq.PQpipelineSync(self._pgconn_ptr)
        if rv == 0:
            raise e.OperationalError("connection not in pipeline mode")
        if rv != 1:
            raise e.OperationalError("failed to sync pipeline")

    def send_flush_request(self) -> None:
        """Sends a request for the server to flush its output buffer.

        :raises ~e.OperationalError: if the flush request failed.
        """
        _check_supported("PQsendFlushRequest ", 140000)
        cdef int rv = libpq.PQsendFlushRequest(self._pgconn_ptr)
        if rv == 0:
            raise e.OperationalError(f"flush request failed: {self.get_error_message()}")


cdef int _ensure_pgconn(PGconn pgconn) except 0:
    if pgconn._pgconn_ptr is not NULL:
        return 1

    raise e.OperationalError("the connection is closed")


cdef char *_call_bytes(PGconn pgconn, conn_bytes_f func) except NULL:
    """
    Call one of the pgconn libpq functions returning a bytes pointer.
    """
    if not _ensure_pgconn(pgconn):
        return NULL
    cdef char *rv = func(pgconn._pgconn_ptr)
    if rv is not NULL:
        return rv
    else:
        return b""


cdef int _call_int(PGconn pgconn, conn_int_f func) except -2:
    """
    Call one of the pgconn libpq functions returning an int.
    """
    if not _ensure_pgconn(pgconn):
        return -2
    return func(pgconn._pgconn_ptr)


cdef void notice_receiver(void *arg, const libpq.PGresult *res_ptr) noexcept with gil:
    cdef PGconn pgconn = <object>arg
    if pgconn.notice_handler is None:
        return

    cdef PGresult res = PGresult._from_ptr(<libpq.PGresult *>res_ptr)
    try:
        pgconn.notice_handler(res)
    except Exception as e:
        logger.exception("error in notice receiver: %s", e)
    finally:
        res._pgresult_ptr = NULL  # avoid destroying the pgresult_ptr


cdef (Py_ssize_t, libpq.Oid *, char * const*, int *, int *) _query_params_args(
    list param_values: Sequence[bytes | None] | None,
    param_types: Sequence[int] | None,
    list param_formats: Sequence[int] | None,
) except *:
    cdef int i

    # the PostgresQuery converts the param_types to tuple, so this operation
    # is most often no-op
    cdef tuple tparam_types
    if param_types is not None and not isinstance(param_types, tuple):
        tparam_types = tuple(param_types)
    else:
        tparam_types = param_types

    cdef Py_ssize_t nparams = len(param_values) if param_values else 0
    if tparam_types is not None and len(tparam_types) != nparams:
        raise ValueError(
            "got %d param_values but %d param_types"
            % (nparams, len(tparam_types))
        )
    if param_formats is not None and len(param_formats) != nparams:
        raise ValueError(
            "got %d param_values but %d param_formats"
            % (nparams, len(param_formats))
        )

    cdef char **aparams = NULL
    cdef int *alenghts = NULL
    cdef char *ptr
    cdef Py_ssize_t length

    if nparams:
        aparams = <char **>PyMem_Malloc(nparams * sizeof(char *))
        alenghts = <int *>PyMem_Malloc(nparams * sizeof(int))
        for i in range(nparams):
            obj = param_values[i]
            if obj is None:
                aparams[i] = NULL
                alenghts[i] = 0
            else:
                # TODO: it is a leak if this fails (but it should only fail
                # on internal error, e.g. if obj is not a buffer)
                _buffer_as_string_and_size(obj, &ptr, &length)
                aparams[i] = ptr
                alenghts[i] = <int>length

    cdef libpq.Oid *atypes = NULL
    if tparam_types:
        atypes = <libpq.Oid *>PyMem_Malloc(nparams * sizeof(libpq.Oid))
        for i in range(nparams):
            atypes[i] = tparam_types[i]

    cdef int *aformats = NULL
    if param_formats is not None:
        aformats = <int *>PyMem_Malloc(nparams * sizeof(int *))
        for i in range(nparams):
            aformats[i] = param_formats[i]

    return (nparams, atypes, aparams, alenghts, aformats)


cdef void _clear_query_params(
    libpq.Oid *ctypes, char *const *cvalues, int *clenghst, int *cformats
):
    PyMem_Free(ctypes)
    PyMem_Free(<char **>cvalues)
    PyMem_Free(clenghst)
    PyMem_Free(cformats)
