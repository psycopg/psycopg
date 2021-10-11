"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import os
import logging
from weakref import ref
from functools import partial

from ctypes import Array, pointer, string_at, create_string_buffer, byref
from ctypes import addressof, c_char_p, c_int, c_size_t, c_ulong
from typing import Any, Callable, List, Optional, Sequence, Tuple
from typing import cast as t_cast, TYPE_CHECKING

from .. import errors as e
from . import _pq_ctypes as impl
from .misc import PGnotify, ConninfoOption, PGresAttDesc
from .misc import error_message, connection_summary
from ._enums import Format, ExecStatus

if TYPE_CHECKING:
    from . import abc

__impl__ = "python"

logger = logging.getLogger("psycopg")


def version() -> int:
    """Return the version number of the libpq currently loaded.

    The number is in the same format of `~psycopg.ConnectionInfo.server_version`.

    Certain features might not be available if the libpq library used is too old.
    """
    return impl.PQlibVersion()


def notice_receiver(
    arg: Any, result_ptr: impl.PGresult_struct, wconn: "ref[PGconn]"
) -> None:
    pgconn = wconn()
    if not (pgconn and pgconn.notice_handler):
        return

    res = PGresult(result_ptr)
    try:
        pgconn.notice_handler(res)
    except Exception as exc:
        logger.exception("error in notice receiver: %s", exc)

    res._pgresult_ptr = None  # avoid destroying the pgresult_ptr


class PGconn:
    """
    Python representation of a libpq connection.
    """

    __module__ = "psycopg.pq"
    __slots__ = (
        "_pgconn_ptr",
        "notice_handler",
        "notify_handler",
        "_notice_receiver",
        "_procpid",
        "__weakref__",
    )

    def __init__(self, pgconn_ptr: impl.PGconn_struct):
        self._pgconn_ptr: Optional[impl.PGconn_struct] = pgconn_ptr
        self.notice_handler: Optional[Callable[["abc.PGresult"], None]] = None
        self.notify_handler: Optional[Callable[[PGnotify], None]] = None

        self._notice_receiver = impl.PQnoticeReceiver(  # type: ignore
            partial(notice_receiver, wconn=ref(self))
        )
        impl.PQsetNoticeReceiver(pgconn_ptr, self._notice_receiver, None)

        self._procpid = os.getpid()

    def __del__(self) -> None:
        # Close the connection only if it was created in this process,
        # not if this object is being GC'd after fork.
        if os.getpid() == self._procpid:
            self.finish()

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = connection_summary(self)
        return f"<{cls} {info} at 0x{id(self):x}>"

    @classmethod
    def connect(cls, conninfo: bytes) -> "PGconn":
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        pgconn_ptr = impl.PQconnectdb(conninfo)
        if not pgconn_ptr:
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    @classmethod
    def connect_start(cls, conninfo: bytes) -> "PGconn":
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        pgconn_ptr = impl.PQconnectStart(conninfo)
        if not pgconn_ptr:
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    def connect_poll(self) -> int:
        return self._call_int(impl.PQconnectPoll)

    def finish(self) -> None:
        self._pgconn_ptr, p = None, self._pgconn_ptr
        if p:
            impl.PQfinish(p)

    @property
    def pgconn_ptr(self) -> Optional[int]:
        """The pointer to the underlying ``PGconn`` structure, as integer.

        `!None` if the connection is closed.

        The value can be used to pass the structure to libpq functions which
        psycopg doesn't (currently) wrap, either in C or in Python using FFI
        libraries such as `ctypes`.
        """
        if self._pgconn_ptr is None:
            return None

        return addressof(self._pgconn_ptr.contents)  # type: ignore[attr-defined]

    @property
    def info(self) -> List["ConninfoOption"]:
        self._ensure_pgconn()
        opts = impl.PQconninfo(self._pgconn_ptr)
        if not opts:
            raise MemoryError("couldn't allocate connection info")
        try:
            return Conninfo._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    def reset(self) -> None:
        self._ensure_pgconn()
        impl.PQreset(self._pgconn_ptr)

    def reset_start(self) -> None:
        if not impl.PQresetStart(self._pgconn_ptr):
            raise e.OperationalError("couldn't reset connection")

    def reset_poll(self) -> int:
        return self._call_int(impl.PQresetPoll)

    @classmethod
    def ping(self, conninfo: bytes) -> int:
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        return impl.PQping(conninfo)

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
        return self._call_bytes(impl.PQhostaddr)

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
    def status(self) -> int:
        return impl.PQstatus(self._pgconn_ptr)

    @property
    def transaction_status(self) -> int:
        return impl.PQtransactionStatus(self._pgconn_ptr)

    def parameter_status(self, name: bytes) -> Optional[bytes]:
        self._ensure_pgconn()
        return impl.PQparameterStatus(self._pgconn_ptr, name)

    @property
    def error_message(self) -> bytes:
        return impl.PQerrorMessage(self._pgconn_ptr)

    @property
    def protocol_version(self) -> int:
        return self._call_int(impl.PQprotocolVersion)

    @property
    def server_version(self) -> int:
        return self._call_int(impl.PQserverVersion)

    @property
    def socket(self) -> int:
        rv = self._call_int(impl.PQsocket)
        if rv == -1:
            raise e.OperationalError("the connection is lost")
        return rv

    @property
    def backend_pid(self) -> int:
        return self._call_int(impl.PQbackendPID)

    @property
    def needs_password(self) -> bool:
        return bool(impl.PQconnectionNeedsPassword(self._pgconn_ptr))

    @property
    def used_password(self) -> bool:
        return bool(impl.PQconnectionUsedPassword(self._pgconn_ptr))

    @property
    def ssl_in_use(self) -> bool:
        return self._call_bool(impl.PQsslInUse)

    def exec_(self, command: bytes) -> "PGresult":
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        self._ensure_pgconn()
        rv = impl.PQexec(self._pgconn_ptr, command)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query(self, command: bytes) -> None:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        self._ensure_pgconn()
        if not impl.PQsendQuery(self._pgconn_ptr, command):
            raise e.OperationalError(
                f"sending query failed: {error_message(self)}"
            )

    def exec_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> "PGresult":
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        self._ensure_pgconn()
        rv = impl.PQexecParams(*args)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> None:
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        self._ensure_pgconn()
        if not impl.PQsendQueryParams(*args):
            raise e.OperationalError(
                f"sending query and params failed: {error_message(self)}"
            )

    def send_prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> None:
        atypes: Optional[Array[impl.Oid]]
        if not param_types:
            nparams = 0
            atypes = None
        else:
            nparams = len(param_types)
            atypes = (impl.Oid * nparams)(*param_types)

        self._ensure_pgconn()
        if not impl.PQsendPrepare(
            self._pgconn_ptr, name, command, nparams, atypes
        ):
            raise e.OperationalError(
                f"sending query and params failed: {error_message(self)}"
            )

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> None:
        # repurpose this function with a cheeky replacement of query with name,
        # drop the param_types from the result
        args = self._query_params_args(
            name, param_values, None, param_formats, result_format
        )
        args = args[:3] + args[4:]

        self._ensure_pgconn()
        if not impl.PQsendQueryPrepared(*args):
            raise e.OperationalError(
                f"sending prepared query failed: {error_message(self)}"
            )

    def _query_params_args(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> Any:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")

        aparams: Optional[Array[c_char_p]]
        alenghts: Optional[Array[c_int]]
        if param_values:
            nparams = len(param_values)
            aparams = (c_char_p * nparams)(
                *(
                    # convert bytearray/memoryview to bytes
                    # TODO: avoid copy, at least in the C implementation.
                    b
                    if b is None or isinstance(b, bytes)
                    else bytes(b)  # type: ignore[arg-type]
                    for b in param_values
                )
            )
            alenghts = (c_int * nparams)(
                *(len(p) if p else 0 for p in param_values)
            )
        else:
            nparams = 0
            aparams = alenghts = None

        atypes: Optional[Array[impl.Oid]]
        if not param_types:
            atypes = None
        else:
            if len(param_types) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_types))
                )
            atypes = (impl.Oid * nparams)(*param_types)

        if not param_formats:
            aformats = None
        else:
            if len(param_formats) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_formats"
                    % (nparams, len(param_formats))
                )
            aformats = (c_int * nparams)(*param_formats)

        return (
            self._pgconn_ptr,
            command,
            nparams,
            atypes,
            aparams,
            alenghts,
            aformats,
            result_format,
        )

    def prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        if not isinstance(command, bytes):
            raise TypeError(
                f"'command' must be bytes, got {type(command)} instead"
            )

        if not param_types:
            nparams = 0
            atypes = None
        else:
            nparams = len(param_types)
            atypes = (impl.Oid * nparams)(*param_types)

        self._ensure_pgconn()
        rv = impl.PQprepare(self._pgconn_ptr, name, command, nparams, atypes)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def exec_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[bytes]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = 0,
    ) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        aparams: Optional[Array[c_char_p]]
        alenghts: Optional[Array[c_int]]
        if param_values:
            nparams = len(param_values)
            aparams = (c_char_p * nparams)(*param_values)
            alenghts = (c_int * nparams)(
                *(len(p) if p else 0 for p in param_values)
            )
        else:
            nparams = 0
            aparams = alenghts = None

        if not param_formats:
            aformats = None
        else:
            if len(param_formats) != nparams:
                raise ValueError(
                    "got %d param_values but %d param_types"
                    % (nparams, len(param_formats))
                )
            aformats = (c_int * nparams)(*param_formats)

        self._ensure_pgconn()
        rv = impl.PQexecPrepared(
            self._pgconn_ptr,
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

    def describe_prepared(self, name: bytes) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        rv = impl.PQdescribePrepared(self._pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_describe_prepared(self, name: bytes) -> None:
        if not isinstance(name, bytes):
            raise TypeError(f"bytes expected, got {type(name)} instead")
        self._ensure_pgconn()
        if not impl.PQsendDescribePrepared(self._pgconn_ptr, name):
            raise e.OperationalError(
                f"sending describe prepared failed: {error_message(self)}"
            )

    def describe_portal(self, name: bytes) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        rv = impl.PQdescribePortal(self._pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_describe_portal(self, name: bytes) -> None:
        if not isinstance(name, bytes):
            raise TypeError(f"bytes expected, got {type(name)} instead")
        self._ensure_pgconn()
        if not impl.PQsendDescribePortal(self._pgconn_ptr, name):
            raise e.OperationalError(
                f"sending describe portal failed: {error_message(self)}"
            )

    def get_result(self) -> Optional["PGresult"]:
        rv = impl.PQgetResult(self._pgconn_ptr)
        return PGresult(rv) if rv else None

    def consume_input(self) -> None:
        if 1 != impl.PQconsumeInput(self._pgconn_ptr):
            raise e.OperationalError(
                f"consuming input failed: {error_message(self)}"
            )

    def is_busy(self) -> int:
        return impl.PQisBusy(self._pgconn_ptr)

    @property
    def nonblocking(self) -> int:
        return impl.PQisnonblocking(self._pgconn_ptr)

    @nonblocking.setter
    def nonblocking(self, arg: int) -> None:
        if 0 > impl.PQsetnonblocking(self._pgconn_ptr, arg):
            raise e.OperationalError(
                f"setting nonblocking failed: {error_message(self)}"
            )

    def flush(self) -> int:
        if not self._pgconn_ptr:
            raise e.OperationalError(
                "flushing failed: the connection is closed"
            )
        rv: int = impl.PQflush(self._pgconn_ptr)
        if rv < 0:
            raise e.OperationalError(f"flushing failed: {error_message(self)}")
        return rv

    def set_single_row_mode(self) -> None:
        if not impl.PQsetSingleRowMode(self._pgconn_ptr):
            raise e.OperationalError("setting single row mode failed")

    def get_cancel(self) -> "PGcancel":
        """
        Create an object with the information needed to cancel a command.

        See :pq:`PQgetCancel` for details.
        """
        rv = impl.PQgetCancel(self._pgconn_ptr)
        if not rv:
            raise e.OperationalError("couldn't create cancel object")
        return PGcancel(rv)

    def notifies(self) -> Optional[PGnotify]:
        ptr = impl.PQnotifies(self._pgconn_ptr)
        if ptr:
            c = ptr.contents
            return PGnotify(c.relname, c.be_pid, c.extra)
            impl.PQfreemem(ptr)
        else:
            return None

    def put_copy_data(self, buffer: bytes) -> int:
        # TODO: should be done without copy
        if not isinstance(buffer, bytes):
            buffer = bytes(buffer)
        rv = impl.PQputCopyData(self._pgconn_ptr, buffer, len(buffer))
        if rv < 0:
            raise e.OperationalError(
                f"sending copy data failed: {error_message(self)}"
            )
        return rv

    def put_copy_end(self, error: Optional[bytes] = None) -> int:
        rv = impl.PQputCopyEnd(self._pgconn_ptr, error)
        if rv < 0:
            raise e.OperationalError(
                f"sending copy end failed: {error_message(self)}"
            )
        return rv

    def get_copy_data(self, async_: int) -> Tuple[int, memoryview]:
        buffer_ptr = c_char_p()
        nbytes = impl.PQgetCopyData(
            self._pgconn_ptr, byref(buffer_ptr), async_
        )
        if nbytes == -2:
            raise e.OperationalError(
                f"receiving copy data failed: {error_message(self)}"
            )
        if buffer_ptr:
            # TODO: do it without copy
            data = string_at(buffer_ptr, nbytes)
            impl.PQfreemem(buffer_ptr)
            return nbytes, memoryview(data)
        else:
            return nbytes, memoryview(b"")

    def encrypt_password(
        self, passwd: bytes, user: bytes, algorithm: Optional[bytes] = None
    ) -> bytes:
        out = impl.PQencryptPasswordConn(
            self._pgconn_ptr, passwd, user, algorithm
        )
        if not out:
            raise e.OperationalError(
                f"password encryption failed: {error_message(self)}"
            )

        rv = string_at(out)
        impl.PQfreemem(out)
        return rv

    def make_empty_result(self, exec_status: int) -> "PGresult":
        rv = impl.PQmakeEmptyPGresult(self._pgconn_ptr, exec_status)
        if not rv:
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult(rv)

    @property
    def pipeline_status(self) -> int:
        if version() < 140000:
            return 0
        return impl.PQpipelineStatus(self._pgconn_ptr)

    def enter_pipeline_mode(self) -> None:
        """Enter pipeline mode.

        :raises ~e.OperationalError: in case of failure to enter the pipeline
            mode.
        """
        if impl.PQenterPipelineMode(self._pgconn_ptr) != 1:
            raise e.OperationalError("failed to enter pipeline mode")

    def exit_pipeline_mode(self) -> None:
        """Exit pipeline mode.

        :raises ~e.OperationalError: in case of failure to exit the pipeline
            mode.
        """
        if impl.PQexitPipelineMode(self._pgconn_ptr) != 1:
            raise e.OperationalError(error_message(self))

    def pipeline_sync(self) -> None:
        """Mark a synchronization point in a pipeline.

        :raises ~e.OperationalError: if the connection is not in pipeline mode
            or if sync failed.
        """
        rv = impl.PQpipelineSync(self._pgconn_ptr)
        if rv == 0:
            raise e.OperationalError("connection not in pipeline mode")
        if rv != 1:
            raise e.OperationalError("failed to sync pipeline")

    def send_flush_request(self) -> None:
        """Sends a request for the server to flush its output buffer.

        :raises ~e.OperationalError: if the flush request failed.
        """
        if impl.PQsendFlushRequest(self._pgconn_ptr) == 0:
            raise e.OperationalError(
                f"flush request failed: {error_message(self)}"
            )

    def _call_bytes(
        self, func: Callable[[impl.PGconn_struct], Optional[bytes]]
    ) -> bytes:
        """
        Call one of the pgconn libpq functions returning a bytes pointer.
        """
        if not self._pgconn_ptr:
            raise e.OperationalError("the connection is closed")
        rv = func(self._pgconn_ptr)
        assert rv is not None
        return rv

    def _call_int(self, func: Callable[[impl.PGconn_struct], int]) -> int:
        """
        Call one of the pgconn libpq functions returning an int.
        """
        if not self._pgconn_ptr:
            raise e.OperationalError("the connection is closed")
        return func(self._pgconn_ptr)

    def _call_bool(self, func: Callable[[impl.PGconn_struct], int]) -> bool:
        """
        Call one of the pgconn libpq functions returning a logical value.
        """
        if not self._pgconn_ptr:
            raise e.OperationalError("the connection is closed")
        return bool(func(self._pgconn_ptr))

    def _ensure_pgconn(self) -> None:
        if not self._pgconn_ptr:
            raise e.OperationalError("the connection is closed")


class PGresult:
    """
    Python representation of a libpq result.
    """

    __module__ = "psycopg.pq"
    __slots__ = ("_pgresult_ptr",)

    def __init__(self, pgresult_ptr: impl.PGresult_struct):
        self._pgresult_ptr: Optional[impl.PGresult_struct] = pgresult_ptr

    def __del__(self) -> None:
        self.clear()

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        status = ExecStatus(self.status)
        return f"<{cls} [{status.name}] at 0x{id(self):x}>"

    def clear(self) -> None:
        self._pgresult_ptr, p = None, self._pgresult_ptr
        if p:
            impl.PQclear(p)

    @property
    def pgresult_ptr(self) -> Optional[int]:
        """The pointer to the underlying ``PGresult`` structure, as integer.

        `!None` if the result was cleared.

        The value can be used to pass the structure to libpq functions which
        psycopg doesn't (currently) wrap, either in C or in Python using FFI
        libraries such as `ctypes`.
        """
        if self._pgresult_ptr is None:
            return None

        return addressof(self._pgresult_ptr.contents)  # type: ignore[attr-defined]

    @property
    def status(self) -> int:
        return impl.PQresultStatus(self._pgresult_ptr)

    @property
    def error_message(self) -> bytes:
        return impl.PQresultErrorMessage(self._pgresult_ptr)

    def error_field(self, fieldcode: int) -> Optional[bytes]:
        return impl.PQresultErrorField(self._pgresult_ptr, fieldcode)

    @property
    def ntuples(self) -> int:
        return impl.PQntuples(self._pgresult_ptr)

    @property
    def nfields(self) -> int:
        return impl.PQnfields(self._pgresult_ptr)

    def fname(self, column_number: int) -> Optional[bytes]:
        return impl.PQfname(self._pgresult_ptr, column_number)

    def ftable(self, column_number: int) -> int:
        return impl.PQftable(self._pgresult_ptr, column_number)

    def ftablecol(self, column_number: int) -> int:
        return impl.PQftablecol(self._pgresult_ptr, column_number)

    def fformat(self, column_number: int) -> int:
        return impl.PQfformat(self._pgresult_ptr, column_number)

    def ftype(self, column_number: int) -> int:
        return impl.PQftype(self._pgresult_ptr, column_number)

    def fmod(self, column_number: int) -> int:
        return impl.PQfmod(self._pgresult_ptr, column_number)

    def fsize(self, column_number: int) -> int:
        return impl.PQfsize(self._pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> int:
        return impl.PQbinaryTuples(self._pgresult_ptr)

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        length: int = impl.PQgetlength(
            self._pgresult_ptr, row_number, column_number
        )
        if length:
            v = impl.PQgetvalue(self._pgresult_ptr, row_number, column_number)
            return string_at(v, length)
        else:
            if impl.PQgetisnull(self._pgresult_ptr, row_number, column_number):
                return None
            else:
                return b""

    @property
    def nparams(self) -> int:
        return impl.PQnparams(self._pgresult_ptr)

    def param_type(self, param_number: int) -> int:
        return impl.PQparamtype(self._pgresult_ptr, param_number)

    @property
    def command_status(self) -> Optional[bytes]:
        return impl.PQcmdStatus(self._pgresult_ptr)

    @property
    def command_tuples(self) -> Optional[int]:
        rv = impl.PQcmdTuples(self._pgresult_ptr)
        return int(rv) if rv else None

    @property
    def oid_value(self) -> int:
        return impl.PQoidValue(self._pgresult_ptr)

    def set_attributes(self, descriptions: List[PGresAttDesc]) -> None:
        structs = [
            impl.PGresAttDesc_struct(*desc)  # type: ignore
            for desc in descriptions
        ]
        array = (impl.PGresAttDesc_struct * len(structs))(*structs)  # type: ignore
        rv = impl.PQsetResultAttrs(self._pgresult_ptr, len(structs), array)
        if rv == 0:
            raise e.OperationalError("PQsetResultAttrs failed")


class PGcancel:
    """
    Token to cancel the current operation on a connection.

    Created by `PGconn.get_cancel()`.
    """

    __module__ = "psycopg.pq"
    __slots__ = ("pgcancel_ptr",)

    def __init__(self, pgcancel_ptr: impl.PGcancel_struct):
        self.pgcancel_ptr: Optional[impl.PGcancel_struct] = pgcancel_ptr

    def __del__(self) -> None:
        self.free()

    def free(self) -> None:
        """
        Free the data structure created by :pq:`PQgetCancel()`.

        Automatically invoked by `!__del__()`.

        See :pq:`PQfreeCancel()` for details.
        """
        self.pgcancel_ptr, p = None, self.pgcancel_ptr
        if p:
            impl.PQfreeCancel(p)

    def cancel(self) -> None:
        """Requests that the server abandon processing of the current command.

        See :pq:`PQcancel()` for details.
        """
        buf = create_string_buffer(256)
        res = impl.PQcancel(
            self.pgcancel_ptr, pointer(buf), len(buf)  # type: ignore
        )
        if not res:
            raise e.OperationalError(
                f"cancel failed: {buf.value.decode('utf8', 'ignore')}"
            )


class Conninfo:
    """
    Utility object to manipulate connection strings.
    """

    __module__ = "psycopg.pq"

    @classmethod
    def get_defaults(cls) -> List[ConninfoOption]:
        opts = impl.PQconndefaults()
        if not opts:
            raise MemoryError("couldn't allocate connection defaults")
        try:
            return cls._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    @classmethod
    def parse(cls, conninfo: bytes) -> List[ConninfoOption]:
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        errmsg = c_char_p()
        rv = impl.PQconninfoParse(conninfo, pointer(errmsg))
        if not rv:
            if not errmsg:
                raise MemoryError("couldn't allocate on conninfo parse")
            else:
                exc = e.OperationalError(
                    (errmsg.value or b"").decode("utf8", "replace")
                )
                impl.PQfreemem(errmsg)
                raise exc

        try:
            return cls._options_from_array(rv)
        finally:
            impl.PQconninfoFree(rv)

    @classmethod
    def _options_from_array(
        cls, opts: Sequence[impl.PQconninfoOption_struct]
    ) -> List[ConninfoOption]:
        rv = []
        skws = "keyword envvar compiled val label dispchar".split()
        for opt in opts:
            if not opt.keyword:
                break
            d = {kw: getattr(opt, kw) for kw in skws}
            d["dispsize"] = opt.dispsize
            rv.append(ConninfoOption(**d))

        return rv


class Escaping:
    """
    Utility object to escape strings for SQL interpolation.
    """

    __module__ = "psycopg.pq"

    def __init__(self, conn: Optional[PGconn] = None):
        self.conn = conn

    def escape_literal(self, data: "abc.Buffer") -> memoryview:
        if not self.conn:
            raise e.OperationalError(
                "escape_literal failed: no connection provided"
            )

        self.conn._ensure_pgconn()
        # TODO: might be done without copy (however C does that)
        if not isinstance(data, bytes):
            data = bytes(data)
        out = impl.PQescapeLiteral(self.conn._pgconn_ptr, data, len(data))
        if not out:
            raise e.OperationalError(
                f"escape_literal failed: {error_message(self.conn)} bytes"
            )
        rv = string_at(out)
        impl.PQfreemem(out)
        return memoryview(rv)

    def escape_identifier(self, data: "abc.Buffer") -> memoryview:
        if not self.conn:
            raise e.OperationalError(
                "escape_identifier failed: no connection provided"
            )

        self.conn._ensure_pgconn()

        if not isinstance(data, bytes):
            data = bytes(data)
        out = impl.PQescapeIdentifier(self.conn._pgconn_ptr, data, len(data))
        if not out:
            raise e.OperationalError(
                f"escape_identifier failed: {error_message(self.conn)} bytes"
            )
        rv = string_at(out)
        impl.PQfreemem(out)
        return memoryview(rv)

    def escape_string(self, data: "abc.Buffer") -> memoryview:
        if not isinstance(data, bytes):
            data = bytes(data)

        if self.conn:
            self.conn._ensure_pgconn()
            error = c_int()
            out = create_string_buffer(len(data) * 2 + 1)
            impl.PQescapeStringConn(
                self.conn._pgconn_ptr,
                pointer(out),  # type: ignore
                data,
                len(data),
                pointer(error),
            )

            if error:
                raise e.OperationalError(
                    f"escape_string failed: {error_message(self.conn)} bytes"
                )

        else:
            out = create_string_buffer(len(data) * 2 + 1)
            impl.PQescapeString(
                pointer(out),  # type: ignore
                data,
                len(data),
            )

        return memoryview(out.value)

    def escape_bytea(self, data: "abc.Buffer") -> memoryview:
        len_out = c_size_t()
        # TODO: might be able to do without a copy but it's a mess.
        # the C library does it better anyway, so maybe not worth optimising
        # https://mail.python.org/pipermail/python-dev/2012-September/121780.html
        if not isinstance(data, bytes):
            data = bytes(data)
        if self.conn:
            self.conn._ensure_pgconn()
            out = impl.PQescapeByteaConn(
                self.conn._pgconn_ptr,
                data,
                len(data),
                pointer(t_cast(c_ulong, len_out)),
            )
        else:
            out = impl.PQescapeBytea(
                data, len(data), pointer(t_cast(c_ulong, len_out))
            )
        if not out:
            raise MemoryError(
                f"couldn't allocate for escape_bytea of {len(data)} bytes"
            )

        rv = string_at(out, len_out.value - 1)  # out includes final 0
        impl.PQfreemem(out)
        return memoryview(rv)

    def unescape_bytea(self, data: bytes) -> memoryview:
        # not needed, but let's keep it symmetric with the escaping:
        # if a connection is passed in, it must be valid.
        if self.conn:
            self.conn._ensure_pgconn()

        len_out = c_size_t()
        out = impl.PQunescapeBytea(data, pointer(t_cast(c_ulong, len_out)))
        if not out:
            raise MemoryError(
                f"couldn't allocate for unescape_bytea of {len(data)} bytes"
            )

        rv = string_at(out, len_out.value)
        impl.PQfreemem(out)
        return memoryview(rv)


# importing the ssl module sets up Python's libcrypto callbacks
import ssl  # noqa

# disable libcrypto setup in libpq, so it won't stomp on the callbacks
# that have already been set up
impl.PQinitOpenSSL(1, 0)
