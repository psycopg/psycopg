# mypy: ignore-errors

"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import sys
import logging
from os import getpid
from ctypes import POINTER, Array, addressof, byref, c_char_p, c_int, c_size_t, c_ulong
from ctypes import c_void_p, cast, create_string_buffer, py_object, string_at
from typing import TYPE_CHECKING, Any
from typing import cast as t_cast
from weakref import ref
from collections.abc import Callable, Sequence

from . import _pq_ctypes as impl
from .. import errors as e
from .misc import ConninfoOption, PGnotify, PGresAttDesc, _clean_error_message
from .misc import connection_summary
from ._enums import ConnStatus, ExecStatus, Format, Trace
from .._encodings import pg2pyenc

if TYPE_CHECKING:
    from . import abc

__impl__ = "python"

logger = logging.getLogger("psycopg")

OK = ConnStatus.OK

# note_on_del: functions called on __del__ are imported as local values to
# avoid warnings on interpreter shutdown in case the module is gc'd before the
# object is destroyed.


def version() -> int:
    """Return the version number of the libpq currently loaded.

    The number is in the same format of `~psycopg.ConnectionInfo.server_version`.

    Certain features might not be available if the libpq library used is too old.
    """
    return impl.PQlibVersion()


@impl.PQnoticeReceiver  # type: ignore
def notice_receiver(arg: c_void_p, result_ptr: impl.PGresult_struct) -> None:
    pgconn = cast(arg, POINTER(py_object)).contents.value
    if callable(pgconn):  # Not a weak reference on PyPy.
        pgconn = pgconn()

    if not (pgconn and pgconn.notice_handler):
        return

    res = PGresult(result_ptr)
    try:
        pgconn.notice_handler(res)
    except Exception as exc:
        logger.exception("error in notice receiver: %s", exc)
    finally:
        res._pgresult_ptr = None  # avoid destroying the pgresult_ptr


class PGconn:
    """
    Python representation of a libpq connection.
    """

    __slots__ = (
        "_pgconn_ptr",
        "notice_handler",
        "notify_handler",
        "_self_ptr",
        "_procpid",
        "__weakref__",
    )

    def __init__(self, pgconn_ptr: impl.PGconn_struct):
        self._pgconn_ptr: impl.PGconn_struct | None = pgconn_ptr
        self.notice_handler: Callable[[abc.PGresult], None] | None = None
        self.notify_handler: Callable[[PGnotify], None] | None = None

        # Keep alive for the lifetime of PGconn
        self._self_ptr = py_object(ref(self))
        impl.PQsetNoticeReceiver(pgconn_ptr, notice_receiver, byref(self._self_ptr))

        self._procpid = getpid()

    def __del__(self, __getpid: Callable[[], int] = getpid) -> None:
        # Close the connection only if it was created in this process,
        # not if this object is being GC'd after fork.
        if __getpid() == self._procpid:  # see note_on_del
            self.finish()

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = connection_summary(self)
        return f"<{cls} {info} at 0x{id(self):x}>"

    @classmethod
    def connect(cls, conninfo: bytes) -> PGconn:
        """Open a new connection to the database, in a blocking way.

        :param conninfo: the connection string.
        :return: the new connection object. It is returned even if the
            connection failed: check its `status` to verify that it succeeded.

        See :pq:`PQconnectdb()` for details.
        """
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")
        if not (pgconn_ptr := impl.PQconnectdb(conninfo)):
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    @classmethod
    def connect_start(cls, conninfo: bytes) -> PGconn:
        """Start a non-blocking connection to the database.

        :param conninfo: the connection string.
        :return: the new connection object. Call `connect_poll()` on it, waiting
            on its `socket`, until the connection is established or failed.

        See :pq:`PQconnectStart()` for details.
        """
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")
        if not (pgconn_ptr := impl.PQconnectStart(conninfo)):
            raise MemoryError("couldn't allocate PGconn")
        return cls(pgconn_ptr)

    def connect_poll(self) -> int:
        """Advance a non-blocking connection started by `connect_start()`.

        :return: a `PollingStatus` value, telling whether to wait for the
            `socket` to be ready to read or write, or if the connection
            is established or failed.

        See :pq:`PQconnectPoll()` for details.
        """
        return self._call_int(impl.PQconnectPoll)

    def finish(self, __PQfinish: Any = impl.PQfinish) -> None:
        """Close the connection to the server and free the libpq object.

        The method can be called more than once. It is invoked automatically
        when the object is deleted.

        See :pq:`PQfinish()` for details.
        """
        self._pgconn_ptr, p = None, self._pgconn_ptr
        if p:
            __PQfinish(p)

    @property
    def pgconn_ptr(self) -> int | None:
        """The pointer to the underlying `!PGconn` structure, as integer.

        `!None` if the connection is closed.

        The value can be used to pass the structure to libpq functions which
        Psycopg doesn't (currently) wrap, either in C or in Python using FFI
        libraries such as `ctypes`.
        """
        if self._pgconn_ptr is None:
            return None

        return addressof(self._pgconn_ptr.contents)  # type: ignore[attr-defined]

    @property
    def info(self) -> list[ConninfoOption]:
        """The connection options used by the connection, as `ConninfoOption`
        objects.

        See :pq:`PQconninfo()` for details.
        """
        self._ensure_pgconn()
        if not (opts := impl.PQconninfo(self._pgconn_ptr)):
            raise MemoryError("couldn't allocate connection info")
        try:
            return Conninfo._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    def reset(self) -> None:
        """Reset the communication channel to the server, in a blocking way.

        The connection is closed and opened again, using the same parameters.

        See :pq:`PQreset()` for details.
        """
        self._ensure_pgconn()
        impl.PQreset(self._pgconn_ptr)

    def reset_start(self) -> None:
        """Start a non-blocking reset of the connection to the server.

        Call `reset_poll()` to complete the operation, as for
        `connect_start()` and `connect_poll()`.

        :raises ~psycopg.OperationalError: if the reset could not start.

        See :pq:`PQresetStart()` for details.
        """
        if not impl.PQresetStart(self._pgconn_ptr):
            raise e.OperationalError("couldn't reset connection")

    def reset_poll(self) -> int:
        """Advance a non-blocking reset started by `reset_start()`.

        :return: a `PollingStatus` value.

        See :pq:`PQresetPoll()` for details.
        """
        return self._call_int(impl.PQresetPoll)

    @classmethod
    def ping(self, conninfo: bytes) -> int:
        """Report the status of the server, without opening a connection.

        :param conninfo: the connection string.
        :return: a `Ping` value.

        See :pq:`PQping()` for details.
        """
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        return impl.PQping(conninfo)

    @property
    def db(self) -> bytes:
        """The database name of the connection.

        See :pq:`PQdb()` for details.
        """
        return self._call_bytes(impl.PQdb)

    @property
    def user(self) -> bytes:
        """The user name of the connection.

        See :pq:`PQuser()` for details.
        """
        return self._call_bytes(impl.PQuser)

    @property
    def password(self) -> bytes:
        """The password of the connection.

        See :pq:`PQpass()` for details.
        """
        return self._call_bytes(impl.PQpass)

    @property
    def host(self) -> bytes:
        """The server host name of the active connection.

        It can be a host name, an IP address, or a directory path, if the
        connection is through a Unix socket.

        See :pq:`PQhost()` for details.
        """
        return self._call_bytes(impl.PQhost)

    @property
    def hostaddr(self) -> bytes:
        """The server IP address of the active connection.

        It is empty if the connection is through a Unix socket.

        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 12.

        See :pq:`PQhostaddr()` for details.
        """
        return self._call_bytes(impl.PQhostaddr)

    @property
    def port(self) -> bytes:
        """The port of the active connection.

        See :pq:`PQport()` for details.
        """
        return self._call_bytes(impl.PQport)

    @property
    def tty(self) -> bytes:
        """The debug TTY of the connection.

        The server no longer uses this setting, so the value is always empty.

        See :pq:`PQtty()` for details.
        """
        return self._call_bytes(impl.PQtty)

    @property
    def options(self) -> bytes:
        """The command-line options passed in the connection request.

        See :pq:`PQoptions()` for details.
        """
        return self._call_bytes(impl.PQoptions)

    @property
    def status(self, __PQstatus: Any = impl.PQstatus) -> int:
        """The status of the connection, as a `ConnStatus` value.

        See :pq:`PQstatus()` for details.
        """
        return __PQstatus(self._pgconn_ptr)

    @property
    def transaction_status(self) -> int:
        """The current in-transaction status of the server, as a
        `TransactionStatus` value.

        See :pq:`PQtransactionStatus()` for details.
        """
        return impl.PQtransactionStatus(self._pgconn_ptr)

    def parameter_status(self, name: bytes) -> bytes | None:
        """Look up a current parameter setting of the server.

        :param name: the name of the parameter, for instance
            ``b"server_version"`` or ``b"client_encoding"``.
        :return: the value of the parameter, or `!None` if the server
            didn't report it.

        See :pq:`PQparameterStatus()` for details.
        """
        self._ensure_pgconn()
        return impl.PQparameterStatus(self._pgconn_ptr, name)

    @property
    def error_message(self) -> bytes:
        """The error message most recently generated by an operation on the
        connection.

        The message is empty if there is no error. It can be multi-line, and
        usually has a trailing newline.

        See :pq:`PQerrorMessage()` for details.
        """
        return impl.PQerrorMessage(self._pgconn_ptr)

    def get_error_message(self, encoding: str = "") -> str:
        """Return the `error_message` as a string, cleaned up for display.

        :param encoding: the encoding to decode the message with. By default,
            the connection client encoding.
        """
        return _clean_error_message(self.error_message, encoding or self._encoding)

    @property
    def _encoding(self) -> str:
        if self.status == OK:
            pgenc = self.parameter_status(b"client_encoding") or b"UTF8"
            return pg2pyenc(pgenc)
        else:
            return "utf-8"

    @property
    def protocol_version(self) -> int:
        """The frontend/backend protocol major version, as an integer.

        It is currently always 3.

        See :pq:`PQprotocolVersion()` for details.
        """
        return self._call_int(impl.PQprotocolVersion)

    @property
    def full_protocol_version(self) -> int:
        """The frontend/backend protocol version, including the minor version.

        The number is the major version multiplied by 10000, plus the minor
        version. For instance, protocol 3.2 is returned as 30002.

        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 18.

        See :pq:`PQfullProtocolVersion()` for details.
        """
        return self._call_int(impl.PQfullProtocolVersion)

    @property
    def server_version(self) -> int:
        """The version of the server, as an integer.

        The number is in the same format of
        `~psycopg.ConnectionInfo.server_version`.

        See :pq:`PQserverVersion()` for details.
        """
        return self._call_int(impl.PQserverVersion)

    @property
    def socket(self) -> int:
        """The file descriptor number of the socket connected to the server.

        :raises ~psycopg.OperationalError: if the connection is lost.

        See :pq:`PQsocket()` for details.
        """
        if (rv := self._call_int(impl.PQsocket)) == -1:
            raise e.OperationalError("the connection is lost")
        return rv

    @property
    def backend_pid(self) -> int:
        """The process ID of the server process handling the connection.

        See :pq:`PQbackendPID()` for details.
        """
        return self._call_int(impl.PQbackendPID)

    @property
    def needs_password(self) -> bool:
        """True if the connection authentication method required a password,
        but none was available.

        See :pq:`PQconnectionNeedsPassword` for details.
        """
        return bool(impl.PQconnectionNeedsPassword(self._pgconn_ptr))

    @property
    def used_password(self) -> bool:
        """True if the connection authentication method used a password.

        See :pq:`PQconnectionUsedPassword` for details.
        """
        return bool(impl.PQconnectionUsedPassword(self._pgconn_ptr))

    @property
    def used_gssapi(self) -> bool:
        """True if the connection authentication method used GSSAPI.

        See :pq:`PQconnectionUsedGSSAPI` for details.
        """
        return bool(impl.PQconnectionUsedGSSAPI(self._pgconn_ptr))

    @property
    def ssl_in_use(self) -> bool:
        """True if the connection uses SSL.

        See :pq:`PQsslInUse()` for details.
        """
        return self._call_bool(impl.PQsslInUse)

    def exec_(self, command: bytes) -> PGresult:
        """Submit a command to the server and wait for the result.

        :param command: the command to execute. It can contain more than one
            statement, separated by semicolons.
        :return: the result of the command. A command failed on the server
            still returns a result: check its `~PGresult.status`.
        :raises ~psycopg.OperationalError: if the command could not be sent
            to the server.

        See :pq:`PQexec()` for details.
        """
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        self._ensure_pgconn()
        if not (rv := impl.PQexec(self._pgconn_ptr, command)):
            raise e.OperationalError(
                f"executing query failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def send_query(self, command: bytes) -> None:
        """Submit a command to the server without waiting for the result.

        Call `get_result()` to retrieve the results.

        :param command: the command to execute.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQsendQuery()` for details.
        """
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        self._ensure_pgconn()
        if not impl.PQsendQuery(self._pgconn_ptr, command):
            raise e.OperationalError(
                f"sending query failed: {self.get_error_message()}"
            )

    def exec_params(
        self,
        command: bytes,
        param_values: Sequence[abc.Buffer | None] | None,
        param_types: Sequence[int] | None = None,
        param_formats: Sequence[int] | None = None,
        result_format: int = Format.TEXT,
    ) -> PGresult:
        """Submit a command to the server, with separate parameters, and wait for
        the result.

        :param command: the command to execute, with placeholders ``$1``,
            ``$2``... for the parameters. Only one statement is allowed.
        :param param_values: the values of the parameters, as `!bytes` or
            other buffer objects. Use `!None` for a :sql:`NULL` value.
        :param param_types: the OIDs of the types of the parameters. If omitted,
            or if an OID is 0, the server infers the type.
        :param param_formats: the `Format` of each parameter. If omitted, all
            the parameters are in text format.
        :param result_format: the `Format` requested for the results.
        :return: the result of the command. A command failed on the server
            still returns a result: check its `~PGresult.status`.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQexecParams()` for details.
        """
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        self._ensure_pgconn()
        if not (rv := impl.PQexecParams(*args)):
            raise e.OperationalError(
                f"executing query failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def send_query_params(
        self,
        command: bytes,
        param_values: Sequence[abc.Buffer | None] | None,
        param_types: Sequence[int] | None = None,
        param_formats: Sequence[int] | None = None,
        result_format: int = Format.TEXT,
    ) -> None:
        """Submit a command to the server, with separate parameters, without
        waiting for the result.

        Call `get_result()` to retrieve the results.

        :param command: the command to execute, with placeholders ``$1``,
            ``$2``... for the parameters.
        :param param_values: the values of the parameters, as `!bytes` or
            other buffer objects. Use `!None` for a :sql:`NULL` value.
        :param param_types: the OIDs of the types of the parameters. If omitted,
            or if an OID is 0, the server infers the type.
        :param param_formats: the `Format` of each parameter. If omitted, all
            the parameters are in text format.
        :param result_format: the `Format` requested for the results.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQsendQueryParams()` for details.
        """
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        self._ensure_pgconn()
        if not impl.PQsendQueryParams(*args):
            raise e.OperationalError(
                f"sending query and params failed: {self.get_error_message()}"
            )

    def send_prepare(
        self, name: bytes, command: bytes, param_types: Sequence[int] | None = None
    ) -> None:
        """Create a prepared statement without waiting for completion.

        Call `get_result()` to retrieve the results. The parameters are the
        same as `prepare()`.

        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQsendPrepare()` for details.
        """
        atypes: Array[impl.Oid] | None
        if not param_types:
            nparams = 0
            atypes = None
        else:
            nparams = len(param_types)
            atypes = (impl.Oid * nparams)(*param_types)

        self._ensure_pgconn()
        if not impl.PQsendPrepare(self._pgconn_ptr, name, command, nparams, atypes):
            raise e.OperationalError(
                f"sending query and params failed: {self.get_error_message()}"
            )

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Sequence[abc.Buffer | None] | None,
        param_formats: Sequence[int] | None = None,
        result_format: int = Format.TEXT,
    ) -> None:
        # repurpose this function with a cheeky replacement of query with name,
        # drop the param_types from the result
        """Execute a prepared statement without waiting for the result.

        Call `get_result()` to retrieve the results.

        :param name: the name of the prepared statement.
        :param param_values: the values of the parameters, as `!bytes` or
            other buffer objects. Use `!None` for a :sql:`NULL` value.
        :param param_formats: the `Format` of each parameter. If omitted, all
            the parameters are in text format.
        :param result_format: the `Format` requested for the results.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQsendQueryPrepared()` for details.
        """
        args = self._query_params_args(
            name, param_values, None, param_formats, result_format
        )
        args = args[:3] + args[4:]

        self._ensure_pgconn()
        if not impl.PQsendQueryPrepared(*args):
            raise e.OperationalError(
                f"sending prepared query failed: {self.get_error_message()}"
            )

    def _query_params_args(
        self,
        command: bytes,
        param_values: Sequence[abc.Buffer | None] | None,
        param_types: Sequence[int] | None = None,
        param_formats: Sequence[int] | None = None,
        result_format: int = Format.TEXT,
    ) -> Any:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")

        aparams: Array[c_char_p] | None
        alenghts: Array[c_int] | None
        if param_values:
            nparams = len(param_values)
            aparams = (c_char_p * nparams)(
                *(
                    # convert bytearray/memoryview to bytes
                    b if b is None or isinstance(b, bytes) else bytes(b)
                    for b in param_values
                )
            )
            alenghts = (c_int * nparams)(*(len(p) if p else 0 for p in param_values))
        else:
            nparams = 0
            aparams = alenghts = None

        atypes: Array[impl.Oid] | None
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
        self, name: bytes, command: bytes, param_types: Sequence[int] | None = None
    ) -> PGresult:
        """Create a prepared statement and wait for completion.

        :param name: the name of the statement. ``b""`` is the unnamed
            statement.
        :param command: the command to prepare, with placeholders ``$1``,
            ``$2``... for the parameters.
        :param param_types: the OIDs of the types of the parameters. If omitted,
            or if an OID is 0, the server infers the type.
        :return: the result of the command. A command failed on the server
            still returns a result: check its `~PGresult.status`.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQprepare()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        if not isinstance(command, bytes):
            raise TypeError(f"'command' must be bytes, got {type(command)} instead")

        if not param_types:
            nparams = 0
            atypes = None
        else:
            nparams = len(param_types)
            atypes = (impl.Oid * nparams)(*param_types)

        self._ensure_pgconn()
        if not (rv := impl.PQprepare(self._pgconn_ptr, name, command, nparams, atypes)):
            raise e.OperationalError(
                f"preparing query failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def exec_prepared(
        self,
        name: bytes,
        param_values: Sequence[abc.Buffer] | None,
        param_formats: Sequence[int] | None = None,
        result_format: int = 0,
    ) -> PGresult:
        """Execute a prepared statement and wait for the result.

        :param name: the name of the prepared statement.
        :param param_values: the values of the parameters, as `!bytes` or
            other buffer objects. Use `!None` for a :sql:`NULL` value.
        :param param_formats: the `Format` of each parameter. If omitted, all
            the parameters are in text format.
        :param result_format: the `Format` requested for the results.
        :return: the result of the command. A command failed on the server
            still returns a result: check its `~PGresult.status`.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQexecPrepared()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        aparams: Array[c_char_p] | None
        alenghts: Array[c_int] | None
        if param_values:
            nparams = len(param_values)
            aparams = (c_char_p * nparams)(
                *(
                    # convert bytearray/memoryview to bytes
                    b if b is None or isinstance(b, bytes) else bytes(b)
                    for b in param_values
                )
            )
            alenghts = (c_int * nparams)(*(len(p) if p else 0 for p in param_values))
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
            raise e.OperationalError(
                f"executing prepared query failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def describe_prepared(self, name: bytes) -> PGresult:
        """Obtain information about a prepared statement and wait for the result.

        :param name: the name of the statement. ``b""`` is the unnamed
            statement.
        :return: a result with no rows, describing the parameters (see
            `~PGresult.nparams`) and the columns of the statement.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQdescribePrepared()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        if not (rv := impl.PQdescribePrepared(self._pgconn_ptr, name)):
            raise e.OperationalError(
                f"describe prepared failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def send_describe_prepared(self, name: bytes) -> None:
        """Obtain information about a prepared statement without waiting for the
        result.

        Call `get_result()` to retrieve the results.

        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQsendDescribePrepared()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"bytes expected, got {type(name)} instead")
        self._ensure_pgconn()
        if not impl.PQsendDescribePrepared(self._pgconn_ptr, name):
            raise e.OperationalError(
                f"sending describe prepared failed: {self.get_error_message()}"
            )

    def describe_portal(self, name: bytes) -> PGresult:
        """Obtain information about a portal and wait for the result.

        :param name: the name of the portal, for instance the name of a
            cursor declared with :sql:`DECLARE`. ``b""`` is the unnamed portal.
        :return: a result with no rows, describing the columns of the portal.
        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQdescribePortal()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        if not (rv := impl.PQdescribePortal(self._pgconn_ptr, name)):
            raise e.OperationalError(
                f"describe portal failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def send_describe_portal(self, name: bytes) -> None:
        """Obtain information about a portal without waiting for the result.

        Call `get_result()` to retrieve the results.

        :raises ~psycopg.OperationalError: if the command could not be sent.

        See :pq:`PQsendDescribePortal()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"bytes expected, got {type(name)} instead")
        self._ensure_pgconn()
        if not impl.PQsendDescribePortal(self._pgconn_ptr, name):
            raise e.OperationalError(
                f"sending describe portal failed: {self.get_error_message()}"
            )

    def close_prepared(self, name: bytes) -> PGresult:
        """Close a prepared statement and wait for completion.

        :param name: the name of the statement. ``b""`` is the unnamed
            statement.
        :return: the result of the command. A command failed on the server
            still returns a result: check its `~PGresult.status`.
        :raises ~psycopg.OperationalError: if the command could not be sent.
        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 17.

        See :pq:`PQclosePrepared()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        if not (rv := impl.PQclosePrepared(self._pgconn_ptr, name)):
            raise e.OperationalError(
                f"close prepared failed: {self.get_error_message()}"
            )
        return PGresult(rv)

    def send_close_prepared(self, name: bytes) -> None:
        """Close a prepared statement without waiting for completion.

        Call `get_result()` to retrieve the results.

        :raises ~psycopg.OperationalError: if the command could not be sent.
        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 17.

        See :pq:`PQsendClosePrepared()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"bytes expected, got {type(name)} instead")
        self._ensure_pgconn()
        if not impl.PQsendClosePrepared(self._pgconn_ptr, name):
            raise e.OperationalError(
                f"sending close prepared failed: {self.get_error_message()}"
            )

    def close_portal(self, name: bytes) -> PGresult:
        """Close a portal and wait for completion.

        :param name: the name of the portal. ``b""`` is the unnamed portal.
        :return: the result of the command. A command failed on the server
            still returns a result: check its `~PGresult.status`.
        :raises ~psycopg.OperationalError: if the command could not be sent.
        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 17.

        See :pq:`PQclosePortal()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        self._ensure_pgconn()
        if not (rv := impl.PQclosePortal(self._pgconn_ptr, name)):
            raise e.OperationalError(f"close portal failed: {self.get_error_message()}")
        return PGresult(rv)

    def send_close_portal(self, name: bytes) -> None:
        """Close a portal without waiting for completion.

        Call `get_result()` to retrieve the results.

        :raises ~psycopg.OperationalError: if the command could not be sent.
        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 17.

        See :pq:`PQsendClosePortal()` for details.
        """
        if not isinstance(name, bytes):
            raise TypeError(f"bytes expected, got {type(name)} instead")
        self._ensure_pgconn()
        if not impl.PQsendClosePortal(self._pgconn_ptr, name):
            raise e.OperationalError(
                f"sending close portal failed: {self.get_error_message()}"
            )

    def get_result(self) -> PGresult | None:
        """Return the next result of a command sent without waiting.

        The method blocks if the result is not available yet: call
        `consume_input()` and check `is_busy()` to avoid blocking.

        :return: the next result, or `!None` when the command is complete and
            there are no more results.

        See :pq:`PQgetResult()` for details.
        """
        rv = impl.PQgetResult(self._pgconn_ptr)
        return PGresult(rv) if rv else None

    def consume_input(self) -> None:
        """Read the data available from the server, if any.

        Call it when the `socket` is ready to read, then check `is_busy()` and
        `notifies()` to process what was received.

        :raises ~psycopg.OperationalError: if reading failed.

        See :pq:`PQconsumeInput()` for details.
        """
        if 1 != impl.PQconsumeInput(self._pgconn_ptr):
            raise e.OperationalError(
                f"consuming input failed: {self.get_error_message()}"
            )

    def is_busy(self) -> int:
        """Return 1 if a command is busy, meaning that `get_result()` would block.

        Return 0 if `get_result()` can be called without blocking.

        See :pq:`PQisBusy()` for details.
        """
        return impl.PQisBusy(self._pgconn_ptr)

    @property
    def nonblocking(self) -> int:
        """The non-blocking status of the connection, as an integer.

        1 if the connection is in non-blocking mode, 0 if it is blocking. Set it
        to change the mode. In non-blocking mode, sending a command doesn't
        wait until all the data is sent: call `flush()` to complete it.

        :raises ~psycopg.OperationalError: if setting the mode failed.

        See :pq:`PQsetnonblocking()` and :pq:`PQisnonblocking()` for details.
        """
        return impl.PQisnonblocking(self._pgconn_ptr)

    @nonblocking.setter
    def nonblocking(self, arg: int) -> None:
        if 0 > impl.PQsetnonblocking(self._pgconn_ptr, arg):
            raise e.OperationalError(
                f"setting nonblocking failed: {self.get_error_message()}"
            )

    def flush(self) -> int:
        # PQflush segfaults if it receives a NULL connection
        """Try to send the data queued for the server.

        :return: 0 if all the data was sent, or there was no data to send. 1 if
            not all the data could be sent yet: wait for the `socket` to be
            ready to write and call `!flush()` again.
        :raises ~psycopg.OperationalError: if sending failed, or the connection
            is closed.

        See :pq:`PQflush()` for details.
        """
        if not self._pgconn_ptr:
            raise e.OperationalError("flushing failed: the connection is closed")
        rv: int = impl.PQflush(self._pgconn_ptr)
        if rv < 0:
            raise e.OperationalError(f"flushing failed: {self.get_error_message()}")
        return rv

    def set_single_row_mode(self) -> None:
        """Return the results of the command just sent one row at a time.

        Call it right after `send_query()` or a similar method, before any call
        to `get_result()`.

        :raises ~psycopg.OperationalError: if the mode could not be set.

        See :pq:`PQsetSingleRowMode()` for details.
        """
        if not impl.PQsetSingleRowMode(self._pgconn_ptr):
            raise e.OperationalError("setting single row mode failed")

    def set_chunked_rows_mode(self, size: int) -> None:
        """Return the results of the command just sent in chunks of rows.

        Call it right after `send_query()` or a similar method, before any call
        to `get_result()`.

        :param size: the maximum number of rows in each result.
        :raises ~psycopg.OperationalError: if the mode could not be set.
        :raises ~psycopg.NotSupportedError: if the libpq version is older
            than 17.

        See :pq:`PQsetChunkedRowsMode()` for details.
        """
        if not impl.PQsetChunkedRowsMode(self._pgconn_ptr, size):
            raise e.OperationalError("setting chunked rows mode failed")

    def cancel_conn(self) -> PGcancelConn:
        """
        Create a connection over which a cancel request can be sent.

        See :pq:`PQcancelCreate` for details.
        """
        if not (rv := impl.PQcancelCreate(self._pgconn_ptr)):
            raise e.OperationalError("couldn't create cancelConn object")
        return PGcancelConn(rv)

    def get_cancel(self) -> PGcancel:
        """
        Create an object with the information needed to cancel a command.

        See :pq:`PQgetCancel` for details.
        """
        if not (rv := impl.PQgetCancel(self._pgconn_ptr)):
            raise e.OperationalError("couldn't create cancel object")
        return PGcancel(rv)

    def notifies(self) -> PGnotify | None:
        """Return the next notification received from the server, if any.

        Notifications are read by `consume_input()` or by the methods
        waiting for a result.

        :return: the next notification, or `!None` if there are no more.

        See :pq:`PQnotifies()` for details.
        """
        if ptr := impl.PQnotifies(self._pgconn_ptr):
            c = ptr.contents
            rv = PGnotify(c.relname, c.be_pid, c.extra)
            impl.PQfreemem(ptr)
            return rv
        else:
            return None

    def put_copy_data(self, buffer: abc.Buffer) -> int:
        """Send data to the server during :sql:`COPY FROM STDIN`.

        :param buffer: the data to send.
        :return: 1 if the data was queued. 0 if it was not queued because the
            connection is non-blocking and its buffers are full: wait for the
            `socket` to be ready to write and try again.
        :raises ~psycopg.OperationalError: if sending failed.

        See :pq:`PQputCopyData()` for details.
        """
        if not isinstance(buffer, bytes):
            buffer = bytes(buffer)
        if (rv := impl.PQputCopyData(self._pgconn_ptr, buffer, len(buffer))) < 0:
            raise e.OperationalError(
                f"sending copy data failed: {self.get_error_message()}"
            )
        return rv

    def put_copy_end(self, error: bytes | None = None) -> int:
        """Notify the server that :sql:`COPY FROM STDIN` is finished.

        :param error: if not `!None`, the :sql:`COPY` fails, with this error
            message.
        :return: 1 if the termination message was queued. 0 if it was not
            queued because the connection is non-blocking and its buffers are
            full: try again later.
        :raises ~psycopg.OperationalError: if sending failed.

        See :pq:`PQputCopyEnd()` for details.
        """
        if (rv := impl.PQputCopyEnd(self._pgconn_ptr, error)) < 0:
            raise e.OperationalError(
                f"sending copy end failed: {self.get_error_message()}"
            )
        return rv

    def get_copy_data(self, async_: int) -> tuple[int, memoryview]:
        """Receive data from the server during :sql:`COPY TO STDOUT`.

        :param async\\_: if 0, wait until a row is available. If 1, return
            immediately if no data is available.
        :return: a tuple with the number of bytes received and the data. The
            number is 0 if no data is available yet (only if `!async_` is 1),
            and -1 when the :sql:`COPY` is finished: call `get_result()` to
            receive its result.
        :raises ~psycopg.OperationalError: if receiving failed.

        See :pq:`PQgetCopyData()` for details.
        """
        buffer_ptr = c_char_p()
        nbytes = impl.PQgetCopyData(self._pgconn_ptr, byref(buffer_ptr), async_)
        if nbytes == -2:
            raise e.OperationalError(
                f"receiving copy data failed: {self.get_error_message()}"
            )
        if buffer_ptr:
            # TODO: do it without copy
            data = string_at(buffer_ptr, nbytes)
            impl.PQfreemem(buffer_ptr)
            return nbytes, memoryview(data)
        else:
            return nbytes, memoryview(b"")

    def trace(self, fileno: int) -> None:
        """
        Enable tracing of the client/server communication to a file stream.

        See :pq:`PQtrace` for details.
        """
        if sys.platform != "linux":
            raise e.NotSupportedError("currently only supported on Linux")
        stream = impl.fdopen(fileno, b"w")
        impl.PQtrace(self._pgconn_ptr, stream)

    def set_trace_flags(self, flags: Trace) -> None:
        """
        Configure tracing behavior of client/server communication.

        :param flags: operating mode of tracing.

        See :pq:`PQsetTraceFlags` for details.
        """
        impl.PQsetTraceFlags(self._pgconn_ptr, flags)

    def untrace(self) -> None:
        """
        Disable tracing, previously enabled through `trace()`.

        See :pq:`PQuntrace` for details.
        """
        impl.PQuntrace(self._pgconn_ptr)

    def encrypt_password(
        self, passwd: bytes, user: bytes, algorithm: bytes | None = None
    ) -> bytes:
        """
        Return the encrypted form of a PostgreSQL password.

        See :pq:`PQencryptPasswordConn` for details.
        """
        out = impl.PQencryptPasswordConn(self._pgconn_ptr, passwd, user, algorithm)
        if not out:
            raise e.OperationalError(
                f"password encryption failed: {self.get_error_message()}"
            )

        rv = string_at(out)
        impl.PQfreemem(out)
        return rv

    def change_password(self, user: bytes, passwd: bytes) -> None:
        """
        Change a PostgreSQL password.

        :raises ~psycopg.OperationalError: if the command to change password failed.

        See :pq:`PQchangePassword` for details.
        """
        res = impl.PQchangePassword(self._pgconn_ptr, user, passwd)
        if impl.PQresultStatus(res) != ExecStatus.COMMAND_OK:
            raise e.OperationalError(
                f"failed to change password change command: {self.get_error_message()}"
            )

    def make_empty_result(self, exec_status: int) -> PGresult:
        """Create an empty result, with the given status.

        :param exec_status: the `ExecStatus` of the result.

        See :pq:`PQmakeEmptyPGresult()` for details.
        """
        if not (rv := impl.PQmakeEmptyPGresult(self._pgconn_ptr, exec_status)):
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult(rv)

    @property
    def pipeline_status(self) -> int:
        """The current pipeline mode status, as a `PipelineStatus` value.

        It is always `~PipelineStatus.OFF` if the libpq version is older
        than 14.

        See :pq:`PQpipelineStatus()` for details.
        """
        if version() < 140000:
            return 0
        return impl.PQpipelineStatus(self._pgconn_ptr)

    def enter_pipeline_mode(self) -> None:
        """Enter pipeline mode.

        :raises ~psycopg.OperationalError: in case of failure to enter the pipeline
            mode.
        """
        if impl.PQenterPipelineMode(self._pgconn_ptr) != 1:
            raise e.OperationalError("failed to enter pipeline mode")

    def exit_pipeline_mode(self) -> None:
        """Exit pipeline mode.

        :raises ~psycopg.OperationalError: in case of failure to exit the pipeline
            mode.
        """
        if impl.PQexitPipelineMode(self._pgconn_ptr) != 1:
            raise e.OperationalError(self.get_error_message())

    def pipeline_sync(self) -> None:
        """Mark a synchronization point in a pipeline.

        :raises ~psycopg.OperationalError: if the connection is not in pipeline mode
            or if sync failed.
        """

        if (rv := impl.PQpipelineSync(self._pgconn_ptr)) == 0:
            raise e.OperationalError("connection not in pipeline mode")
        if rv != 1:
            raise e.OperationalError("failed to sync pipeline")

    def send_flush_request(self) -> None:
        """Sends a request for the server to flush its output buffer.

        :raises ~psycopg.OperationalError: if the flush request failed.
        """
        if impl.PQsendFlushRequest(self._pgconn_ptr) == 0:
            raise e.OperationalError(
                f"flush request failed: {self.get_error_message()}"
            )

    def _call_bytes(self, func: Callable[[impl.PGconn_struct], bytes | None]) -> bytes:
        """
        Call one of the pgconn libpq functions returning a bytes pointer.
        """
        if not self._pgconn_ptr:
            raise e.OperationalError("the connection is closed")
        rv = func(self._pgconn_ptr)
        return rv if rv is not None else b""

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

    __slots__ = ("_pgresult_ptr",)

    def __init__(self, pgresult_ptr: impl.PGresult_struct):
        self._pgresult_ptr: impl.PGresult_struct | None = pgresult_ptr

    def __del__(self) -> None:
        self.clear()

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        try:
            status = ExecStatus(self.status).name
        except ValueError:
            status = f"{self.status} (status unknown)"
        return f"<{cls} [{status}] at 0x{id(self):x}>"

    def clear(self, __PQclear: Any = impl.PQclear) -> None:
        """Free the storage associated with the result.

        The method can be called more than once. It is invoked automatically
        when the object is deleted.

        See :pq:`PQclear()` for details.
        """
        self._pgresult_ptr, p = None, self._pgresult_ptr
        if p:
            __PQclear(p)  # see note_on_del

    @property
    def pgresult_ptr(self) -> int | None:
        """The pointer to the underlying `!PGresult` structure, as integer.

        `!None` if the result was cleared.

        The value can be used to pass the structure to libpq functions which
        Psycopg doesn't (currently) wrap, either in C or in Python using FFI
        libraries such as `ctypes`.
        """
        if self._pgresult_ptr is None:
            return None

        return addressof(self._pgresult_ptr.contents)  # type: ignore[attr-defined]

    @property
    def status(self) -> int:
        """The status of the result, as an `ExecStatus` value.

        See :pq:`PQresultStatus()` for details.
        """
        return impl.PQresultStatus(self._pgresult_ptr)

    @property
    def error_message(self) -> bytes:
        """The error message associated with the result.

        The message is empty if there is no error.

        See :pq:`PQresultErrorMessage()` for details.
        """
        return impl.PQresultErrorMessage(self._pgresult_ptr)

    def get_error_message(self, encoding: str = "utf-8") -> str:
        """Return the `error_message` as a string, cleaned up for display.

        :param encoding: the encoding to decode the message with.
        """
        return _clean_error_message(self.error_message, encoding)

    def error_field(self, fieldcode: int) -> bytes | None:
        """Return a field of the error report.

        :param fieldcode: the field to return, as a `DiagnosticField` value.
        :return: the value of the field, or `!None` if the result is not an
            error or doesn't include the field.

        The values are also available, decoded, as the attributes of
        `psycopg.errors.Diagnostic`.

        See :pq:`PQresultErrorField()` for details.
        """
        return impl.PQresultErrorField(self._pgresult_ptr, fieldcode)

    @property
    def ntuples(self) -> int:
        """The number of rows in the result.

        See :pq:`PQntuples()` for details.
        """
        return impl.PQntuples(self._pgresult_ptr)

    @property
    def nfields(self) -> int:
        """The number of columns in the result.

        See :pq:`PQnfields()` for details.
        """
        return impl.PQnfields(self._pgresult_ptr)

    def fname(self, column_number: int) -> bytes | None:
        """Return the name of a column.

        :param column_number: the column number, starting from 0.
        :return: the name, or `!None` if the column number is out of range.

        See :pq:`PQfname()` for details.
        """
        return impl.PQfname(self._pgresult_ptr, column_number)

    def ftable(self, column_number: int) -> int:
        """Return the OID of the table a column was fetched from.

        :param column_number: the column number, starting from 0.
        :return: the OID, or 0 if the column is not a simple reference to a
            table column.

        See :pq:`PQftable()` for details.
        """
        return impl.PQftable(self._pgresult_ptr, column_number)

    def ftablecol(self, column_number: int) -> int:
        """Return the number of the table column a column was fetched from.

        :param column_number: the column number, starting from 0.
        :return: the table column number, starting from 1, or 0 if the column
            is not a simple reference to a table column.

        See :pq:`PQftablecol()` for details.
        """
        return impl.PQftablecol(self._pgresult_ptr, column_number)

    def fformat(self, column_number: int) -> int:
        """Return the format of a column, as a `Format` value.

        :param column_number: the column number, starting from 0.

        See :pq:`PQfformat()` for details.
        """
        return impl.PQfformat(self._pgresult_ptr, column_number)

    def ftype(self, column_number: int) -> int:
        """Return the OID of the data type of a column.

        :param column_number: the column number, starting from 0.

        See :pq:`PQftype()` for details.
        """
        return impl.PQftype(self._pgresult_ptr, column_number)

    def fmod(self, column_number: int) -> int:
        """Return the type modifier of a column.

        :param column_number: the column number, starting from 0.
        :return: the modifier, for instance the length of a :sql:`varchar(n)`
            column, as encoded by the type. -1 if there is no modifier.

        See :pq:`PQfmod()` for details.
        """
        return impl.PQfmod(self._pgresult_ptr, column_number)

    def fsize(self, column_number: int) -> int:
        """Return the size of the server internal representation of a column type.

        :param column_number: the column number, starting from 0.
        :return: the size in bytes. A negative value means that the type has
            variable length.

        See :pq:`PQfsize()` for details.
        """
        return impl.PQfsize(self._pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> int:
        """1 if all the columns of the result are binary, 0 otherwise.

        Prefer `fformat()`, which tells the format of each column.

        See :pq:`PQbinaryTuples()` for details.
        """
        return impl.PQbinaryTuples(self._pgresult_ptr)

    def get_value(self, row_number: int, column_number: int) -> bytes | None:
        """Return the value of a field of the result.

        :param row_number: the row number, starting from 0.
        :param column_number: the column number, starting from 0.
        :return: the value, in the format of the column (see `fformat()`), or
            `!None` if the value is :sql:`NULL`.

        See :pq:`PQgetvalue()` for details.
        """
        length: int = impl.PQgetlength(self._pgresult_ptr, row_number, column_number)
        if length:
            v = impl.PQgetvalue(self._pgresult_ptr, row_number, column_number)
            return string_at(v, length)
        elif impl.PQgetisnull(self._pgresult_ptr, row_number, column_number):
            return None
        else:
            return b""

    @property
    def nparams(self) -> int:
        """The number of parameters of a prepared statement.

        Only meaningful in a result returned by `PGconn.describe_prepared()`.

        See :pq:`PQnparams()` for details.
        """
        return impl.PQnparams(self._pgresult_ptr)

    def param_type(self, param_number: int) -> int:
        """Return the OID of the data type of a statement parameter.

        Only meaningful in a result returned by `PGconn.describe_prepared()`.

        :param param_number: the parameter number, starting from 0.

        See :pq:`PQparamtype()` for details.
        """
        return impl.PQparamtype(self._pgresult_ptr, param_number)

    @property
    def command_status(self) -> bytes | None:
        """The command status tag of the command that generated the result.

        For instance ``b"SELECT 10"`` or ``b"INSERT 0 1"``.

        See :pq:`PQcmdStatus()` for details.
        """
        return impl.PQcmdStatus(self._pgresult_ptr)

    @property
    def command_tuples(self) -> int | None:
        """The number of rows affected by the command, or `!None` if the command
        doesn't report it.

        See :pq:`PQcmdTuples()` for details.
        """
        rv = impl.PQcmdTuples(self._pgresult_ptr)
        return int(rv) if rv else None

    @property
    def oid_value(self) -> int:
        """The OID of the row inserted, if the command was an :sql:`INSERT` of
        exactly one row into a table with OIDs. 0 otherwise.

        See :pq:`PQoidValue()` for details.
        """
        return impl.PQoidValue(self._pgresult_ptr)

    def set_attributes(self, descriptions: list[PGresAttDesc]) -> None:
        """Set the description of the columns of the result.

        Only allowed on a result created by `PGconn.make_empty_result()` and
        without columns yet.

        :param descriptions: the description of each column.
        :raises ~psycopg.OperationalError: if setting the description failed.

        See :pq:`PQsetResultAttrs()` for details.
        """
        structs = [
            impl.PGresAttDesc_struct(*desc) for desc in descriptions  # type: ignore
        ]
        array = (impl.PGresAttDesc_struct * len(structs))(*structs)  # type: ignore

        if impl.PQsetResultAttrs(self._pgresult_ptr, len(structs), array) == 0:
            raise e.OperationalError("PQsetResultAttrs failed")


class PGcancelConn:
    """
    Token to handle non-blocking cancellation requests.

    Created by `PGconn.cancel_conn()`.
    """

    __slots__ = ("pgcancelconn_ptr",)

    def __init__(self, pgcancelconn_ptr: impl.PGcancelConn_struct):
        self.pgcancelconn_ptr: impl.PGcancelConn_struct | None = pgcancelconn_ptr

    def __del__(self) -> None:
        self.finish()

    def start(self) -> None:
        """Requests that the server abandons processing of the current command
        in a non-blocking manner.

        See :pq:`PQcancelStart` for details.
        """
        if not impl.PQcancelStart(self.pgcancelconn_ptr):
            raise e.OperationalError(
                f"couldn't start cancellation: {self.get_error_message()}"
            )

    def blocking(self) -> None:
        """Requests that the server abandons processing of the current command
        in a blocking manner.

        See :pq:`PQcancelBlocking` for details.
        """
        if not impl.PQcancelBlocking(self.pgcancelconn_ptr):
            raise e.OperationalError(
                f"couldn't start cancellation: {self.get_error_message()}"
            )

    def poll(self) -> int:
        """Advance a non-blocking cancel request started by `start()`.

        :return: a `PollingStatus` value.

        See :pq:`PQcancelPoll()` for details.
        """
        self._ensure_pgcancelconn()
        return impl.PQcancelPoll(self.pgcancelconn_ptr)

    @property
    def status(self) -> int:
        """The status of the cancel connection, as a `ConnStatus` value.

        See :pq:`PQcancelStatus()` for details.
        """
        return impl.PQcancelStatus(self.pgcancelconn_ptr)

    @property
    def socket(self) -> int:
        """The file descriptor number of the socket of the cancel connection.

        :raises ~psycopg.OperationalError: if the cancel connection is not open.

        See :pq:`PQcancelSocket()` for details.
        """
        if (rv := impl.PQcancelSocket(self.pgcancelconn_ptr)) == -1:
            raise e.OperationalError("cancel connection not opened")
        return rv

    @property
    def error_message(self) -> bytes:
        """The error message most recently generated by an operation on the
        cancel connection.

        See :pq:`PQcancelErrorMessage()` for details.
        """
        return impl.PQcancelErrorMessage(self.pgcancelconn_ptr)

    def get_error_message(self, encoding: str = "utf-8") -> str:
        """Return the `error_message` as a string, cleaned up for display.

        :param encoding: the encoding to decode the message with.
        """
        return _clean_error_message(self.error_message, encoding)

    def reset(self) -> None:
        """Reset the cancel connection, so that it can be used for a new cancel
        request.

        See :pq:`PQcancelReset()` for details.
        """
        self._ensure_pgcancelconn()
        impl.PQcancelReset(self.pgcancelconn_ptr)

    def finish(self, __PQcancelFinish: Any = impl.PQcancelFinish) -> None:
        """
        Free the data structure created by :pq:`PQcancelCreate()`.

        Automatically invoked by `!__del__()`.

        See :pq:`PQcancelFinish()` for details.
        """
        self.pgcancelconn_ptr, p = None, self.pgcancelconn_ptr
        if p:
            __PQcancelFinish(p)  # see note_on_del

    def _ensure_pgcancelconn(self) -> None:
        if not self.pgcancelconn_ptr:
            raise e.OperationalError("the cancel connection is closed")


class PGcancel:
    """
    Token to cancel the current operation on a connection.

    Created by `PGconn.get_cancel()`.
    """

    __slots__ = ("pgcancel_ptr",)

    def __init__(self, pgcancel_ptr: impl.PGcancel_struct):
        self.pgcancel_ptr: impl.PGcancel_struct | None = pgcancel_ptr

    def __del__(self) -> None:
        self.free()

    def free(self, __PQfreeCancel: Any = impl.PQfreeCancel) -> None:
        """
        Free the data structure created by :pq:`PQgetCancel()`.

        Automatically invoked by `!__del__()`.

        See :pq:`PQfreeCancel()` for details.
        """
        self.pgcancel_ptr, p = None, self.pgcancel_ptr
        if p:
            __PQfreeCancel(p)  # see note_on_del

    def cancel(self) -> None:
        """Requests that the server abandon processing of the current command.

        See :pq:`PQcancel()` for details.
        """
        buf = create_string_buffer(256)
        res = impl.PQcancel(
            self.pgcancel_ptr,
            byref(buf),  # type: ignore[arg-type]
            len(buf),
        )
        if not res:
            raise e.OperationalError(
                f"cancel failed: {buf.value.decode('utf8', 'ignore')}"
            )


class Conninfo:
    """
    Utility object to manipulate connection strings.
    """

    @classmethod
    def get_defaults(cls) -> list[ConninfoOption]:
        """Return the default connection options.

        The defaults take into account the environment variables, such as
        ``PGHOST``, and the compiled-in defaults.

        See :pq:`PQconndefaults()` for details.
        """
        if not (opts := impl.PQconndefaults()):
            raise MemoryError("couldn't allocate connection defaults")
        try:
            return cls._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    @classmethod
    def parse(cls, conninfo: bytes) -> list[ConninfoOption]:
        """Parse a connection string into its options.

        :param conninfo: the connection string, as key/value pairs or URI.
        :return: all the options. The ones not in the connection string
            have `~ConninfoOption.val` `!None`.
        :raises ~psycopg.OperationalError: if the connection string is not
            valid.

        See :pq:`PQconninfoParse()` for details.
        """
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        errmsg = c_char_p()
        rv = impl.PQconninfoParse(conninfo, byref(errmsg))  # type: ignore[arg-type]
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
    ) -> list[ConninfoOption]:
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

    def __init__(self, conn: PGconn | None = None):
        self.conn = conn

    def escape_literal(self, data: abc.Buffer) -> bytes:
        """Escape a string to be used as a literal in a SQL command.

        The result includes the surrounding quotes.

        :raises ~psycopg.OperationalError: if the object has no connection or
            if escaping failed, for instance for an invalid encoding.

        See :pq:`PQescapeLiteral()` for details.
        """
        if not self.conn:
            raise e.OperationalError("escape_literal failed: no connection provided")

        self.conn._ensure_pgconn()
        # TODO: might be done without copy (however C does that)
        if not isinstance(data, bytes):
            data = bytes(data)

        if not (out := impl.PQescapeLiteral(self.conn._pgconn_ptr, data, len(data))):
            raise e.OperationalError(
                f"escape_literal failed: {self.conn.get_error_message()} bytes"
            )
        rv = string_at(out)
        impl.PQfreemem(out)
        return rv

    def escape_identifier(self, data: abc.Buffer) -> bytes:
        """Escape a string to be used as an identifier in a SQL command, such as
        a table or column name.

        The result includes the surrounding double quotes.

        :raises ~psycopg.OperationalError: if the object has no connection or
            if escaping failed, for instance for an invalid encoding.

        See :pq:`PQescapeIdentifier()` for details.
        """
        if not self.conn:
            raise e.OperationalError("escape_identifier failed: no connection provided")

        self.conn._ensure_pgconn()

        if not isinstance(data, bytes):
            data = bytes(data)

        if not (out := impl.PQescapeIdentifier(self.conn._pgconn_ptr, data, len(data))):
            raise e.OperationalError(
                f"escape_identifier failed: {self.conn.get_error_message()} bytes"
            )
        rv = string_at(out)
        impl.PQfreemem(out)
        return rv

    def escape_string(self, data: abc.Buffer) -> bytes:
        """Escape a string to be used in a SQL string literal.

        Unlike `escape_literal()`, the result doesn't include the quotes.
        Without a connection, the escaping can't take into account the encoding
        and the :sql:`standard_conforming_strings` setting of the connection.

        :raises ~psycopg.OperationalError: if escaping failed, for instance for
            an invalid encoding.

        See :pq:`PQescapeStringConn()` and :pq:`PQescapeString()` for details.
        """
        if not isinstance(data, bytes):
            data = bytes(data)

        if self.conn:
            self.conn._ensure_pgconn()
            error = c_int()
            out = create_string_buffer(len(data) * 2 + 1)
            impl.PQescapeStringConn(
                self.conn._pgconn_ptr,
                byref(out),  # type: ignore[arg-type]
                data,
                len(data),
                byref(error),  # type: ignore[arg-type]
            )

            if error:
                raise e.OperationalError(
                    f"escape_string failed: {self.conn.get_error_message()} bytes"
                )

        else:
            out = create_string_buffer(len(data) * 2 + 1)
            impl.PQescapeString(
                byref(out),  # type: ignore[arg-type]
                data,
                len(data),
            )

        return out.value

    def escape_bytea(self, data: abc.Buffer) -> bytes:
        """Escape binary data to be used as a :sql:`bytea` literal in a SQL
        command.

        Unlike `escape_literal()`, the result doesn't include the quotes.

        See :pq:`PQescapeByteaConn()` and :pq:`PQescapeBytea()` for details.
        """
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
                byref(t_cast(c_ulong, len_out)),  # type: ignore[arg-type]
            )
        else:
            out = impl.PQescapeBytea(
                data,
                len(data),
                byref(t_cast(c_ulong, len_out)),  # type: ignore[arg-type]
            )
        if not out:
            raise MemoryError(
                f"couldn't allocate for escape_bytea of {len(data)} bytes"
            )

        rv = string_at(out, len_out.value - 1)  # out includes final 0
        impl.PQfreemem(out)
        return rv

    def unescape_bytea(self, data: abc.Buffer) -> bytes:
        # not needed, but let's keep it symmetric with the escaping:
        # if a connection is passed in, it must be valid.
        """Convert the text representation of a :sql:`bytea` value to binary data.

        Use it to decode a :sql:`bytea` value received in text format.

        See :pq:`PQunescapeBytea()` for details.
        """
        if self.conn:
            self.conn._ensure_pgconn()

        len_out = c_size_t()
        if not isinstance(data, bytes):
            data = bytes(data)
        out = impl.PQunescapeBytea(
            data,
            byref(t_cast(c_ulong, len_out)),  # type: ignore[arg-type]
        )
        if not out:
            raise MemoryError(
                f"couldn't allocate for unescape_bytea of {len(data)} bytes"
            )

        rv = string_at(out, len_out.value)
        impl.PQfreemem(out)
        return rv


# importing the ssl module sets up Python's libcrypto callbacks
import ssl  # noqa

# disable libcrypto setup in libpq, so it won't stomp on the callbacks
# that have already been set up
impl.PQinitOpenSSL(1, 0)

__build_version__ = version()
