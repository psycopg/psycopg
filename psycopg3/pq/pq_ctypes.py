"""
libpq Python wrapper using ctypes bindings.

Clients shouldn't use this module directly, unless for testing: they should use
the `pq` module instead, which is in charge of choosing the best
implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from ctypes import string_at
from ctypes import c_char_p, c_int, pointer
from typing import Any, List, Optional, Sequence

from .enums import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)
from .misc import error_message, ConninfoOption
from . import _pq_ctypes as impl
from ..exceptions import OperationalError
from ..utils.typing import Oid


def version() -> int:
    return impl.PQlibVersion()  # type: ignore


class PQerror(OperationalError):
    pass


class PGconn:
    __slots__ = ("pgconn_ptr",)

    def __init__(self, pgconn_ptr: impl.PGconn_struct):
        self.pgconn_ptr: Optional[impl.PGconn_struct] = pgconn_ptr

    def __del__(self) -> None:
        self.finish()

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

    def connect_poll(self) -> PollingStatus:
        rv = impl.PQconnectPoll(self.pgconn_ptr)
        return PollingStatus(rv)

    def finish(self) -> None:
        self.pgconn_ptr, p = None, self.pgconn_ptr
        if p is not None:
            impl.PQfinish(p)

    @property
    def info(self) -> List["ConninfoOption"]:
        opts = impl.PQconninfo(self.pgconn_ptr)
        if not opts:
            raise MemoryError("couldn't allocate connection info")
        try:
            return Conninfo._options_from_array(opts)
        finally:
            impl.PQconninfoFree(opts)

    def reset(self) -> None:
        impl.PQreset(self.pgconn_ptr)

    def reset_start(self) -> None:
        if not impl.PQresetStart(self.pgconn_ptr):
            raise PQerror("couldn't reset connection")

    def reset_poll(self) -> PollingStatus:
        rv = impl.PQresetPoll(self.pgconn_ptr)
        return PollingStatus(rv)

    @classmethod
    def ping(self, conninfo: bytes) -> Ping:
        if not isinstance(conninfo, bytes):
            raise TypeError(f"bytes expected, got {type(conninfo)} instead")

        rv = impl.PQping(conninfo)
        return Ping(rv)

    @property
    def db(self) -> bytes:
        return impl.PQdb(self.pgconn_ptr)  # type: ignore

    @property
    def user(self) -> bytes:
        return impl.PQuser(self.pgconn_ptr)  # type: ignore

    @property
    def password(self) -> bytes:
        return impl.PQpass(self.pgconn_ptr)  # type: ignore

    @property
    def host(self) -> bytes:
        return impl.PQhost(self.pgconn_ptr)  # type: ignore

    @property
    def hostaddr(self) -> bytes:
        return impl.PQhostaddr(self.pgconn_ptr)  # type: ignore

    @property
    def port(self) -> bytes:
        return impl.PQport(self.pgconn_ptr)  # type: ignore

    @property
    def tty(self) -> bytes:
        return impl.PQtty(self.pgconn_ptr)  # type: ignore

    @property
    def options(self) -> bytes:
        return impl.PQoptions(self.pgconn_ptr)  # type: ignore

    @property
    def status(self) -> ConnStatus:
        rv = impl.PQstatus(self.pgconn_ptr)
        return ConnStatus(rv)

    @property
    def transaction_status(self) -> TransactionStatus:
        rv = impl.PQtransactionStatus(self.pgconn_ptr)
        return TransactionStatus(rv)

    def parameter_status(self, name: bytes) -> bytes:
        return impl.PQparameterStatus(self.pgconn_ptr, name)  # type: ignore

    @property
    def protocol_version(self) -> int:
        return impl.PQprotocolVersion(self.pgconn_ptr)  # type: ignore

    @property
    def server_version(self) -> int:
        return impl.PQserverVersion(self.pgconn_ptr)  # type: ignore

    @property
    def error_message(self) -> bytes:
        return impl.PQerrorMessage(self.pgconn_ptr)  # type: ignore

    @property
    def socket(self) -> int:
        return impl.PQsocket(self.pgconn_ptr)  # type: ignore

    @property
    def backend_pid(self) -> int:
        return impl.PQbackendPID(self.pgconn_ptr)  # type: ignore

    @property
    def needs_password(self) -> bool:
        return bool(impl.PQconnectionNeedsPassword(self.pgconn_ptr))

    @property
    def used_password(self) -> bool:
        return bool(impl.PQconnectionUsedPassword(self.pgconn_ptr))

    @property
    def ssl_in_use(self) -> bool:
        return bool(impl.PQsslInUse(self.pgconn_ptr))

    def exec_(self, command: bytes) -> "PGresult":
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        rv = impl.PQexec(self.pgconn_ptr, command)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query(self, command: bytes) -> None:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")
        if not impl.PQsendQuery(self.pgconn_ptr, command):
            raise PQerror(f"sending query failed: {error_message(self)}")

    def exec_params(
        self,
        command: bytes,
        param_values: List[Optional[bytes]],
        param_types: Optional[List[Oid]] = None,
        param_formats: Optional[List[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> "PGresult":
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        rv = impl.PQexecParams(*args)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def send_query_params(
        self,
        command: bytes,
        param_values: List[Optional[bytes]],
        param_types: Optional[List[Oid]] = None,
        param_formats: Optional[List[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        args = self._query_params_args(
            command, param_values, param_types, param_formats, result_format
        )
        if not impl.PQsendQueryParams(*args):
            raise PQerror(
                f"sending query and params failed: {error_message(self)}"
            )

    def _query_params_args(
        self,
        command: bytes,
        param_values: List[Optional[bytes]],
        param_types: Optional[List[Oid]] = None,
        param_formats: Optional[List[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> Any:
        if not isinstance(command, bytes):
            raise TypeError(f"bytes expected, got {type(command)} instead")

        nparams = len(param_values)
        if nparams:
            aparams = (c_char_p * nparams)(*param_values)
            alenghts = (c_int * nparams)(
                *(len(p) if p is not None else 0 for p in param_values)
            )
        else:
            aparams = alenghts = None  # type: ignore

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

        return (
            self.pgconn_ptr,
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
        param_types: Optional[List[Oid]] = None,
    ) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        if not isinstance(command, bytes):
            raise TypeError(
                f"'command' must be bytes, got {type(command)} instead"
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
        self,
        name: bytes,
        param_values: List[bytes],
        param_formats: Optional[List[int]] = None,
        result_format: int = 0,
    ) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")

        nparams = len(param_values)
        if nparams:
            aparams = (c_char_p * nparams)(*param_values)
            alenghts = (c_int * nparams)(
                *(len(p) if p is not None else 0 for p in param_values)
            )
        else:
            aparams = alenghts = None  # type: ignore

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

    def describe_prepared(self, name: bytes) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        rv = impl.PQdescribePrepared(self.pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def describe_portal(self, name: bytes) -> "PGresult":
        if not isinstance(name, bytes):
            raise TypeError(f"'name' must be bytes, got {type(name)} instead")
        rv = impl.PQdescribePortal(self.pgconn_ptr, name)
        if not rv:
            raise MemoryError("couldn't allocate PGresult")
        return PGresult(rv)

    def get_result(self) -> Optional["PGresult"]:
        rv = impl.PQgetResult(self.pgconn_ptr)
        return PGresult(rv) if rv else None

    def consume_input(self) -> None:
        if 1 != impl.PQconsumeInput(self.pgconn_ptr):
            raise PQerror(f"consuming input failed: {error_message(self)}")

    def is_busy(self) -> int:
        return impl.PQisBusy(self.pgconn_ptr)  # type: ignore

    @property
    def nonblocking(self) -> int:
        return impl.PQisnonblocking(self.pgconn_ptr)  # type: ignore

    @nonblocking.setter
    def nonblocking(self, arg: int) -> None:
        if 0 > impl.PQsetnonblocking(self.pgconn_ptr, arg):
            raise PQerror(f"setting nonblocking failed: {error_message(self)}")

    def flush(self) -> int:
        rv: int = impl.PQflush(self.pgconn_ptr)
        if rv < 0:
            raise PQerror(f"flushing failed: {error_message(self)}")
        return rv

    def make_empty_result(self, exec_status: ExecStatus) -> "PGresult":
        rv = impl.PQmakeEmptyPGresult(self.pgconn_ptr, exec_status)
        if not rv:
            raise MemoryError("couldn't allocate empty PGresult")
        return PGresult(rv)


class PGresult:
    __slots__ = ("pgresult_ptr",)

    def __init__(self, pgresult_ptr: type):
        self.pgresult_ptr: Optional[type] = pgresult_ptr

    def __del__(self) -> None:
        self.clear()

    def clear(self) -> None:
        self.pgresult_ptr, p = None, self.pgresult_ptr
        if p is not None:
            impl.PQclear(p)

    @property
    def status(self) -> ExecStatus:
        rv = impl.PQresultStatus(self.pgresult_ptr)
        return ExecStatus(rv)

    @property
    def error_message(self) -> bytes:
        return impl.PQresultErrorMessage(self.pgresult_ptr)  # type: ignore

    def error_field(self, fieldcode: DiagnosticField) -> bytes:
        return impl.PQresultErrorField(  # type: ignore
            self.pgresult_ptr, fieldcode
        )

    @property
    def ntuples(self) -> int:
        return impl.PQntuples(self.pgresult_ptr)  # type: ignore

    @property
    def nfields(self) -> int:
        return impl.PQnfields(self.pgresult_ptr)  # type: ignore

    def fname(self, column_number: int) -> int:
        return impl.PQfname(self.pgresult_ptr, column_number)  # type: ignore

    def ftable(self, column_number: int) -> Oid:
        return impl.PQftable(self.pgresult_ptr, column_number)  # type: ignore

    def ftablecol(self, column_number: int) -> int:
        return impl.PQftablecol(  # type: ignore
            self.pgresult_ptr, column_number
        )

    def fformat(self, column_number: int) -> Format:
        return impl.PQfformat(self.pgresult_ptr, column_number)  # type: ignore

    def ftype(self, column_number: int) -> Oid:
        return impl.PQftype(self.pgresult_ptr, column_number)  # type: ignore

    def fmod(self, column_number: int) -> int:
        return impl.PQfmod(self.pgresult_ptr, column_number)  # type: ignore

    def fsize(self, column_number: int) -> int:
        return impl.PQfsize(self.pgresult_ptr, column_number)  # type: ignore

    @property
    def binary_tuples(self) -> Format:
        return Format(impl.PQbinaryTuples(self.pgresult_ptr))

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        length: int = impl.PQgetlength(
            self.pgresult_ptr, row_number, column_number
        )
        if length:
            v = impl.PQgetvalue(self.pgresult_ptr, row_number, column_number)
            return string_at(v, length)
        else:
            if impl.PQgetisnull(self.pgresult_ptr, row_number, column_number):
                return None
            else:
                return b""

    @property
    def nparams(self) -> int:
        return impl.PQnparams(self.pgresult_ptr)  # type: ignore

    def param_type(self, param_number: int) -> Oid:
        return impl.PQparamtype(  # type: ignore
            self.pgresult_ptr, param_number
        )

    @property
    def command_status(self) -> bytes:
        return impl.PQcmdStatus(self.pgresult_ptr)  # type: ignore

    @property
    def command_tuples(self) -> Optional[int]:
        rv = impl.PQcmdTuples(self.pgresult_ptr)
        return int(rv) if rv else None

    @property
    def oid_value(self) -> Oid:
        return impl.PQoidValue(self.pgresult_ptr)  # type: ignore


class Conninfo:
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
                exc = PQerror((errmsg.value or b"").decode("utf8", "replace"))
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
        skws = "keyword envvar compiled val label dispatcher".split()
        for opt in opts:
            if not opt.keyword:
                break
            d = {kw: getattr(opt, kw) for kw in skws}
            d["dispsize"] = opt.dispsize
            rv.append(ConninfoOption(**d))

        return rv
