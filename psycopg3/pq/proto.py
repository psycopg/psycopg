"""
Protocol objects to represent objects exposed by different pq implementations.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Callable, List, Optional, Sequence, TYPE_CHECKING
from typing_extensions import Protocol

from .enums import (
    ConnStatus,
    PollingStatus,
    ExecStatus,
    TransactionStatus,
    Ping,
    DiagnosticField,
    Format,
)

if TYPE_CHECKING:
    from .misc import PGnotify, ConninfoOption  # noqa


class PGconn(Protocol):

    notice_handler: Optional[Callable[["PGresult"], None]]
    notify_handler: Optional[Callable[["PGnotify"], None]]

    @classmethod
    def connect(cls, conninfo: bytes) -> "PGconn":
        ...

    @classmethod
    def connect_start(cls, conninfo: bytes) -> "PGconn":
        ...

    def connect_poll(self) -> PollingStatus:
        ...

    def finish(self) -> None:
        ...

    @property
    def info(self) -> List["ConninfoOption"]:
        ...

    def reset(self) -> None:
        ...

    def reset_start(self) -> None:
        ...

    def reset_poll(self) -> PollingStatus:
        ...

    @classmethod
    def ping(self, conninfo: bytes) -> Ping:
        ...

    @property
    def db(self) -> bytes:
        ...

    @property
    def user(self) -> bytes:
        ...

    @property
    def password(self) -> bytes:
        ...

    @property
    def host(self) -> bytes:
        ...

    @property
    def hostaddr(self) -> bytes:
        ...

    @property
    def port(self) -> bytes:
        ...

    @property
    def tty(self) -> bytes:
        ...

    @property
    def options(self) -> bytes:
        ...

    @property
    def status(self) -> ConnStatus:
        ...

    @property
    def transaction_status(self) -> TransactionStatus:
        ...

    def parameter_status(self, name: bytes) -> Optional[bytes]:
        ...

    @property
    def error_message(self) -> bytes:
        ...

    @property
    def protocol_version(self) -> int:
        ...

    @property
    def server_version(self) -> int:
        ...

    @property
    def socket(self) -> int:
        ...

    @property
    def backend_pid(self) -> int:
        ...

    @property
    def needs_password(self) -> bool:
        ...

    @property
    def used_password(self) -> bool:
        ...

    @property
    def ssl_in_use(self) -> bool:
        ...

    def exec_(self, command: bytes) -> "PGresult":
        ...

    def send_query(self, command: bytes) -> None:
        ...

    def exec_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> "PGresult":
        ...

    def send_query_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        ...

    def send_prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> None:
        ...

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_formats: Optional[Sequence[Format]] = None,
        result_format: Format = Format.TEXT,
    ) -> None:
        ...

    def prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> "PGresult":
        ...

    def exec_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[bytes]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = 0,
    ) -> "PGresult":
        ...

    def describe_prepared(self, name: bytes) -> "PGresult":
        ...

    def describe_portal(self, name: bytes) -> "PGresult":
        ...

    def get_result(self) -> Optional["PGresult"]:
        ...

    def consume_input(self) -> None:
        ...

    def is_busy(self) -> int:
        ...

    @property
    def nonblocking(self) -> int:
        ...

    @nonblocking.setter
    def nonblocking(self, arg: int) -> None:
        ...

    def flush(self) -> int:
        ...

    def get_cancel(self) -> "PGcancel":
        ...

    def notifies(self) -> Optional["PGnotify"]:
        ...

    def make_empty_result(self, exec_status: ExecStatus) -> "PGresult":
        ...


class PGresult(Protocol):
    def clear(self) -> None:
        ...

    @property
    def status(self) -> ExecStatus:
        ...

    @property
    def error_message(self) -> bytes:
        ...

    def error_field(self, fieldcode: DiagnosticField) -> Optional[bytes]:
        ...

    @property
    def ntuples(self) -> int:
        ...

    @property
    def nfields(self) -> int:
        ...

    def fname(self, column_number: int) -> Optional[bytes]:
        ...

    def ftable(self, column_number: int) -> int:
        ...

    def ftablecol(self, column_number: int) -> int:
        ...

    def fformat(self, column_number: int) -> Format:
        ...

    def ftype(self, column_number: int) -> int:
        ...

    def fmod(self, column_number: int) -> int:
        ...

    def fsize(self, column_number: int) -> int:
        ...

    @property
    def binary_tuples(self) -> Format:
        ...

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        ...

    @property
    def nparams(self) -> int:
        ...

    def param_type(self, param_number: int) -> int:
        ...

    @property
    def command_status(self) -> Optional[bytes]:
        ...

    @property
    def command_tuples(self) -> Optional[int]:
        ...

    @property
    def oid_value(self) -> int:
        ...


class PGcancel(Protocol):
    def free(self) -> None:
        ...

    def cancel(self) -> None:
        ...


class Conninfo(Protocol):
    @classmethod
    def get_defaults(cls) -> List["ConninfoOption"]:
        ...

    @classmethod
    def parse(cls, conninfo: bytes) -> List["ConninfoOption"]:
        ...

    @classmethod
    def _options_from_array(
        cls, opts: Sequence[Any]
    ) -> List["ConninfoOption"]:
        ...


class Escaping(Protocol):
    def __init__(self, conn: Optional[PGconn] = None):
        ...

    def escape_bytea(self, data: bytes) -> bytes:
        ...

    def unescape_bytea(self, data: bytes) -> bytes:
        ...
