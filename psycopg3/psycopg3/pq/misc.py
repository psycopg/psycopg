"""
Various functionalities to make easier to work with the libpq.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import cast, NamedTuple, Optional, Union

from ..errors import OperationalError
from .enums import DiagnosticField, ConnStatus
from .proto import PGconn, PGresult
from .encodings import py_codecs


class PQerror(OperationalError):
    pass


class PGnotify(NamedTuple):
    relname: bytes
    be_pid: int
    extra: bytes


class ConninfoOption(NamedTuple):
    keyword: bytes
    envvar: Optional[bytes]
    compiled: Optional[bytes]
    val: Optional[bytes]
    label: bytes
    dispchar: bytes
    dispsize: int


class PGresAttDesc(NamedTuple):
    name: bytes
    tableid: int
    columnid: int
    format: int
    typid: int
    typlen: int
    atttypmod: int


def error_message(obj: Union[PGconn, PGresult], encoding: str = "utf8") -> str:
    """
    Return an error message from a PGconn or PGresult.

    The return value is a str (unlike pq data which is usually bytes): use
    the connection encoding if available, otherwise the *encoding* parameter
    as a fallback for decoding. Don't raise exception on decode errors.

    """
    bmsg: bytes

    if hasattr(obj, "error_field"):
        # obj is a PGresult
        obj = cast(PGresult, obj)

        bmsg = obj.error_field(DiagnosticField.MESSAGE_PRIMARY) or b""
        if not bmsg:
            bmsg = obj.error_message

            # strip severity and whitespaces
            if bmsg:
                bmsg = bmsg.splitlines()[0].split(b":", 1)[-1].strip()

    elif hasattr(obj, "error_message"):
        # obj is a PGconn
        obj = cast(PGconn, obj)
        if obj.status == ConnStatus.OK:
            encoding = py_codecs.get(
                obj.parameter_status(b"client_encoding"), "utf8"
            )
        bmsg = obj.error_message

        # strip severity and whitespaces
        if bmsg:
            bmsg = bmsg.splitlines()[0].split(b":", 1)[-1].strip()

    else:
        raise TypeError(
            f"PGconn or PGresult expected, got {type(obj).__name__}"
        )

    if bmsg:
        msg = bmsg.decode(encoding, "replace")
    else:
        msg = "no details available"

    return msg
