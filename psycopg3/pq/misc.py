"""
Various functionalities to make easier to work with the libpq.
"""

# Copyright (C) 2020 The Psycopg Team

from collections import namedtuple
from typing import cast, Union

from ..errors import OperationalError
from .enums import DiagnosticField
from .proto import PGconn, PGresult


class PQerror(OperationalError):
    pass


ConninfoOption = namedtuple(
    "ConninfoOption", "keyword envvar compiled val label dispchar dispsize"
)


def error_message(obj: Union[PGconn, PGresult]) -> str:
    """
    Return an error message from a PGconn or PGresult.

    The return value is a str (unlike pq data which is usually bytes).
    """
    bmsg: bytes

    if hasattr(obj, "error_field"):
        obj = cast(PGresult, obj)

        bmsg = obj.error_field(DiagnosticField.MESSAGE_PRIMARY) or b""
        if not bmsg:
            bmsg = obj.error_message

            # strip severity and whitespaces
            if bmsg:
                bmsg = bmsg.splitlines()[0].split(b":", 1)[-1].strip()

    elif hasattr(obj, "error_message"):
        # obj is a PGconn
        bmsg = obj.error_message

        # strip severity and whitespaces
        if bmsg:
            bmsg = bmsg.splitlines()[0].split(b":", 1)[-1].strip()

    else:
        raise TypeError(
            f"PGconn or PGresult expected, got {type(obj).__name__}"
        )

    if bmsg:
        msg = bmsg.decode(
            "utf8", "replace"
        )  # TODO: or in connection encoding?
    else:
        msg = "no details available"

    return msg
