"""
Various functionalities to make easier to work with the libpq.
"""

# Copyright (C) 2020 The Psycopg Team

from collections import namedtuple
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from psycopg3.pq import PGconn, PGresult  # noqa


ConninfoOption = namedtuple(
    "ConninfoOption", "keyword envvar compiled val label dispatcher dispsize"
)


def error_message(obj: Union["PGconn", "PGresult"]) -> str:
    """
    Return an error message from a PGconn or PGresult.

    The return value is a str (unlike pq data which is usually bytes).
    """
    from psycopg3 import pq

    bmsg: bytes

    if isinstance(obj, pq.PGconn):
        bmsg = obj.error_message

        # strip severity and whitespaces
        if bmsg:
            bmsg = bmsg.splitlines()[0].split(b":", 1)[-1].strip()

    elif isinstance(obj, pq.PGresult):
        bmsg = obj.error_field(pq.DiagnosticField.MESSAGE_PRIMARY)
        if not bmsg:
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
