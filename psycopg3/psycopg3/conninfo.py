"""
Functions to manipulate conninfo strings
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
from typing import Any, Dict, List

from . import pq
from . import errors as e


def make_conninfo(conninfo: str = "", **kwargs: Any) -> str:
    """
    Merge a string and keyword params into a single conninfo string.

    Raise ProgrammingError if the input don't make a valid conninfo.
    """
    if not conninfo and not kwargs:
        return ""

    # If no kwarg specified don't mung the conninfo but check if it's correct.
    # Make sure to return a string, not a subtype, to avoid making Liskov sad.
    if not kwargs:
        _parse_conninfo(conninfo)
        return str(conninfo)

    # Override the conninfo with the parameters
    # Drop the None arguments
    kwargs = {k: v for (k, v) in kwargs.items() if v is not None}

    if conninfo:
        tmp = conninfo_to_dict(conninfo)
        tmp.update(kwargs)
        kwargs = tmp

    conninfo = " ".join(
        ["%s=%s" % (k, _param_escape(str(v))) for (k, v) in kwargs.items()]
    )

    # Verify the result is valid
    _parse_conninfo(conninfo)

    return conninfo


def conninfo_to_dict(conninfo: str) -> Dict[str, str]:
    """
    Convert the *conninfo* string into a dictionary of parameters.

    Raise ProgrammingError if the string is not valid.
    """
    opts = _parse_conninfo(conninfo)
    return {
        opt.keyword.decode("utf8"): opt.val.decode("utf8")
        for opt in opts
        if opt.val is not None
    }


def _parse_conninfo(conninfo: str) -> List[pq.ConninfoOption]:
    """
    Verify that *conninfo* is a valid connection string.

    Raise ProgrammingError if the string is not valid.

    Return the result of pq.Conninfo.parse() on success.
    """
    try:
        return pq.Conninfo.parse(conninfo.encode("utf8"))
    except e.OperationalError as ex:
        raise e.ProgrammingError(str(ex))


re_escape = re.compile(r"([\\'])")
re_space = re.compile(r"\s")


def _param_escape(s: str) -> str:
    """
    Apply the escaping rule required by PQconnectdb
    """
    if not s:
        return "''"

    s = re_escape.sub(r"\\\1", s)
    if re_space.search(s):
        s = "'" + s + "'"

    return s
