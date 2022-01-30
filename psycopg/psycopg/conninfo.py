"""
Functions to manipulate conninfo strings
"""

# Copyright (C) 2020 The Psycopg Team

import re
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import tzinfo

from . import pq
from . import errors as e
from ._tz import get_tzinfo
from ._encodings import pgconn_encoding


def make_conninfo(conninfo: str = "", **kwargs: Any) -> str:
    """
    Merge a string and keyword params into a single conninfo string.

    :param conninfo: A `connection string`__ as accepted by PostgreSQL.
    :param kwargs: Parameters overriding the ones specified in *conninfo*.
    :return: A connection string valid for PostgreSQL, with the *kwargs*
        parameters merged.

    Raise `~psycopg.ProgrammingError` if the input doesn't make a valid
    conninfo string.

    .. __: https://www.postgresql.org/docs/current/libpq-connect.html
           #LIBPQ-CONNSTRING
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

    conninfo = " ".join(f"{k}={_param_escape(str(v))}" for (k, v) in kwargs.items())

    # Verify the result is valid
    _parse_conninfo(conninfo)

    return conninfo


def conninfo_to_dict(conninfo: str = "", **kwargs: Any) -> Dict[str, Any]:
    """
    Convert the *conninfo* string into a dictionary of parameters.

    :param conninfo: A `connection string`__ as accepted by PostgreSQL.
    :param kwargs: Parameters overriding the ones specified in *conninfo*.
    :return: Dictionary with the parameters parsed from *conninfo* and
        *kwargs*.

    Raise `~psycopg.ProgrammingError` if *conninfo* is not a a valid connection
    string.

    .. __: https://www.postgresql.org/docs/current/libpq-connect.html
           #LIBPQ-CONNSTRING
    """
    opts = _parse_conninfo(conninfo)
    rv = {opt.keyword.decode(): opt.val.decode() for opt in opts if opt.val is not None}
    for k, v in kwargs.items():
        if v is not None:
            rv[k] = v
    return rv


def _parse_conninfo(conninfo: str) -> List[pq.ConninfoOption]:
    """
    Verify that *conninfo* is a valid connection string.

    Raise ProgrammingError if the string is not valid.

    Return the result of pq.Conninfo.parse() on success.
    """
    try:
        return pq.Conninfo.parse(conninfo.encode())
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


class ConnectionInfo:
    """Allow access to information about the connection."""

    __module__ = "psycopg"

    def __init__(self, pgconn: pq.abc.PGconn):
        self.pgconn = pgconn

    @property
    def host(self) -> str:
        """The server host name of the active connection. See :pq:`PQhost()`."""
        return self._get_pgconn_attr("host")

    @property
    def hostaddr(self) -> str:
        """The server IP address of the connection. See :pq:`PQhostaddr()`."""
        return self._get_pgconn_attr("hostaddr")

    @property
    def port(self) -> int:
        """The port of the active connection. See :pq:`PQport()`."""
        return int(self._get_pgconn_attr("port"))

    @property
    def dbname(self) -> str:
        """The database name of the connection. See :pq:`PQdb()`."""
        return self._get_pgconn_attr("db")

    @property
    def user(self) -> str:
        """The user name of the connection. See :pq:`PQuser()`."""
        return self._get_pgconn_attr("user")

    @property
    def password(self) -> str:
        """The password of the connection. See :pq:`PQpass()`."""
        return self._get_pgconn_attr("password")

    @property
    def options(self) -> str:
        """
        The command-line options passed in the connection request.
        See :pq:`PQoptions`.
        """
        return self._get_pgconn_attr("options")

    def get_parameters(self) -> Dict[str, str]:
        """Return the connection parameters values.

        Return all the parameters set to a non-default value, which might come
        either from the connection string and parameters passed to
        `~Connection.connect()` or from environment variables. The password
        is never returned (you can read it using the `password` attribute).
        """
        pyenc = self.encoding

        # Get the known defaults to avoid reporting them
        defaults = {
            i.keyword: i.compiled
            for i in pq.Conninfo.get_defaults()
            if i.compiled is not None
        }
        # Not returned by the libq. Bug? Bet we're using SSH.
        defaults.setdefault(b"channel_binding", b"prefer")
        defaults[b"passfile"] = str(Path.home() / ".pgpass").encode()

        return {
            i.keyword.decode(pyenc): i.val.decode(pyenc)
            for i in self.pgconn.info
            if i.val is not None
            and i.keyword != b"password"
            and i.val != defaults.get(i.keyword)
        }

    @property
    def dsn(self) -> str:
        """Return the connection string to connect to the database.

        The string contains all the parameters set to a non-default value,
        which might come either from the connection string and parameters
        passed to `~Connection.connect()` or from environment variables. The
        password is never returned (you can read it using the `password`
        attribute).
        """
        return make_conninfo(**self.get_parameters())

    @property
    def status(self) -> pq.ConnStatus:
        """The status of the connection. See :pq:`PQstatus()`."""
        return pq.ConnStatus(self.pgconn.status)

    @property
    def transaction_status(self) -> pq.TransactionStatus:
        """
        The current in-transaction status of the server.
        See :pq:`PQtransactionStatus()`.
        """
        return pq.TransactionStatus(self.pgconn.transaction_status)

    def parameter_status(self, param_name: str) -> Optional[str]:
        """
        Return a parameter setting of the connection.

        Return `None` is the parameter is unknown.
        """
        res = self.pgconn.parameter_status(param_name.encode(self.encoding))
        return res.decode(self.encoding) if res is not None else None

    @property
    def server_version(self) -> int:
        """
        An integer representing the server version. See :pq:`PQserverVersion()`.

        The number is formed by converting the major, minor, and revision
        numbers into two-decimal-digit numbers and appending them together.
        After PostgreSQL 10 the minor version was dropped, so the second group
        of digits is always 00. For example, version 9.3.5 is returned as
        90305, version 10.2 as 100002.
        """
        return self.pgconn.server_version

    @property
    def backend_pid(self) -> int:
        """
        The process ID (PID) of the backend process handling this connection.
        See :pq:`PQbackendPID()`.
        """
        return self.pgconn.backend_pid

    @property
    def error_message(self) -> str:
        """
        The error message most recently generated by an operation on the connection.
        See :pq:`PQerrorMessage()`.
        """
        return self._get_pgconn_attr("error_message")

    @property
    def timezone(self) -> tzinfo:
        """The Python timezone info of the connection's timezone."""
        return get_tzinfo(self.pgconn)

    @property
    def encoding(self) -> str:
        """The Python codec name of the connection's client encoding."""
        return pgconn_encoding(self.pgconn)

    def _get_pgconn_attr(self, name: str) -> str:
        value: bytes = getattr(self.pgconn, name)
        return value.decode(self.encoding)
