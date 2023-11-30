"""
Functions to manipulate conninfo strings
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import os
import re
import socket
import asyncio
from typing import Any, Iterator, AsyncIterator
from random import shuffle
from pathlib import Path
from datetime import tzinfo
from functools import lru_cache
from ipaddress import ip_address
from typing_extensions import TypeAlias

from . import pq
from . import errors as e
from ._tz import get_tzinfo
from ._compat import cache
from ._encodings import pgconn_encoding

ConnDict: TypeAlias = "dict[str, Any]"


def make_conninfo(conninfo: str = "", **kwargs: Any) -> str:
    """
    Merge a string and keyword params into a single conninfo string.

    :param conninfo: A `connection string`__ as accepted by PostgreSQL.
    :param kwargs: Parameters overriding the ones specified in `!conninfo`.
    :return: A connection string valid for PostgreSQL, with the `!kwargs`
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


def conninfo_to_dict(conninfo: str = "", **kwargs: Any) -> ConnDict:
    """
    Convert the `!conninfo` string into a dictionary of parameters.

    :param conninfo: A `connection string`__ as accepted by PostgreSQL.
    :param kwargs: Parameters overriding the ones specified in `!conninfo`.
    :return: Dictionary with the parameters parsed from `!conninfo` and
        `!kwargs`.

    Raise `~psycopg.ProgrammingError` if `!conninfo` is not a a valid connection
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


def _parse_conninfo(conninfo: str) -> list[pq.ConninfoOption]:
    """
    Verify that `!conninfo` is a valid connection string.

    Raise ProgrammingError if the string is not valid.

    Return the result of pq.Conninfo.parse() on success.
    """
    try:
        return pq.Conninfo.parse(conninfo.encode())
    except e.OperationalError as ex:
        raise e.ProgrammingError(str(ex)) from None


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
    def vendor(self) -> str:
        """A string representing the database vendor connected to."""
        return "PostgreSQL"

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

    def get_parameters(self) -> dict[str, str]:
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
        The current in-transaction status of the session.
        See :pq:`PQtransactionStatus()`.
        """
        return pq.TransactionStatus(self.pgconn.transaction_status)

    @property
    def pipeline_status(self) -> pq.PipelineStatus:
        """
        The current pipeline status of the client.
        See :pq:`PQpipelineStatus()`.
        """
        return pq.PipelineStatus(self.pgconn.pipeline_status)

    def parameter_status(self, param_name: str) -> str | None:
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


def conninfo_attempts(params: ConnDict) -> Iterator[ConnDict]:
    """Split a set of connection params on the single attempts to perforn.

    A connection param can perform more than one attempt more than one ``host``
    is provided.

    Because the libpq async function doesn't honour the timeout, we need to
    reimplement the repeated attempts.
    """
    if params.get("load_balance_hosts", "disable") == "random":
        attempts = list(_split_attempts(_inject_defaults(params)))
        shuffle(attempts)
        yield from attempts
    else:
        yield from _split_attempts(_inject_defaults(params))


async def conninfo_attempts_async(params: ConnDict) -> AsyncIterator[ConnDict]:
    """Split a set of connection params on the single attempts to perforn.

    A connection param can perform more than one attempt more than one ``host``
    is provided.

    Also perform async resolution of the hostname into hostaddr in order to
    avoid blocking. Because a host can resolve to more than one address, this
    can lead to yield more attempts too. Raise `OperationalError` if no host
    could be resolved.

    Because the libpq async function doesn't honour the timeout, we need to
    reimplement the repeated attempts.
    """
    yielded = False
    last_exc = None
    for attempt in _split_attempts(_inject_defaults(params)):
        try:
            async for a2 in _split_attempts_and_resolve(attempt):
                yielded = True
                yield a2
        except OSError as ex:
            last_exc = ex

    if not yielded:
        assert last_exc
        # We couldn't resolve anything
        raise e.OperationalError(str(last_exc))


def _inject_defaults(params: ConnDict) -> ConnDict:
    """
    Add defaults to a dictionary of parameters.

    This avoids the need to look up for env vars at various stages during
    processing.

    Note that a port is always specified. 5432 likely comes from here.

    The `host`, `hostaddr`, `port` will be always set to a string.
    """
    defaults = _conn_defaults()
    out = params.copy()

    def inject(name: str, envvar: str) -> None:
        value = out.get(name)
        if not value:
            out[name] = os.environ.get(envvar, defaults[name])
        else:
            out[name] = str(value)

    inject("host", "PGHOST")
    inject("hostaddr", "PGHOSTADDR")
    inject("port", "PGPORT")

    return out


def _split_attempts(params: ConnDict) -> Iterator[ConnDict]:
    """
    Split connection parameters with a sequence of hosts into separate attempts.

    Assume that `host`, `hostaddr`, `port` are always present and a string (as
    emitted from `_inject_defaults()`).
    """

    def split_val(key: str) -> list[str]:
        # Assume all keys are present and strings.
        val: str = params[key]
        return val.split(",") if val else []

    hosts = split_val("host")
    hostaddrs = split_val("hostaddr")
    ports = split_val("port")

    if hosts and hostaddrs and len(hosts) != len(hostaddrs):
        raise e.OperationalError(
            f"could not match {len(hosts)} host names"
            f" with {len(hostaddrs)} hostaddr values"
        )

    nhosts = max(len(hosts), len(hostaddrs))

    if 1 < len(ports) != nhosts:
        raise e.OperationalError(
            f"could not match {len(ports)} port numbers to {len(hosts)} hosts"
        )
    elif len(ports) == 1:
        ports *= nhosts

    # A single attempt to make
    if nhosts <= 1:
        yield params
        return

    # Now all lists are either empty or have the same length
    for i in range(nhosts):
        attempt = params.copy()
        if hosts:
            attempt["host"] = hosts[i]
        if hostaddrs:
            attempt["hostaddr"] = hostaddrs[i]
        if ports:
            attempt["port"] = ports[i]
        yield attempt


async def _split_attempts_and_resolve(params: ConnDict) -> AsyncIterator[ConnDict]:
    """
    Perform async DNS lookup of the hosts and return a new params dict.

    :param params: The input parameters, for instance as returned by
        `~psycopg.conninfo.conninfo_to_dict()`. The function expects at most
        a single entry for host, hostaddr, port and doesn't check for env vars
        because it is designed to further process the input of _split_attempts()

    If a ``host`` param is present but not ``hostname``, resolve the host
    addresses dynamically.

    The function may change the input ``host``, ``hostname``, ``port`` to allow
    connecting without further DNS lookups.

    Raise `~psycopg.OperationalError` if resolution fails.
    """
    host = params["host"]
    if not host or host.startswith("/") or host[1:2] == ":":
        # Local path, or no host to resolve
        yield params
        return

    hostaddr = params["hostaddr"]
    if hostaddr:
        # Already resolved
        yield params
        return

    if is_ip_address(host):
        # If the host is already an ip address don't try to resolve it
        params["hostaddr"] = host
        yield params
        return

    loop = asyncio.get_running_loop()

    port = params["port"]
    ans = await loop.getaddrinfo(
        host, port, proto=socket.IPPROTO_TCP, type=socket.SOCK_STREAM
    )

    attempt = params.copy()
    for item in ans:
        attempt["hostaddr"] = item[4][0]
    yield attempt


@cache
def _conn_defaults() -> dict[str, str]:
    """
    Return a dictionary of defaults for connection strings parameters.
    """
    defs = pq.Conninfo.get_defaults()
    return {
        d.keyword.decode(): d.compiled.decode() if d.compiled is not None else ""
        for d in defs
    }


@lru_cache()
def is_ip_address(s: str) -> bool:
    """Return True if the string represent a valid ip address."""
    try:
        ip_address(s)
    except ValueError:
        return False
    return True
