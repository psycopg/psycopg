"""
Functions to manipulate conninfo strings
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import os
import re
import socket
import asyncio
import logging
from typing import Any
from random import shuffle
from pathlib import Path
from datetime import tzinfo
from functools import lru_cache
from ipaddress import ip_address
from dataclasses import dataclass
from typing_extensions import TypeAlias

from . import pq
from . import errors as e
from ._tz import get_tzinfo
from ._encodings import pgconn_encoding

ConnDict: TypeAlias = "dict[str, Any]"

# Default timeout for connection a attempt.
# Arbitrary timeout, what applied by the libpq on my computer.
# Your mileage won't vary.
_DEFAULT_CONNECT_TIMEOUT = 130

logger = logging.getLogger("psycopg")


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


def conninfo_attempts(params: ConnDict) -> list[ConnDict]:
    """Split a set of connection params on the single attempts to perform.

    A connection param can perform more than one attempt more than one ``host``
    is provided.

    Because the libpq async function doesn't honour the timeout, we need to
    reimplement the repeated attempts.
    """
    # TODO: we should actually resolve the hosts ourselves.
    # If an host resolves to more than one ip, the libpq will make more than
    # one attempt and wouldn't get to try the following ones, as before
    # fixing #674.
    attempts = _split_attempts(params)
    if _get_param(params, "load_balance_hosts") == "random":
        shuffle(attempts)
    return attempts


async def conninfo_attempts_async(params: ConnDict) -> list[ConnDict]:
    """Split a set of connection params on the single attempts to perform.

    A connection param can perform more than one attempt more than one ``host``
    is provided.

    Also perform async resolution of the hostname into hostaddr in order to
    avoid blocking. Because a host can resolve to more than one address, this
    can lead to yield more attempts too. Raise `OperationalError` if no host
    could be resolved.

    Because the libpq async function doesn't honour the timeout, we need to
    reimplement the repeated attempts.
    """
    last_exc = None
    attempts = []
    for attempt in _split_attempts(params):
        try:
            attempts.extend(await _resolve_hostnames(attempt))
        except OSError as ex:
            logger.debug("failed to resolve host %r: %s", attempt.get("host"), str(ex))
            last_exc = ex

    if not attempts:
        assert last_exc
        # We couldn't resolve anything
        raise e.OperationalError(str(last_exc))

    if _get_param(params, "load_balance_hosts") == "random":
        shuffle(attempts)

    return attempts


def _split_attempts(params: ConnDict) -> list[ConnDict]:
    """
    Split connection parameters with a sequence of hosts into separate attempts.
    """

    def split_val(key: str) -> list[str]:
        val = _get_param(params, key)
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

    # A single attempt to make. Don't mangle the conninfo string.
    if nhosts <= 1:
        return [params]

    if len(ports) == 1:
        ports *= nhosts

    # Now all lists are either empty or have the same length
    rv = []
    for i in range(nhosts):
        attempt = params.copy()
        if hosts:
            attempt["host"] = hosts[i]
        if hostaddrs:
            attempt["hostaddr"] = hostaddrs[i]
        if ports:
            attempt["port"] = ports[i]
        rv.append(attempt)

    return rv


async def _resolve_hostnames(params: ConnDict) -> list[ConnDict]:
    """
    Perform async DNS lookup of the hosts and return a new params dict.

    If a ``host`` param is present but not ``hostname``, resolve the host
    addresses asynchronously.

    :param params: The input parameters, for instance as returned by
        `~psycopg.conninfo.conninfo_to_dict()`. The function expects at most
        a single entry for host, hostaddr because it is designed to further
        process the input of _split_attempts().

    :return: A list of attempts to make (to include the case of a hostname
        resolving to more than one IP).
    """
    host = _get_param(params, "host")
    if not host or host.startswith("/") or host[1:2] == ":":
        # Local path, or no host to resolve
        return [params]

    hostaddr = _get_param(params, "hostaddr")
    if hostaddr:
        # Already resolved
        return [params]

    if is_ip_address(host):
        # If the host is already an ip address don't try to resolve it
        return [{**params, "hostaddr": host}]

    loop = asyncio.get_running_loop()

    port = _get_param(params, "port")
    if not port:
        port_def = _get_param_def("port")
        port = port_def and port_def.compiled or "5432"

    ans = await loop.getaddrinfo(
        host, int(port), proto=socket.IPPROTO_TCP, type=socket.SOCK_STREAM
    )
    return [{**params, "hostaddr": item[4][0]} for item in ans]


def timeout_from_conninfo(params: ConnDict) -> int:
    """
    Return the timeout in seconds from the connection parameters.
    """
    # Follow the libpq convention:
    #
    # - 0 or less means no timeout (but we will use a default to simulate
    #   the socket timeout)
    # - at least 2 seconds.
    #
    # See connectDBComplete in fe-connect.c
    value: str | int | None = _get_param(params, "connect_timeout")
    if value is None:
        value = _DEFAULT_CONNECT_TIMEOUT
    try:
        timeout = int(value)
    except ValueError:
        raise e.ProgrammingError(f"bad value for connect_timeout: {value!r}")

    if timeout <= 0:
        # The sync connect function will stop on the default socket timeout
        # Because in async connection mode we need to enforce the timeout
        # ourselves, we need a finite value.
        timeout = _DEFAULT_CONNECT_TIMEOUT
    elif timeout < 2:
        # Enforce a 2s min
        timeout = 2

    return timeout


def _get_param(params: ConnDict, name: str) -> str | None:
    """
    Return a value from a connection string.

    The value may be also specified in a PG* env var.
    """
    if name in params:
        return str(params[name])

    # TODO: check if in service

    paramdef = _get_param_def(name)
    if not paramdef:
        return None

    env = os.environ.get(paramdef.envvar)
    if env is not None:
        return env

    return None


@dataclass
class ParamDef:
    """
    Information about defaults and env vars for connection params
    """

    keyword: str
    envvar: str
    compiled: str | None


def _get_param_def(keyword: str, _cache: dict[str, ParamDef] = {}) -> ParamDef | None:
    """
    Return the ParamDef of a connection string parameter.
    """
    if not _cache:
        defs = pq.Conninfo.get_defaults()
        for d in defs:
            cd = ParamDef(
                keyword=d.keyword.decode(),
                envvar=d.envvar.decode() if d.envvar else "",
                compiled=d.compiled.decode() if d.compiled is not None else None,
            )
            _cache[cd.keyword] = cd

    return _cache.get(keyword)


@lru_cache()
def is_ip_address(s: str) -> bool:
    """Return True if the string represent a valid ip address."""
    try:
        ip_address(s)
    except ValueError:
        return False
    return True
