"""
Internal utilities to manipulate connection strings
"""

# Copyright (C) 2024 The Psycopg Team

from __future__ import annotations

import os
import sys
from functools import lru_cache
from ipaddress import ip_address
from dataclasses import dataclass

from . import errors as e
from . import pq
from .abc import ConnDict, ConnMapping


def split_attempts(params: ConnMapping) -> list[ConnDict]:
    """
    Split connection parameters with a sequence of hosts into separate attempts.
    """

    def split_val(key: str) -> list[str]:
        val = get_param(params, key)
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
        return [{**params}]

    if len(ports) == 1:
        ports *= nhosts

    # Now all lists are either empty or have the same length
    rv = []
    for i in range(nhosts):
        attempt = {**params}
        if hosts:
            attempt["host"] = hosts[i]
        if hostaddrs:
            attempt["hostaddr"] = hostaddrs[i]
        if ports:
            attempt["port"] = ports[i]
        rv.append(attempt)

    return rv


def get_param(params: ConnMapping, name: str) -> str | None:
    """
    Return a value from a connection string.

    The value may be also specified in a service file or in a PG* env var.
    The precedence is the same as in libpq: connection string, service file,
    env var.
    """
    if name in params:
        return str(params[name])

    if name != "service" and (service := _get_service(params)) is not None:
        if (val := service.get(name)) is not None:
            return val

    if not (paramdef := get_param_def(name)):
        return None

    if (env := os.environ.get(paramdef.envvar)) is not None:
        return env

    return None


def _get_service(params: ConnMapping) -> dict[str, str] | None:
    """
    Return the parameters of the service requested, if any.

    Look for the service in the file specified by ``PGSERVICEFILE``, or in the
    user and in the system service files, following libpq.
    """
    name = get_param(params, "service")
    if not name:
        return None

    # Same order as libpq: PGSERVICEFILE or the user file, then the system
    # file if the service wasn't found there.
    if fn := os.environ.get("PGSERVICEFILE"):
        pass
    elif sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        fn = os.path.join(appdata, "postgresql", ".pg_service.conf") if appdata else ""
    else:
        home = os.path.expanduser("~")
        fn = os.path.join(home, ".pg_service.conf") if home != "~" else ""

    if fn and (rv := _read_service_file(fn, name)) is not None:
        return rv

    # libpq uses a compile-time SYSCONFDIR if PGSYSCONFDIR is not set. That
    # value is not known from Python, so in that case the system file is not
    # read.
    if sysconf := os.environ.get("PGSYSCONFDIR"):
        return _read_service_file(os.path.join(sysconf, "pg_service.conf"), name)

    return None


def _read_service_file(filename: str, service: str) -> dict[str, str] | None:
    """
    Return the parameters of a service in a file, None if not found.
    """
    try:
        mtime = os.stat(filename).st_mtime_ns
    except OSError:
        return None
    return _parse_service_file(filename, mtime).get(service)


@lru_cache(maxsize=8)
def _parse_service_file(filename: str, mtime: int) -> dict[str, dict[str, str]]:
    # The mtime is only used to invalidate the cache if the file changes.
    rv: dict[str, dict[str, str]] = {}
    try:
        with open(filename, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return rv

    # Whitespace as in libpq: trim the line, not the key or the value.
    ws = " \t\n\r\f\v"
    current: dict[str, str] | None = None
    for line in lines:
        line = line.strip(ws)
        if not line or line.startswith("#"):
            continue
        if line.startswith("["):
            # As in libpq, the first definition of a service wins.
            name, sep, _ = line[1:].partition("]")
            if not sep:
                current = {}  # malformed header: skip its content
            else:
                current = {} if name in rv else rv.setdefault(name, {})
            continue
        if current is not None and "=" in line:
            key, val = line.split("=", 1)
            current.setdefault(key, val)

    return rv


@dataclass
class ParamDef:
    """
    Information about defaults and env vars for connection params
    """

    keyword: str
    envvar: str
    compiled: str | None


def get_param_def(keyword: str, _cache: dict[str, ParamDef] = {}) -> ParamDef | None:
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


@lru_cache
def is_ip_address(s: str) -> bool:
    """Return True if the string represent a valid ip address."""
    try:
        ip_address(s)
    except ValueError:
        return False
    return True


def gssapi_requested(params: ConnDict) -> bool:
    """Return `true` if `gssencmode` was specified explicitly."""
    return bool(get_param(params, "gssencmode"))
