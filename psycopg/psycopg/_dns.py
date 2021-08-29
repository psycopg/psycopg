# type: ignore  # dnspython is currently optional and mypy fails if missing
"""
DNS query support
"""

# Copyright (C) 2021 The Psycopg Team

import os
from typing import Any, Dict
from functools import lru_cache
from ipaddress import ip_address

try:
    from dns.resolver import Cache
    from dns.asyncresolver import Resolver
    from dns.exception import DNSException
except ImportError:
    raise ImportError(
        "the module psycopg._dns requires the package 'dnspython' installed"
    )

from . import pq
from . import errors as e

async_resolver = Resolver()
async_resolver.cache = Cache()


async def resolve_hostaddr_async(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform async DNS lookup of the hosts and return a new params dict.

    If a ``host`` param is present but not ``hostname``, resolve the host
    addresses dynamically.

    Change ``host``, ``hostname``, ``port`` in place to allow to connect
    without further DNS lookups (remove hosts that are not resolved, keep the
    lists consistent).

    Raise `OperationalError` if connection is not possible (e.g. no host
    resolve, inconsistent lists length).

    See `the PostgreSQL docs`__ for explanation of how these params are used,
    and how they support multiple entries.

    .. __: https://www.postgresql.org/docs/current/libpq-connect.html
           #LIBPQ-PARAMKEYWORDS

    .. warning::
        This function doesn't handle the ``/etc/hosts`` file.
    """
    host_arg: str = params.get("host", os.environ.get("PGHOST", ""))
    hostaddr_arg = params.get("hostaddr", os.environ.get("PGHOSTADDR", ""))

    if hostaddr_arg or not host_arg:
        return params

    port_arg: str = str(params.get("port", os.environ.get("PGPORT", "")))

    if pq.version() < 100000:
        # hostaddr not supported
        return params

    if host_arg.startswith("/") or host_arg[1:2] == ":":
        # Local path
        return params

    hosts_in = host_arg.split(",")
    ports_in = port_arg.split(",")
    if len(ports_in) == 1:
        # If only one port is specified, the libpq will apply it to all
        # the hosts, so don't mangle it.
        del ports_in[:]
    elif len(ports_in) > 1:
        if len(ports_in) != len(hosts_in):
            # ProgrammingError would have been more appropriate, but this is
            # what the raise if the libpq fails connect in the same case.
            raise e.OperationalError(
                f"cannot match {len(hosts_in)} hosts with {len(ports_in)}"
                " port numbers"
            )
        ports_out = []

    hosts_out = []
    hostaddr_out = []
    for i, host in enumerate(hosts_in):
        # If the host is already an ip address don't try to resolve it
        if is_ip_address(host):
            hosts_out.append(host)
            hostaddr_out.append(host)
            if ports_in:
                ports_out.append(ports_in[i])
            continue

        try:
            ans = await async_resolver.resolve(host)
        except DNSException as ex:
            # Special case localhost: on MacOS it doesn't get resolved.
            # I assue it is just resolved by /etc/hosts, which is not handled
            # by dnspython.
            if host == "localhost":
                hosts_out.append(host)
                hostaddr_out.append("127.0.0.1")
                if ports_in:
                    ports_out.append(ports_in[i])
            else:
                last_exc = ex
        else:
            for rdata in ans:
                hosts_out.append(host)
                hostaddr_out.append(rdata.address)
                if ports_in:
                    ports_out.append(ports_in[i])

    # Throw an exception if no host could be resolved
    if not hosts_out:
        raise e.OperationalError(str(last_exc))

    out = params.copy()
    out["host"] = ",".join(hosts_out)
    out["hostaddr"] = ",".join(hostaddr_out)
    if ports_in:
        out["port"] = ",".join(ports_out)

    return out


@lru_cache()
def is_ip_address(s: str) -> bool:
    """Return True if the string represent a valid ip address."""
    try:
        ip_address(s)
    except ValueError:
        return False
    return True
