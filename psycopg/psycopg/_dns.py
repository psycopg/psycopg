# type: ignore  # dnspython is currently optional and mypy fails if missing
"""
DNS query support
"""

# Copyright (C) 2021 The Psycopg Team

import os
import re
from random import randint
from typing import Any, DefaultDict, Dict, List, NamedTuple, Optional, Sequence
from typing import TYPE_CHECKING
from functools import lru_cache
from ipaddress import ip_address
from collections import defaultdict

try:
    from dns.resolver import Resolver, Cache
    from dns.asyncresolver import Resolver as AsyncResolver
    from dns.exception import DNSException
except ImportError:
    raise ImportError(
        "the module psycopg._dns requires the package 'dnspython' installed"
    )

from . import pq
from . import errors as e

if TYPE_CHECKING:
    from dns.rdtypes.IN.SRV import SRV

resolver = Resolver()
resolver.cache = Cache()

async_resolver = AsyncResolver()
async_resolver.cache = Cache()


async def resolve_hostaddr_async(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform async DNS lookup of the hosts and return a new params dict.
    """
    hostaddr_arg = params.get("hostaddr", os.environ.get("PGHOSTADDR", ""))
    if hostaddr_arg:
        # Already resolved
        return params

    if pq.version() < 100000:
        # hostaddr not supported
        return params

    host_arg: str = params.get("host", os.environ.get("PGHOST", ""))
    if not host_arg:
        # Nothing to resolve
        return params

    hosts_in = host_arg.split(",")
    port_arg: str = str(params.get("port", os.environ.get("PGPORT", "")))
    ports_in = port_arg.split(",") if port_arg else []

    if len(ports_in) == 1:
        # If only one port is specified, the libpq will apply it to all
        # the hosts, so don't mangle it.
        del ports_in[:]
    elif len(ports_in) > 1:
        if len(ports_in) != len(hosts_in):
            # ProgrammingError would have been more appropriate, but this is
            # what the raise if the libpq fails connect in the same case.
            raise e.OperationalError(
                f"cannot match {len(hosts_in)} hosts with {len(ports_in)} port numbers"
            )
        ports_out = []

    hosts_out = []
    hostaddr_out = []
    for i, host in enumerate(hosts_in):
        if not host or host.startswith("/") or host[1:2] == ":":
            # Local path
            hosts_out.append(host)
            hostaddr_out.append("")
            if ports_in:
                ports_out.append(ports_in[i])
            continue

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


def resolve_srv(params: Dict[str, Any]) -> Dict[str, Any]:
    """Apply SRV DNS lookup as defined in :RFC:`2782`."""
    return Rfc2782Resolver().resolve(params)


async def resolve_srv_async(params: Dict[str, Any]) -> Dict[str, Any]:
    """Async equivalent of `resolve_srv()`."""
    return await Rfc2782Resolver().resolve_async(params)


class HostPort(NamedTuple):
    host: str
    port: str
    totry: bool = False
    target: Optional[str] = None


class Rfc2782Resolver:
    """Implement SRV RR Resolution as per RFC 2782

    The class is organised to minimise code duplication between the sync and
    the async paths.
    """

    re_srv_rr = re.compile(r"^(?P<service>_[^\.]+)\.(?P<proto>_[^\.]+)\.(?P<target>.+)")

    def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Update the parameters host and port after SRV lookup."""
        attempts = self._get_attempts(params)
        if not attempts:
            return params

        hps = []
        for hp in attempts:
            if hp.totry:
                hps.extend(self._resolve_srv(hp))
            else:
                hps.append(hp)

        return self._return_params(params, hps)

    async def resolve_async(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Update the parameters host and port after SRV lookup."""
        attempts = self._get_attempts(params)
        if not attempts:
            return params

        hps = []
        for hp in attempts:
            if hp.totry:
                hps.extend(await self._resolve_srv_async(hp))
            else:
                hps.append(hp)

        return self._return_params(params, hps)

    def _get_attempts(self, params: Dict[str, Any]) -> List[HostPort]:
        """
        Return the list of host, and for each host if SRV lookup must be tried.

        Return an empty list if no lookup is requested.
        """
        # If hostaddr is defined don't do any resolution.
        if params.get("hostaddr", os.environ.get("PGHOSTADDR", "")):
            return []

        host_arg: str = params.get("host", os.environ.get("PGHOST", ""))
        hosts_in = host_arg.split(",")
        port_arg: str = str(params.get("port", os.environ.get("PGPORT", "")))
        ports_in = port_arg.split(",")

        if len(ports_in) == 1:
            # If only one port is specified, it applies to all the hosts.
            ports_in *= len(hosts_in)
        if len(ports_in) != len(hosts_in):
            # ProgrammingError would have been more appropriate, but this is
            # what the raise if the libpq fails connect in the same case.
            raise e.OperationalError(
                f"cannot match {len(hosts_in)} hosts with {len(ports_in)} port numbers"
            )

        out = []
        srv_found = False
        for host, port in zip(hosts_in, ports_in):
            m = self.re_srv_rr.match(host)
            if m or port.lower() == "srv":
                srv_found = True
                target = m.group("target") if m else None
                hp = HostPort(host=host, port=port, totry=True, target=target)
            else:
                hp = HostPort(host=host, port=port)
            out.append(hp)

        return out if srv_found else []

    def _resolve_srv(self, hp: HostPort) -> List[HostPort]:
        try:
            ans = resolver.resolve(hp.host, "SRV")
        except DNSException:
            ans = ()
        return self._get_solved_entries(hp, ans)

    async def _resolve_srv_async(self, hp: HostPort) -> List[HostPort]:
        try:
            ans = await async_resolver.resolve(hp.host, "SRV")
        except DNSException:
            ans = ()
        return self._get_solved_entries(hp, ans)

    def _get_solved_entries(
        self, hp: HostPort, entries: "Sequence[SRV]"
    ) -> List[HostPort]:
        if not entries:
            # No SRV entry found. Delegate the libpq a QNAME=target lookup
            if hp.target and hp.port.lower() != "srv":
                return [HostPort(host=hp.target, port=hp.port)]
            else:
                return []

        # If there is precisely one SRV RR, and its Target is "." (the root
        # domain), abort.
        if len(entries) == 1 and str(entries[0].target) == ".":
            return []

        return [
            HostPort(host=str(entry.target).rstrip("."), port=str(entry.port))
            for entry in self.sort_rfc2782(entries)
        ]

    def _return_params(
        self, params: Dict[str, Any], hps: List[HostPort]
    ) -> Dict[str, Any]:
        if not hps:
            # Nothing found, we ended up with an empty list
            raise e.OperationalError("no host found after SRV RR lookup")

        out = params.copy()
        out["host"] = ",".join(hp.host for hp in hps)
        out["port"] = ",".join(str(hp.port) for hp in hps)
        return out

    def sort_rfc2782(self, ans: "Sequence[SRV]") -> "List[SRV]":
        """
        Implement the priority/weight ordering defined in RFC 2782.
        """
        # Divide the entries by priority:
        priorities: DefaultDict[int, "List[SRV]"] = defaultdict(list)
        out: "List[SRV]" = []
        for entry in ans:
            priorities[entry.priority].append(entry)

        for pri, entries in sorted(priorities.items()):
            if len(entries) == 1:
                out.append(entries[0])
                continue

            entries.sort(key=lambda ent: ent.weight)
            total_weight = sum(ent.weight for ent in entries)
            while entries:
                r = randint(0, total_weight)
                csum = 0
                for i, ent in enumerate(entries):
                    csum += ent.weight
                    if csum >= r:
                        break
                out.append(ent)
                total_weight -= ent.weight
                del entries[i]

        return out


@lru_cache()
def is_ip_address(s: str) -> bool:
    """Return True if the string represent a valid ip address."""
    try:
        ip_address(s)
    except ValueError:
        return False
    return True
