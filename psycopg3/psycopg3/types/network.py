"""
Adapters for network types.
"""

# Copyright (C) 2020 The Psycopg Team

# TODO: consiter lazy dumper registration.
import ipaddress
from ipaddress import IPv4Address, IPv4Interface, IPv4Network
from ipaddress import IPv6Address, IPv6Interface, IPv6Network

from typing import cast, Callable, Union

from ..oids import builtins
from ..adapt import Dumper, Loader

Address = Union[IPv4Address, IPv6Address]
Interface = Union[IPv4Interface, IPv6Interface]
Network = Union[IPv4Network, IPv6Network]

# in typeshed these types are commented out
ip_address = cast(Callable[[str], Address], ipaddress.ip_address)
ip_interface = cast(Callable[[str], Interface], ipaddress.ip_interface)
ip_network = cast(Callable[[str], Network], ipaddress.ip_network)


@Dumper.text(IPv4Address)
@Dumper.text(IPv6Address)
@Dumper.text(IPv4Interface)
@Dumper.text(IPv6Interface)
class InterfaceDumper(Dumper):

    oid = builtins["inet"].oid

    def dump(self, obj: Interface) -> bytes:
        return str(obj).encode("utf8")


@Dumper.text(IPv4Network)
@Dumper.text(IPv6Network)
class NetworkDumper(Dumper):

    oid = builtins["cidr"].oid

    def dump(self, obj: Network) -> bytes:
        return str(obj).encode("utf8")


@Loader.text(builtins["inet"].oid)
class InetLoader(Loader):
    def load(self, data: bytes) -> Union[Address, Interface]:
        if b"/" in data:
            return ip_interface(data.decode("utf8"))
        else:
            return ip_address(data.decode("utf8"))


@Loader.text(builtins["cidr"].oid)
class CidrLoader(Loader):
    def load(self, data: bytes) -> Network:
        return ip_network(data.decode("utf8"))
