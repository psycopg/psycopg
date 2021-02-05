"""
Adapters for network types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Callable, Optional, Union, TYPE_CHECKING

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader
from ..proto import AdaptContext

if TYPE_CHECKING:
    import ipaddress

Address = Union["ipaddress.IPv4Address", "ipaddress.IPv6Address"]
Interface = Union["ipaddress.IPv4Interface", "ipaddress.IPv6Interface"]
Network = Union["ipaddress.IPv4Network", "ipaddress.IPv6Network"]

# These functions will be imported lazily
imported = False
ip_address: Callable[[str], Address]
ip_interface: Callable[[str], Interface]
ip_network: Callable[[str], Network]


class InterfaceDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["inet"].oid

    def dump(self, obj: Interface) -> bytes:
        return str(obj).encode("utf8")


class NetworkDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["cidr"].oid

    def dump(self, obj: Network) -> bytes:
        return str(obj).encode("utf8")


class _LazyIpaddress(Loader):
    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        global imported, ip_address, ip_interface, ip_network
        if not imported:
            from ipaddress import ip_address, ip_interface, ip_network

            imported = True


class InetLoader(_LazyIpaddress):

    format = Format.TEXT

    def load(self, data: Buffer) -> Union[Address, Interface]:
        if isinstance(data, memoryview):
            data = bytes(data)

        if b"/" in data:
            return ip_interface(data.decode("utf8"))
        else:
            return ip_address(data.decode("utf8"))


class CidrLoader(_LazyIpaddress):

    format = Format.TEXT

    def load(self, data: Buffer) -> Network:
        if isinstance(data, memoryview):
            data = bytes(data)

        return ip_network(data.decode("utf8"))
