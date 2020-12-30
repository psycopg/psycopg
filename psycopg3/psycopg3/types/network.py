"""
Adapters for network types.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Callable, Optional, Union, TYPE_CHECKING

from ..oids import builtins
from ..adapt import Dumper, Loader, Format
from ..proto import AdaptContext

if TYPE_CHECKING:
    import ipaddress

Address = Union["ipaddress.IPv4Address", "ipaddress.IPv6Address"]
Interface = Union["ipaddress.IPv4Interface", "ipaddress.IPv6Interface"]
Network = Union["ipaddress.IPv4Network", "ipaddress.IPv6Network"]

# These functions will be imported lazily
ip_address: Callable[[str], Address]
ip_interface: Callable[[str], Interface]
ip_network: Callable[[str], Network]


@Dumper.text("ipaddress.IPv4Address")
@Dumper.text("ipaddress.IPv6Address")
@Dumper.text("ipaddress.IPv4Interface")
@Dumper.text("ipaddress.IPv6Interface")
class InterfaceDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["inet"].oid

    def dump(self, obj: Interface) -> bytes:
        return str(obj).encode("utf8")


@Dumper.text("ipaddress.IPv4Network")
@Dumper.text("ipaddress.IPv6Network")
class NetworkDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["cidr"].oid

    def dump(self, obj: Network) -> bytes:
        return str(obj).encode("utf8")


class _LazyIpaddress(Loader):
    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        global ip_address, ip_interface, ip_network
        from ipaddress import ip_address, ip_interface, ip_network


@Loader.text(builtins["inet"].oid)
class InetLoader(_LazyIpaddress):

    format = Format.TEXT

    def load(self, data: bytes) -> Union[Address, Interface]:
        if b"/" in data:
            return ip_interface(data.decode("utf8"))
        else:
            return ip_address(data.decode("utf8"))


@Loader.text(builtins["cidr"].oid)
class CidrLoader(_LazyIpaddress):

    format = Format.TEXT

    def load(self, data: bytes) -> Network:
        return ip_network(data.decode("utf8"))
