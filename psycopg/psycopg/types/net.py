"""
Adapters for network types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Callable, Optional, Type, Union, TYPE_CHECKING

from .. import postgres
from ..pq import Format
from ..abc import AdaptContext
from ..adapt import Buffer, Dumper, Loader

if TYPE_CHECKING:
    import ipaddress

Address = Union["ipaddress.IPv4Address", "ipaddress.IPv6Address"]
Interface = Union["ipaddress.IPv4Interface", "ipaddress.IPv6Interface"]
Network = Union["ipaddress.IPv4Network", "ipaddress.IPv6Network"]

# These objects will be imported lazily
imported = False
ip_address: Callable[[str], Address]
ip_interface: Callable[[str], Interface]
ip_network: Callable[[str], Network]
IPv4Address: "Type[ipaddress.IPv4Address]"
IPv6Address: "Type[ipaddress.IPv6Address]"
IPv4Interface: "Type[ipaddress.IPv4Interface]"
IPv6Interface: "Type[ipaddress.IPv6Interface]"
IPv4Network: "Type[ipaddress.IPv4Network]"
IPv6Network: "Type[ipaddress.IPv6Network]"

PGSQL_AF_INET = 2
PGSQL_AF_INET6 = 3
IPV4_PREFIXLEN = 32
IPV6_PREFIXLEN = 128


class InterfaceDumper(Dumper):

    oid = postgres.types["inet"].oid

    def dump(self, obj: Interface) -> bytes:
        return str(obj).encode("utf8")


class NetworkDumper(Dumper):

    oid = postgres.types["cidr"].oid

    def dump(self, obj: Network) -> bytes:
        return str(obj).encode("utf8")


class AddressBinaryDumper(Dumper):

    format = Format.BINARY
    oid = postgres.types["inet"].oid

    def dump(self, obj: Address) -> bytes:
        packed = obj.packed
        family = PGSQL_AF_INET if obj.version == 4 else PGSQL_AF_INET6
        head = bytes((family, obj.max_prefixlen, 0, len(packed)))
        return head + packed


class InterfaceBinaryDumper(Dumper):

    format = Format.BINARY
    oid = postgres.types["inet"].oid

    def dump(self, obj: Interface) -> bytes:
        packed = obj.packed
        family = PGSQL_AF_INET if obj.version == 4 else PGSQL_AF_INET6
        head = bytes((family, obj.network.prefixlen, 0, len(packed)))
        return head + packed


class NetworkBinaryDumper(Dumper):

    format = Format.BINARY
    oid = postgres.types["cidr"].oid

    def dump(self, obj: Network) -> bytes:
        packed = obj.network_address.packed
        family = PGSQL_AF_INET if obj.version == 4 else PGSQL_AF_INET6
        head = bytes((family, obj.prefixlen, 1, len(packed)))
        return head + packed


class _LazyIpaddress(Loader):
    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        global imported, ip_address, ip_interface, ip_network
        global IPv4Address, IPv6Address, IPv4Interface, IPv6Interface
        global IPv4Network, IPv6Network

        if not imported:
            from ipaddress import ip_address, ip_interface, ip_network
            from ipaddress import IPv4Address, IPv6Address
            from ipaddress import IPv4Interface, IPv6Interface
            from ipaddress import IPv4Network, IPv6Network

            imported = True


class InetLoader(_LazyIpaddress):
    def load(self, data: Buffer) -> Union[Address, Interface]:
        if isinstance(data, memoryview):
            data = bytes(data)

        if b"/" in data:
            return ip_interface(data.decode("utf8"))
        else:
            return ip_address(data.decode("utf8"))


class InetBinaryLoader(_LazyIpaddress):

    format = Format.BINARY

    def load(self, data: Buffer) -> Union[Address, Interface]:
        if isinstance(data, memoryview):
            data = bytes(data)

        prefix = data[1]
        packed = data[4:]
        if data[0] == PGSQL_AF_INET:
            if prefix == IPV4_PREFIXLEN:
                return IPv4Address(packed)
            else:
                return IPv4Interface((packed, prefix))
        else:
            if prefix == IPV6_PREFIXLEN:
                return IPv6Address(packed)
            else:
                return IPv6Interface((packed, prefix))


class CidrLoader(_LazyIpaddress):
    def load(self, data: Buffer) -> Network:
        if isinstance(data, memoryview):
            data = bytes(data)

        return ip_network(data.decode("utf8"))


class CidrBinaryLoader(_LazyIpaddress):

    format = Format.BINARY

    def load(self, data: Buffer) -> Network:
        if isinstance(data, memoryview):
            data = bytes(data)

        prefix = data[1]
        packed = data[4:]
        if data[0] == PGSQL_AF_INET:
            return IPv4Network((packed, prefix))
        else:
            return IPv6Network((packed, prefix))

        return ip_network(data.decode("utf8"))


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters
    adapters.register_dumper("ipaddress.IPv4Address", InterfaceDumper)
    adapters.register_dumper("ipaddress.IPv6Address", InterfaceDumper)
    adapters.register_dumper("ipaddress.IPv4Interface", InterfaceDumper)
    adapters.register_dumper("ipaddress.IPv6Interface", InterfaceDumper)
    adapters.register_dumper("ipaddress.IPv4Network", NetworkDumper)
    adapters.register_dumper("ipaddress.IPv6Network", NetworkDumper)
    adapters.register_dumper("ipaddress.IPv4Address", AddressBinaryDumper)
    adapters.register_dumper("ipaddress.IPv6Address", AddressBinaryDumper)
    adapters.register_dumper("ipaddress.IPv4Interface", InterfaceBinaryDumper)
    adapters.register_dumper("ipaddress.IPv6Interface", InterfaceBinaryDumper)
    adapters.register_dumper("ipaddress.IPv4Network", NetworkBinaryDumper)
    adapters.register_dumper("ipaddress.IPv6Network", NetworkBinaryDumper)
    adapters.register_loader("inet", InetLoader)
    adapters.register_loader("inet", InetBinaryLoader)
    adapters.register_loader("cidr", CidrLoader)
    adapters.register_loader("cidr", CidrBinaryLoader)
