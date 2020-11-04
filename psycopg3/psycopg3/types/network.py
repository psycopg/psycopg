"""
Adapters for network types.
"""

# Copyright (C) 2020 The Psycopg Team

# TODO: consiter lazy dumper registration.
from ipaddress import ip_address, ip_interface, ip_network
from ipaddress import IPv4Address, IPv4Interface, IPv4Network
from ipaddress import IPv6Address, IPv6Interface, IPv6Network

from typing import cast, Union

from ..oids import builtins
from ..adapt import Dumper, Loader
from ..utils.codecs import encode_ascii, decode_ascii

Address = Union[IPv4Address, IPv6Address]
Interface = Union[IPv4Interface, IPv6Interface]
Network = Union[IPv4Network, IPv6Network]


@Dumper.text(IPv4Address)
@Dumper.text(IPv6Address)
@Dumper.text(IPv4Interface)
@Dumper.text(IPv6Interface)
class InterfaceDumper(Dumper):

    oid = builtins["inet"].oid

    def dump(self, obj: Interface) -> bytes:
        return encode_ascii(str(obj))[0]


@Dumper.text(IPv4Network)
@Dumper.text(IPv6Network)
class NetworkDumper(Dumper):

    oid = builtins["cidr"].oid

    def dump(self, obj: Network) -> bytes:
        return encode_ascii(str(obj))[0]


@Loader.text(builtins["inet"].oid)
class InetLoader(Loader):
    def load(self, data: bytes) -> Union[Address, Interface]:
        if b"/" in data:
            return cast(Interface, ip_interface(decode_ascii(data)[0]))
        else:
            return cast(Address, ip_address(decode_ascii(data)[0]))


@Loader.text(builtins["cidr"].oid)
class CidrLoader(Loader):
    def load(self, data: bytes) -> Network:
        return cast(Network, ip_network(decode_ascii(data)[0]))
