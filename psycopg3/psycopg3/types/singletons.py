"""
Adapters for None and boolean.
"""

# Copyright (C) 2020 The Psycopg Team

from ..oids import builtins
from ..adapt import Dumper, Loader

BOOL_OID = builtins["bool"].oid


@Dumper.text(bool)
class BoolDumper(Dumper):

    oid = BOOL_OID

    def dump(self, obj: bool) -> bytes:
        return b"t" if obj else b"f"

    def quote(self, obj: bool) -> bytes:
        return b"true" if obj else b"false"


@Dumper.binary(bool)
class BoolBinaryDumper(Dumper):

    oid = BOOL_OID

    def dump(self, obj: bool) -> bytes:
        return b"\x01" if obj else b"\x00"


@Dumper.text(type(None))
class NoneDumper(Dumper):
    """
    Not a complete dumper as it doesn't implement dump(), but it implements
    quote(), so it can be used in sql composition.
    """

    def quote(self, obj: None) -> bytes:
        return b"NULL"


@Loader.text(builtins["bool"].oid)
class BoolLoader(Loader):
    def load(self, data: bytes) -> bool:
        return data == b"t"


@Loader.binary(builtins["bool"].oid)
class BoolBinaryLoader(Loader):
    def load(self, data: bytes) -> bool:
        return data != b"\x00"
