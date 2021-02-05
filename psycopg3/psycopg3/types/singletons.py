"""
Adapters for None and boolean.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader


class BoolDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["bool"].oid

    def dump(self, obj: bool) -> bytes:
        return b"t" if obj else b"f"

    def quote(self, obj: bool) -> bytes:
        return b"true" if obj else b"false"


class BoolBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = builtins["bool"].oid

    def dump(self, obj: bool) -> bytes:
        return b"\x01" if obj else b"\x00"


class NoneDumper(Dumper):
    """
    Not a complete dumper as it doesn't implement dump(), but it implements
    quote(), so it can be used in sql composition.
    """

    format = Format.TEXT

    def dump(self, obj: None) -> bytes:
        raise NotImplementedError("NULL is passed to Postgres in other ways")

    def quote(self, obj: None) -> bytes:
        return b"NULL"


class BoolLoader(Loader):

    format = Format.TEXT

    def load(self, data: Buffer) -> bool:
        return data == b"t"


class BoolBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> bool:
        return data != b"\x00"
