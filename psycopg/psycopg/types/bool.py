"""
Adapters for booleans.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader
from ..proto import AdaptContext


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


class BoolLoader(Loader):

    format = Format.TEXT

    def load(self, data: Buffer) -> bool:
        return data == b"t"


class BoolBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> bool:
        return data != b"\x00"


def register_default_globals(ctx: AdaptContext) -> None:
    BoolDumper.register(bool, ctx)
    BoolBinaryDumper.register(bool, ctx)
    BoolLoader.register("bool", ctx)
    BoolBinaryLoader.register("bool", ctx)
