"""
Adapters for None and boolean.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Dict

from ..adapt import Dumper, Loader
from .oids import builtins

BOOL_OID = builtins["bool"].oid


@Dumper.text(bool)
class TextBoolDumper(Dumper):
    def dump(self, obj: bool) -> bytes:
        return b"t" if obj else b"f"

    def quote(self, obj: bool) -> bytes:
        return b"true" if obj else b"false"

    @property
    def oid(self) -> int:
        return BOOL_OID


@Dumper.binary(bool)
class BinaryBoolDumper(Dumper):
    def dump(self, obj: bool) -> bytes:
        return b"\x01" if obj else b"\x00"

    @property
    def oid(self) -> int:
        return BOOL_OID


@Dumper.text(type(None))
class NoneDumper(Dumper):
    """
    Not a complete dumper as it doesn't implement dump(), but it implements
    quote(), so it can be used in sql composition.
    """

    def quote(self, obj: None) -> bytes:
        return b"NULL"


@Loader.text(builtins["bool"].oid)
class TextBoolLoader(Loader):
    def load(
        self,
        data: bytes,
        __values: Dict[bytes, bool] = {b"t": True, b"f": False},
    ) -> bool:
        return __values[data]


@Loader.binary(builtins["bool"].oid)
class BinaryBoolLoader(Loader):
    def load(
        self,
        data: bytes,
        __values: Dict[bytes, bool] = {b"\x01": True, b"\x00": False},
    ) -> bool:
        return __values[data]
