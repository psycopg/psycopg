"""
Adapters for the UUID type.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Callable, Optional, TYPE_CHECKING

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader
from ..proto import AdaptContext

if TYPE_CHECKING:
    import uuid

# Importing the uuid module is slow, so import it only on request.
imported = False
UUID: Callable[..., "uuid.UUID"]


class UUIDDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["uuid"].oid

    def dump(self, obj: "uuid.UUID") -> bytes:
        return obj.hex.encode("utf8")


class UUIDBinaryDumper(UUIDDumper):

    format = Format.BINARY

    def dump(self, obj: "uuid.UUID") -> bytes:
        return obj.bytes


class UUIDLoader(Loader):

    format = Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        global imported, UUID
        if not imported:
            from uuid import UUID

            imported = True

    def load(self, data: Buffer) -> "uuid.UUID":
        if isinstance(data, memoryview):
            data = bytes(data)
        return UUID(data.decode("utf8"))


class UUIDBinaryLoader(UUIDLoader):

    format = Format.BINARY

    def load(self, data: Buffer) -> "uuid.UUID":
        if isinstance(data, memoryview):
            data = bytes(data)
        return UUID(bytes=data)
