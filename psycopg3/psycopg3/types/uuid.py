"""
Adapters for the UUID type.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Callable, Optional, TYPE_CHECKING

from ..oids import builtins
from ..adapt import Dumper, Loader
from ..proto import AdaptContext

if TYPE_CHECKING:
    import uuid

# Importing the uuid module is slow, so import it only on request.
UUID: Callable[..., "uuid.UUID"]


@Dumper.text("uuid.UUID")
class UUIDDumper(Dumper):

    _oid = builtins["uuid"].oid

    def dump(self, obj: "uuid.UUID") -> bytes:
        return obj.hex.encode("utf8")


@Dumper.binary("uuid.UUID")
class UUIDBinaryDumper(UUIDDumper):
    def dump(self, obj: "uuid.UUID") -> bytes:
        return obj.bytes


@Loader.text(builtins["uuid"].oid)
class UUIDLoader(Loader):
    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        global UUID
        from uuid import UUID

    def load(self, data: bytes) -> "uuid.UUID":
        return UUID(data.decode("utf8"))


@Loader.binary(builtins["uuid"].oid)
class UUIDBinaryLoader(UUIDLoader):
    def load(self, data: bytes) -> "uuid.UUID":
        return UUID(bytes=data)
