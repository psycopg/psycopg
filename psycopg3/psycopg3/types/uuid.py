"""
Adapters for the UUID type.
"""

# Copyright (C) 2020 The Psycopg Team

# TODO: importing uuid is slow. Don't import it at module level.
# Should implement lazy dumper registration.
from uuid import UUID

from ..oids import builtins
from ..adapt import Dumper, Loader


@Dumper.text(UUID)
class UUIDDumper(Dumper):

    oid = builtins["uuid"].oid

    def dump(self, obj: UUID) -> bytes:
        return obj.hex.encode("utf8")


@Dumper.binary(UUID)
class UUIDBinaryDumper(UUIDDumper):
    def dump(self, obj: UUID) -> bytes:
        return obj.bytes


@Loader.text(builtins["uuid"].oid)
class UUIDLoader(Loader):
    def load(self, data: bytes) -> UUID:
        return UUID(data.decode("utf8"))


@Loader.binary(builtins["uuid"].oid)
class UUIDBinaryLoader(Loader):
    def load(self, data: bytes) -> UUID:
        return UUID(bytes=data)
