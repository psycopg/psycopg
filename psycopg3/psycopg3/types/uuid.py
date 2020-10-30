"""
Adapters for the UUID type.
"""

# Copyright (C) 2020 The Psycopg Team

# TODO: importing uuid is slow. Don't import it at module level.
# Should implement lazy dumper registration.
from uuid import UUID

import codecs

from ..oids import builtins
from ..adapt import Dumper, Loader
from ..proto import DecodeFunc, EncodeFunc

_encode_ascii = codecs.lookup("ascii").encode
_decode_ascii = codecs.lookup("ascii").decode


@Dumper.text(UUID)
class UUIDDumper(Dumper):

    oid = builtins["uuid"].oid

    def dump(self, obj: UUID, __encode: EncodeFunc = _encode_ascii) -> bytes:
        return __encode(obj.hex)[0]


@Dumper.binary(UUID)
class UUIDBinaryDumper(Dumper):

    oid = builtins["uuid"].oid

    def dump(self, obj: UUID) -> bytes:
        return obj.bytes


@Loader.text(builtins["uuid"].oid)
class UUIDLoader(Loader):
    def load(self, data: bytes, __decode: DecodeFunc = _decode_ascii) -> UUID:
        return UUID(__decode(data)[0])


@Loader.binary(builtins["uuid"].oid)
class UUIDBinaryLoader(Loader):
    def load(self, data: bytes) -> UUID:
        return UUID(bytes=data)
