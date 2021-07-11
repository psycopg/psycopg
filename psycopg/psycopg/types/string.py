"""
Adapters for textual types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Optional, Union, TYPE_CHECKING

from .. import postgres
from ..pq import Format, Escaping
from ..adapt import Buffer, Dumper, Loader
from ..proto import AdaptContext
from ..errors import DataError

if TYPE_CHECKING:
    from ..pq.proto import Escaping as EscapingProto


class _StrDumper(Dumper):

    _encoding = "utf-8"

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)

        conn = self.connection
        if conn:
            enc = conn.client_encoding
            if enc != "ascii":
                self._encoding = enc


class StrBinaryDumper(_StrDumper):

    format = Format.BINARY
    _oid = postgres.types["text"].oid

    def dump(self, obj: str) -> bytes:
        # the server will raise DataError subclass if the string contains 0x00
        return obj.encode(self._encoding)


class StrDumper(_StrDumper):

    format = Format.TEXT

    def dump(self, obj: str) -> bytes:
        if "\x00" in obj:
            raise DataError(
                "PostgreSQL text fields cannot contain NUL (0x00) bytes"
            )
        else:
            return obj.encode(self._encoding)


class TextLoader(Loader):

    format = Format.TEXT
    _encoding = "utf-8"

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        conn = self.connection
        if conn:
            enc = conn.client_encoding
            self._encoding = enc if enc != "ascii" else ""

    def load(self, data: Buffer) -> Union[bytes, str]:
        if self._encoding:
            if isinstance(data, memoryview):
                return bytes(data).decode(self._encoding)
            else:
                return data.decode(self._encoding)
        else:
            # return bytes for SQL_ASCII db
            return data


class TextBinaryLoader(TextLoader):

    format = Format.BINARY


class BytesDumper(Dumper):

    format = Format.TEXT
    _oid = postgres.types["bytea"].oid

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._esc = Escaping(
            self.connection.pgconn if self.connection else None
        )

    def dump(self, obj: bytes) -> memoryview:
        # TODO: mypy doesn't complain, but this function has the wrong signature
        # probably dump return value should be extended to Buffer
        return self._esc.escape_bytea(obj)


class BytesBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = postgres.types["bytea"].oid

    def dump(
        self, obj: Union[bytes, bytearray, memoryview]
    ) -> Union[bytes, bytearray, memoryview]:
        # TODO: mypy doesn't complain, but this function has the wrong signature
        return obj


class ByteaLoader(Loader):

    format = Format.TEXT
    _escaping: "EscapingProto"

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        if not hasattr(self.__class__, "_escaping"):
            self.__class__._escaping = Escaping()

    def load(self, data: Buffer) -> bytes:
        return self._escaping.unescape_bytea(data)


class ByteaBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> bytes:
        return data


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    # NOTE: the order the dumpers are registered is relevant.
    # The last one registered becomes the default for each type.
    # Normally, binary is the default dumper, except for text (which plays
    # the role of unknown, so it can be cast automatically to other types).
    adapters.register_dumper(str, StrBinaryDumper)
    adapters.register_dumper(str, StrDumper)
    adapters.register_loader(postgres.INVALID_OID, TextLoader)
    adapters.register_loader("bpchar", TextLoader)
    adapters.register_loader("name", TextLoader)
    adapters.register_loader("text", TextLoader)
    adapters.register_loader("varchar", TextLoader)
    adapters.register_loader("bpchar", TextBinaryLoader)
    adapters.register_loader("name", TextBinaryLoader)
    adapters.register_loader("text", TextBinaryLoader)
    adapters.register_loader("varchar", TextBinaryLoader)

    adapters.register_dumper(bytes, BytesDumper)
    adapters.register_dumper(bytearray, BytesDumper)
    adapters.register_dumper(memoryview, BytesDumper)
    adapters.register_dumper(bytes, BytesBinaryDumper)
    adapters.register_dumper(bytearray, BytesBinaryDumper)
    adapters.register_dumper(memoryview, BytesBinaryDumper)
    adapters.register_loader("bytea", ByteaLoader)
    adapters.register_loader(postgres.INVALID_OID, ByteaBinaryLoader)
    adapters.register_loader("bytea", ByteaBinaryLoader)
