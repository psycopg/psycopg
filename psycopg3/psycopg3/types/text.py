"""
Adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Optional, Union, TYPE_CHECKING

from ..pq import Escaping
from ..oids import builtins, INVALID_OID
from ..adapt import Dumper, Loader, Format
from ..proto import AdaptContext
from ..errors import DataError

if TYPE_CHECKING:
    from ..pq.proto import Escaping as EscapingProto


class _StringDumper(Dumper):

    _encoding = "utf-8"

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)

        conn = self.connection
        if conn:
            enc = conn.client_encoding
            if enc != "ascii":
                self._encoding = enc


@Dumper.builtin(str)
class StringBinaryDumper(_StringDumper):

    format = Format.BINARY

    def dump(self, obj: str) -> bytes:
        # the server will raise DataError subclass if the string contains 0x00
        return obj.encode(self._encoding)


@Dumper.builtin(str)
class StringDumper(_StringDumper):

    format = Format.TEXT

    def dump(self, obj: str) -> bytes:
        if "\x00" in obj:
            raise DataError(
                "PostgreSQL text fields cannot contain NUL (0x00) bytes"
            )
        else:
            return obj.encode(self._encoding)


@Loader.builtin(INVALID_OID, "bpchar", "name", "text", "varchar")
class TextLoader(Loader):

    format = Format.TEXT
    _encoding = "utf-8"

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        conn = self.connection
        if conn:
            enc = conn.client_encoding
            self._encoding = enc if enc != "ascii" else ""

    def load(self, data: bytes) -> Union[bytes, str]:
        if self._encoding:
            return data.decode(self._encoding)
        else:
            # return bytes for SQL_ASCII db
            return data


@Loader.builtin("bpchar", "name", "text", "varchar")
class TextBinaryLoader(TextLoader):

    format = Format.BINARY


@Dumper.builtin(bytes, bytearray, memoryview)
class BytesDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["bytea"].oid

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._esc = Escaping(
            self.connection.pgconn if self.connection else None
        )

    def dump(self, obj: bytes) -> memoryview:
        # TODO: mypy doesn't complain, but this function has the wrong signature
        # probably dump return value should be extended to Buffer
        return self._esc.escape_bytea(obj)


@Dumper.builtin(bytes, bytearray, memoryview)
class BytesBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = builtins["bytea"].oid

    def dump(
        self, obj: Union[bytes, bytearray, memoryview]
    ) -> Union[bytes, bytearray, memoryview]:
        # TODO: mypy doesn't complain, but this function has the wrong signature
        return obj


@Loader.builtin("bytea")
class ByteaLoader(Loader):

    format = Format.TEXT
    _escaping: "EscapingProto"

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        if not hasattr(self.__class__, "_escaping"):
            self.__class__._escaping = Escaping()

    def load(self, data: bytes) -> bytes:
        return self._escaping.unescape_bytea(data)


@Loader.builtin("bytea", INVALID_OID)
class ByteaBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: bytes) -> bytes:
        return data
