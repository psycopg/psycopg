"""
Adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Union, TYPE_CHECKING

from ..pq import Escaping
from ..oids import builtins, INVALID_OID
from ..adapt import Dumper, Loader
from ..proto import AdaptContext
from ..errors import DataError

if TYPE_CHECKING:
    from ..pq.proto import Escaping as EscapingProto


class _StringDumper(Dumper):
    def __init__(self, src: type, context: AdaptContext):
        super().__init__(src, context)

        if self.connection:
            if self.connection.client_encoding != "SQL_ASCII":
                self.encoding = self.connection.pyenc
            else:
                self.encoding = "utf-8"
        else:
            self.encoding = "utf-8"


@Dumper.binary(str)
class StringBinaryDumper(_StringDumper):
    def dump(self, obj: str) -> bytes:
        return obj.encode(self.encoding)


@Dumper.text(str)
class StringDumper(_StringDumper):
    def dump(self, obj: str) -> bytes:
        if "\x00" in obj:
            raise DataError(
                "PostgreSQL text fields cannot contain NUL (0x00) bytes"
            )
        else:
            return obj.encode(self.encoding)


@Loader.text(builtins["text"].oid)
@Loader.binary(builtins["text"].oid)
@Loader.text(builtins["varchar"].oid)
@Loader.binary(builtins["varchar"].oid)
@Loader.text(INVALID_OID)
class TextLoader(Loader):
    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)

        if self.connection is not None:
            if self.connection.client_encoding != "SQL_ASCII":
                self.encoding = self.connection.pyenc
            else:
                self.encoding = ""
        else:
            self.encoding = "utf-8"

    def load(self, data: bytes) -> Union[bytes, str]:
        if self.encoding:
            return data.decode(self.encoding)
        else:
            # return bytes for SQL_ASCII db
            return data


@Loader.text(builtins["name"].oid)
@Loader.binary(builtins["name"].oid)
@Loader.text(builtins["bpchar"].oid)
@Loader.binary(builtins["bpchar"].oid)
class UnknownLoader(Loader):
    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)
        self.encoding = self.connection.pyenc if self.connection else "utf-8"

    def load(self, data: bytes) -> str:
        return data.decode(self.encoding)


@Dumper.text(bytes)
class BytesDumper(Dumper):

    oid = builtins["bytea"].oid

    def __init__(self, src: type, context: AdaptContext = None):
        super().__init__(src, context)
        self.esc = Escaping(
            self.connection.pgconn if self.connection is not None else None
        )

    def dump(self, obj: bytes) -> bytes:
        return self.esc.escape_bytea(obj)


@Dumper.binary(bytes)
class BytesBinaryDumper(Dumper):

    oid = builtins["bytea"].oid

    def dump(self, b: bytes) -> bytes:
        return b


@Loader.text(builtins["bytea"].oid)
class ByteaLoader(Loader):
    _escaping: "EscapingProto"

    def __init__(self, oid: int, context: AdaptContext = None):
        super().__init__(oid, context)
        if not hasattr(self.__class__, "_escaping"):
            self.__class__._escaping = Escaping()

    def load(self, data: bytes) -> bytes:
        return self._escaping.unescape_bytea(data)


@Loader.binary(builtins["bytea"].oid)
@Loader.binary(INVALID_OID)
class ByteaBinaryLoader(Loader):
    def load(self, data: bytes) -> bytes:
        return data
