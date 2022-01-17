"""
Adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Optional, Union, TYPE_CHECKING

from .. import postgres
from ..pq import Format, Escaping
from ..abc import AdaptContext
from ..adapt import Buffer, Dumper, Loader
from ..errors import DataError
from .._encodings import pgconn_encoding

if TYPE_CHECKING:
    from ..pq.abc import Escaping as EscapingProto


class _BaseStrDumper(Dumper):

    _encoding = "utf-8"

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)

        conn = self.connection
        if conn:
            enc = pgconn_encoding(conn.pgconn)
            if enc != "ascii":
                self._encoding = enc


class StrBinaryDumper(_BaseStrDumper):

    format = Format.BINARY
    oid = postgres.types["text"].oid

    def dump(self, obj: str) -> bytes:
        # the server will raise DataError subclass if the string contains 0x00
        return obj.encode(self._encoding)


class _StrDumper(_BaseStrDumper):
    def dump(self, obj: str) -> bytes:
        if "\x00" in obj:
            raise DataError(
                "PostgreSQL text fields cannot contain NUL (0x00) bytes"
            )
        else:
            return obj.encode(self._encoding)


class StrDumper(_StrDumper):
    """
    Dumper for strings in text format to the text oid.

    Note that this dumper is not used by default because the type is too strict
    and PostgreSQL would require an explicit casts to everything that is not a
    text field. However it is useful where the unknown oid is ambiguous and the
    text oid is required, for instance with variadic functions.
    """

    oid = postgres.types["text"].oid


class StrDumperUnknown(_StrDumper):
    """
    Dumper for strings in text format to the unknown oid.

    This dumper is the default dumper for strings and allows to use Python
    strings to represent almost every data type. In a few places, however, the
    unknown oid is not accepted (for instance in variadic functions such as
    'concat()'). In that case either a cast on the placeholder ('%s::text) or
    the StrTextDumper should be used.
    """

    pass


class TextLoader(Loader):

    _encoding = "utf-8"

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        conn = self.connection
        if conn:
            enc = pgconn_encoding(conn.pgconn)
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

    oid = postgres.types["bytea"].oid
    _qprefix = b""

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._esc = Escaping(
            self.connection.pgconn if self.connection else None
        )

    def dump(self, obj: bytes) -> Buffer:
        return self._esc.escape_bytea(obj)

    def quote(self, obj: bytes) -> bytes:
        escaped = self.dump(obj)

        # We cannot use the base quoting because escape_bytea already returns
        # the quotes content. if scs is off it will escape the backslashes in
        # the format, otherwise it won't, but it doesn't tell us what quotes to
        # use.
        if self.connection:
            if not self._qprefix:
                scs = self.connection.pgconn.parameter_status(
                    b"standard_conforming_strings"
                )
                self._qprefix = b"'" if scs == b"on" else b" E'"

            return self._qprefix + escaped + b"'"

        # We don't have a connection, so someone is using us to generate a file
        # to use off-line or something like that. PQescapeBytea, like its
        # string counterpart, is not predictable whether it will escape
        # backslashes.
        rv: bytes = b" E'" + escaped + b"'"
        if self._esc.escape_bytea(b"\x00") == b"\\000":
            rv = rv.replace(b"\\", b"\\\\")
        return rv


class BytesBinaryDumper(Dumper):

    format = Format.BINARY
    oid = postgres.types["bytea"].oid

    def dump(self, obj: Buffer) -> Buffer:
        return obj


class ByteaLoader(Loader):

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

    # NOTE: the order the dumpers are registered is relevant. The last one
    # registered becomes the default for each type. Usually, binary is the
    # default dumper. For text we use the text dumper as default because it
    # plays the role of unknown, and it can be cast automatically to other
    # types. However, before that, we register a dumper with the text oid,
    # which will be used when a text dumper is looked up by oid.
    adapters.register_dumper(str, StrBinaryDumper)
    adapters.register_dumper(str, StrDumper)
    adapters.register_dumper(str, StrDumperUnknown)

    adapters.register_loader(postgres.INVALID_OID, TextLoader)
    adapters.register_loader("bpchar", TextLoader)
    adapters.register_loader("name", TextLoader)
    adapters.register_loader("text", TextLoader)
    adapters.register_loader("varchar", TextLoader)
    adapters.register_loader('"char"', TextLoader)
    adapters.register_loader("bpchar", TextBinaryLoader)
    adapters.register_loader("name", TextBinaryLoader)
    adapters.register_loader("text", TextBinaryLoader)
    adapters.register_loader("varchar", TextBinaryLoader)
    adapters.register_loader('"char"', TextBinaryLoader)

    adapters.register_dumper(bytes, BytesDumper)
    adapters.register_dumper(bytearray, BytesDumper)
    adapters.register_dumper(memoryview, BytesDumper)
    adapters.register_dumper(bytes, BytesBinaryDumper)
    adapters.register_dumper(bytearray, BytesBinaryDumper)
    adapters.register_dumper(memoryview, BytesBinaryDumper)

    adapters.register_loader("bytea", ByteaLoader)
    adapters.register_loader(postgres.INVALID_OID, ByteaBinaryLoader)
    adapters.register_loader("bytea", ByteaBinaryLoader)
