"""
Adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Optional, Tuple, Union

from ..adapt import Dumper, Loader
from ..proto import AdaptContext, EncodeFunc, DecodeFunc
from ..pq import Escaping
from .oids import builtins, INVALID_OID


TEXT_OID = builtins["text"].oid
BYTEA_OID = builtins["bytea"].oid


@Dumper.text(str)
@Dumper.binary(str)
class StringDumper(Dumper):
    def __init__(self, src: type, context: AdaptContext):
        super().__init__(src, context)

        self._encode: EncodeFunc
        if self.connection is not None:
            if self.connection.encoding != "SQL_ASCII":
                self._encode = self.connection.codec.encode
            else:
                self._encode = codecs.lookup("utf8").encode
        else:
            self._encode = codecs.lookup("utf8").encode

    def dump(self, obj: str) -> bytes:
        return self._encode(obj)[0]


@Loader.text(builtins["text"].oid)
@Loader.binary(builtins["text"].oid)
@Loader.text(builtins["varchar"].oid)
@Loader.binary(builtins["varchar"].oid)
@Loader.text(INVALID_OID)
class StringLoader(Loader):

    decode: Optional[DecodeFunc]

    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)

        if self.connection is not None:
            if self.connection.encoding != "SQL_ASCII":
                self.decode = self.connection.codec.decode
            else:
                self.decode = None
        else:
            self.decode = codecs.lookup("utf8").decode

    def load(self, data: bytes) -> Union[bytes, str]:
        if self.decode is not None:
            return self.decode(data)[0]
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

        self.decode: DecodeFunc
        if self.connection is not None:
            self.decode = self.connection.codec.decode
        else:
            self.decode = codecs.lookup("utf8").decode

    def load(self, data: bytes) -> str:
        return self.decode(data)[0]


@Dumper.text(bytes)
class BytesDumper(Dumper):
    def __init__(self, src: type, context: AdaptContext = None):
        super().__init__(src, context)
        self.esc = Escaping(
            self.connection.pgconn if self.connection is not None else None
        )

    def dump(self, obj: bytes) -> Tuple[bytes, int]:
        return self.esc.escape_bytea(obj), BYTEA_OID


@Dumper.binary(bytes)
class BinaryBytesDumper(Dumper):
    def dump(self, b: bytes) -> Tuple[bytes, int]:
        return b, BYTEA_OID


@Loader.text(builtins["bytea"].oid)
def load_bytea_text(data: bytes) -> bytes:
    return Escaping().unescape_bytea(data)


@Loader.binary(builtins["bytea"].oid)
@Loader.binary(INVALID_OID)
def load_bytea_binary(data: bytes) -> bytes:
    return data
