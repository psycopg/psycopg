"""
Adapters of textual types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Optional, Tuple, Union

from ..adapt import Adapter, TypeCaster, AdaptContext
from ..utils.typing import EncodeFunc, DecodeFunc
from ..pq import Escaping
from .oids import builtins
from .array import ArrayCaster

TEXT_OID = builtins["text"].oid
BYTEA_OID = builtins["bytea"].oid


@Adapter.text(str)
@Adapter.binary(str)
class StringAdapter(Adapter):
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

    def adapt(self, obj: str) -> bytes:
        return self._encode(obj)[0]


@TypeCaster.text(builtins["text"].oid)
@TypeCaster.binary(builtins["text"].oid)
@ArrayCaster.text(builtins["text"].array_oid)
@ArrayCaster.binary(builtins["text"].array_oid)
class StringCaster(TypeCaster):

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

    def cast(self, data: bytes) -> Union[bytes, str]:
        if self.decode is not None:
            return self.decode(data)[0]
        else:
            # return bytes for SQL_ASCII db
            return data


@Adapter.text(bytes)
class BytesAdapter(Adapter):
    def __init__(self, src: type, context: AdaptContext = None):
        super().__init__(src, context)
        self.esc = Escaping(
            self.connection.pgconn if self.connection is not None else None
        )

    def adapt(self, obj: bytes) -> Tuple[bytes, int]:
        return self.esc.escape_bytea(obj), BYTEA_OID


@Adapter.binary(bytes)
def adapt_bytes(b: bytes) -> Tuple[bytes, int]:
    return b, BYTEA_OID


@TypeCaster.text(builtins["bytea"].oid)
@ArrayCaster.text(builtins["bytea"].array_oid)
def cast_bytea(data: bytes) -> bytes:
    return Escaping().unescape_bytea(data)


@TypeCaster.binary(builtins["bytea"].oid)
def cast_bytea_binary(data: bytes) -> bytes:
    return data
