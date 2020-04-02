"""
Adapters of textual types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Optional, Union

from ..adapt import (
    Adapter,
    Typecaster,
)
from ..connection import BaseConnection
from ..utils.typing import EncodeFunc, DecodeFunc, Oid
from .oids import type_oid


@Adapter.text(str)
@Adapter.binary(str)
class StringAdapter(Adapter):
    def __init__(self, cls: type, conn: BaseConnection):
        super().__init__(cls, conn)

        self._encode: EncodeFunc
        if conn is not None:
            self._encode = conn.codec.encode
        else:
            self._encode = codecs.lookup("utf8").encode

    def adapt(self, obj: str) -> bytes:
        return self._encode(obj)[0]


@Typecaster.text(type_oid["text"])
@Typecaster.binary(type_oid["text"])
class StringCaster(Typecaster):

    decode: Optional[DecodeFunc]

    def __init__(self, oid: Oid, conn: BaseConnection):
        super().__init__(oid, conn)

        if conn is not None:
            if conn.pgenc != b"SQL_ASCII":
                self.decode = conn.codec.decode
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
