"""
Adapters of textual types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs

from ..adaptation import Adapter
from ..adaptation import Typecaster
from .oids import type_oid


@Adapter.register(str)
@Adapter.register_binary(str)
class StringAdapter(Adapter):
    def __init__(self, cls, conn):
        super().__init__(cls, conn)
        self.encode = (
            conn.codec if conn is not None else codecs.lookup("utf8")
        ).encode

    def adapt(self, obj):
        return self.encode(obj)[0]


@Typecaster.register(type_oid["text"])
@Typecaster.register_binary(type_oid["text"])
class StringCaster(Typecaster):
    def __init__(self, oid, conn):
        super().__init__(oid, conn)
        if conn is not None:
            if conn.pgenc != b"SQL_ASCII":
                self.decode = conn.codec.decode
            else:
                self.decode = None
        else:
            self.decode = codecs.lookup("utf8").decode

    def cast(self, data):
        if self.decode is not None:
            return self.decode(data)[0]
        else:
            # return bytes for SQL_ASCII db
            return data
