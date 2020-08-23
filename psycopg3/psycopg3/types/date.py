"""
Adapters for date/time types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from datetime import date

from ..adapt import Dumper, Loader
from ..proto import AdaptContext
from .oids import builtins


@Dumper.text(date)
class DateDumper(Dumper):

    _encode = codecs.lookup("ascii").encode
    DATE_OID = builtins["date"].oid

    def dump(self, obj: date) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return self._encode(str(obj))[0]

    @property
    def oid(self) -> int:
        return self.DATE_OID


@Loader.text(builtins["date"].oid)
class DateLoader(Loader):

    _decode = codecs.lookup("ascii").decode

    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)

        if self.connection:
            ds = self.connection.pgconn.parameter_status(b"DateStyle")
            if not ds or ds.startswith(b"ISO"):
                pass
            elif ds.startswith(b"German"):
                self.load = self.load_dmy  # type: ignore
            elif ds.startswith(b"SQL"):
                self.load = self.load_mdy  # type: ignore
            elif ds.startswith(b"Postgres"):
                self.load = self.load_mdy  # type: ignore

    def load(self, data: bytes) -> date:
        return date(int(data[:4]), int(data[5:7]), int(data[8:]))

    def load_dmy(self, data: bytes) -> date:
        return date(int(data[6:]), int(data[3:5]), int(data[:2]))

    def load_mdy(self, data: bytes) -> date:
        return date(int(data[6:]), int(data[:2]), int(data[3:5]))
