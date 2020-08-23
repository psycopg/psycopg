"""
Adapters for date/time types.
"""

# Copyright (C) 2020 The Psycopg Team

import re
import codecs
from datetime import date

from ..adapt import Dumper, Loader
from ..proto import AdaptContext
from ..errors import InterfaceError
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

        ds = self._get_datestyle()
        if ds == b"ISO":
            pass  # Default: YMD
        elif ds == b"German":
            self.load = self.load_dmy  # type: ignore
        elif ds == b"SQL" or ds == b"Postgres":
            self.load = self.load_mdy  # type: ignore

    def load_ymd(self, data: bytes) -> date:
        try:
            return date(int(data[:4]), int(data[5:7]), int(data[8:]))
        except ValueError as e:
            exc = e

        return self._raise_error(data, exc)

    load = load_ymd

    def load_dmy(self, data: bytes) -> date:
        try:
            return date(int(data[6:]), int(data[3:5]), int(data[:2]))
        except ValueError as e:
            exc = e

        return self._raise_error(data, exc)

    def load_mdy(self, data: bytes) -> date:
        try:
            return date(int(data[6:]), int(data[:2]), int(data[3:5]))
        except ValueError as e:
            exc = e

        return self._raise_error(data, exc)

    def _get_datestyle(self) -> bytes:
        """Return the PostgreSQL output datestyle of the connection."""
        if self.connection:
            ds = self.connection.pgconn.parameter_status(b"DateStyle")
            if ds:
                return ds.split(b",", 1)[0]

        return b"ISO"

    def _raise_error(self, data: bytes, exc: ValueError) -> date:
        # Most likely we received a BC date, which Python doesn't support
        # Otherwise the unexpected value is displayed in the exception.
        if data.endswith(b"BC"):
            raise InterfaceError(
                "Python doesn't support BC date:"
                f" got {data.decode('utf8', 'replace')}"
            )

        # Find the year from the date. This is not the fast path so we don't
        # need crazy speed.
        ds = self._get_datestyle()
        if ds == b"ISO":
            year = int(data.split(b"-", 1)[0])
        else:
            year = int(re.split(rb"[-/\.]", data)[-1])

        if year > 9999:
            raise InterfaceError(
                "Python date doesn't support years after 9999:"
                f" got {data.decode('utf8', 'replace')}"
            )

        # We genuinely received something we cannot parse
        raise exc
