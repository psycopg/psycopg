"""
Adapters for date/time types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from datetime import date, datetime
from typing import cast

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


@Dumper.text(datetime)
class DateTimeDumper(Dumper):

    _encode = codecs.lookup("ascii").encode
    TIMESTAMPTZ_OID = builtins["timestamptz"].oid

    def dump(self, obj: date) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return self._encode(str(obj))[0]

    @property
    def oid(self) -> int:
        return self.TIMESTAMPTZ_OID


@Loader.text(builtins["date"].oid)
class DateLoader(Loader):

    _decode = codecs.lookup("ascii").decode

    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)
        self._date_format = self._format_from_context()

    def load(self, data: bytes) -> date:
        try:
            return datetime.strptime(
                self._decode(data)[0], self._date_format
            ).date()
        except ValueError as e:
            return self._raise_error(data, e)

    def _format_from_context(self) -> str:
        ds = self._get_datestyle()
        if ds.startswith(b"I"):  # ISO
            return "%Y-%m-%d"
        elif ds.startswith(b"G"):  # German
            return "%d.%m.%Y"
        elif ds.startswith(b"S"):  # SQL
            return "%d/%m/%Y" if ds.endswith(b"DMY") else "%m/%d/%Y"
        elif ds.startswith(b"P"):  # Postgres
            return "%d-%m-%Y" if ds.endswith(b"DMY") else "%m-%d-%Y"
        else:
            raise InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    def _get_datestyle(self) -> bytes:
        rv = b"ISO, DMY"
        if self.connection:
            ds = self.connection.pgconn.parameter_status(b"DateStyle")
            if ds:
                rv = ds

        return rv

    def _raise_error(self, data: bytes, exc: ValueError) -> date:
        # Most likely we received a BC date, which Python doesn't support
        # Otherwise the unexpected value is displayed in the exception.
        if data.endswith(b"BC"):
            raise ValueError(
                "Python doesn't support BC date:"
                f" got {data.decode('utf8', 'replace')}"
            )

        # Find the year from the date. We check if >= Y10K only in ISO format,
        # others are too silly to bother being polite.
        ds = self._get_datestyle()
        if ds.startswith(b"ISO"):
            year = int(data.split(b"-", 1)[0])
            if year > 9999:
                raise ValueError(
                    "Python date doesn't support years after 9999:"
                    f" got {data.decode('utf8', 'replace')}"
                )

        # We genuinely received something we cannot parse
        raise exc


@Loader.text(builtins["timestamp"].oid)
class TimestampLoader(DateLoader):
    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)
        self._no_micro_format = self._date_format.replace(".%f", "")

    def load(self, data: bytes) -> datetime:
        # check if the data contains microseconds
        fmt = self._date_format if b"." in data[19:] else self._no_micro_format
        try:
            return datetime.strptime(self._decode(data)[0], fmt)
        except ValueError as e:
            return self._raise_error(data, e)

    def _format_from_context(self) -> str:
        ds = self._get_datestyle()
        if ds.startswith(b"I"):  # ISO
            return "%Y-%m-%d %H:%M:%S.%f"
        elif ds.startswith(b"G"):  # German
            return "%d.%m.%Y %H:%M:%S.%f"
        elif ds.startswith(b"S"):  # SQL
            return (
                "%d/%m/%Y %H:%M:%S.%f"
                if ds.endswith(b"DMY")
                else "%m/%d/%Y %H:%M:%S.%f"
            )
        elif ds.startswith(b"P"):  # Postgres
            return (
                "%a %d %b %H:%M:%S.%f %Y"
                if ds.endswith(b"DMY")
                else "%a %b %d %H:%M:%S.%f %Y"
            )
        else:
            raise InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    def _raise_error(self, data: bytes, exc: ValueError) -> datetime:
        return cast(datetime, super()._raise_error(data, exc))


@Loader.text(builtins["timestamptz"].oid)
class TimestamptzLoader(TimestampLoader):
    def _format_from_context(self) -> str:
        ds = self._get_datestyle()
        if ds.startswith(b"I"):  # ISO
            return "%Y-%m-%d %H:%M:%S.%f%z"
        elif ds.startswith(b"G"):  # German
            return "%d.%m.%Y %H:%M:%S.%f %Z"
        elif ds.startswith(b"S"):  # SQL
            return (
                "%d/%m/%Y %H:%M:%S.%f %Z"
                if ds.endswith(b"DMY")
                else "%m/%d/%Y %H:%M:%S.%f %Z"
            )
        elif ds.startswith(b"P"):  # Postgres
            return (
                "%a %d %b %H:%M:%S.%f %Y %Z"
                if ds.endswith(b"DMY")
                else "%a %b %d %H:%M:%S.%f %Y %Z"
            )
        else:
            raise InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    def load(self, data: bytes) -> datetime:
        # Hack to convert +HH in +HHMM
        if data[-3:-2] in (b"-", b"+"):
            data += b"00"

        return super().load(data)
