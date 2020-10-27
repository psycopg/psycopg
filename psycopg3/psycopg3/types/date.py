"""
Adapters for date/time types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from datetime import date, datetime, time
from typing import cast

from ..adapt import Dumper, Loader
from ..proto import AdaptContext
from ..errors import InterfaceError, DataError
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


@Dumper.text(time)
class TimeDumper(Dumper):

    _encode = codecs.lookup("ascii").encode
    TIMETZ_OID = builtins["timetz"].oid

    def dump(self, obj: time) -> bytes:
        return self._encode(str(obj))[0]

    @property
    def oid(self) -> int:
        return self.TIMETZ_OID


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
        self._format = self._format_from_context()

    def load(self, data: bytes) -> date:
        try:
            return datetime.strptime(
                self._decode(data)[0], self._format
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
            raise DataError(
                "Python doesn't support BC date:"
                f" got {data.decode('utf8', 'replace')}"
            )

        if self._get_year_digits(data) > 4:
            raise DataError(
                "Python date doesn't support years after 9999:"
                f" got {data.decode('utf8', 'replace')}"
            )

        # We genuinely received something we cannot parse
        raise exc

    def _get_year_digits(self, data: bytes) -> int:
        datesep = self._format[2].encode("ascii")
        parts = data.split(b" ")[0].split(datesep)
        return max(map(len, parts))


@Loader.text(builtins["time"].oid)
class TimeLoader(Loader):

    _decode = codecs.lookup("ascii").decode
    _format = "%H:%M:%S.%f"
    _format_no_micro = _format.replace(".%f", "")

    def load(self, data: bytes) -> time:
        # check if the data contains microseconds
        fmt = self._format if b"." in data else self._format_no_micro
        try:
            return datetime.strptime(self._decode(data)[0], fmt).time()
        except ValueError as e:
            return self._raise_error(data, e)

    def _raise_error(self, data: bytes, exc: ValueError) -> time:
        # Most likely, time 24:00
        if data.startswith(b"24"):
            raise DataError(
                f"time not supported by Python: {data.decode('ascii')}"
            )

        # We genuinely received something we cannot parse
        raise exc


@Loader.text(builtins["timetz"].oid)
class TimeTzLoader(TimeLoader):
    _format = "%H:%M:%S.%f%z"
    _format_no_micro = _format.replace(".%f", "")

    def load(self, data: bytes) -> time:
        # Hack to convert +HH in +HHMM
        if data[-3:-2] in (b"-", b"+"):
            data += b"00"

        fmt = self._format if b"." in data else self._format_no_micro
        try:
            dt = datetime.strptime(self._decode(data)[0], fmt)
        except ValueError as e:
            return self._raise_error(data, e)

        return dt.time().replace(tzinfo=dt.tzinfo)


@Loader.text(builtins["timestamp"].oid)
class TimestampLoader(DateLoader):
    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)
        self._format_no_micro = self._format.replace(".%f", "")

    def load(self, data: bytes) -> datetime:
        # check if the data contains microseconds
        fmt = (
            self._format if data.find(b".", 19) >= 0 else self._format_no_micro
        )
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

    def _get_year_digits(self, data: bytes) -> int:
        # Find the year from the date.
        if not self._get_datestyle().startswith(b"P"):  # Postgres
            return super()._get_year_digits(data)
        else:
            parts = data.split()
            if len(parts) > 4:
                return len(parts[4])
            else:
                return 0


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
