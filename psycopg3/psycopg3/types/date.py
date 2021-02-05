"""
Adapters for date/time types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import sys
from datetime import date, datetime, time, timedelta
from typing import cast, Optional, Tuple, Union

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader, Format as Pg3Format
from ..proto import AdaptContext
from ..errors import InterfaceError, DataError


class DateDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["date"].oid

    def dump(self, obj: date) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return str(obj).encode("utf8")


class TimeDumper(Dumper):

    format = Format.TEXT

    # Can change to timetz type if the object dumped is naive
    _oid = builtins["time"].oid

    def dump(self, obj: time) -> bytes:
        return str(obj).encode("utf8")

    def get_key(
        self, obj: time, format: Pg3Format
    ) -> Union[type, Tuple[type]]:
        # Use (cls,) to report the need to upgrade  to a dumper for timetz (the
        # Frankenstein of the data types).
        if not obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    def upgrade(self, obj: time, format: Pg3Format) -> "Dumper":
        if not obj.tzinfo:
            return self
        else:
            return TimeTzDumper(self.cls)


class TimeTzDumper(TimeDumper):

    _oid = builtins["timetz"].oid


class DateTimeTzDumper(Dumper):

    format = Format.TEXT

    # Can change to timestamp type if the object dumped is naive
    _oid = builtins["timestamptz"].oid

    def dump(self, obj: datetime) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return str(obj).encode("utf8")

    def get_key(
        self, obj: datetime, format: Pg3Format
    ) -> Union[type, Tuple[type]]:
        # Use (cls,) to report the need to upgrade (downgrade, actually) to a
        # dumper for naive timestamp.
        if obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    def upgrade(self, obj: datetime, format: Pg3Format) -> "Dumper":
        if obj.tzinfo:
            return self
        else:
            return DateTimeDumper(self.cls)


class DateTimeDumper(DateTimeTzDumper):
    _oid = builtins["timestamp"].oid


class TimeDeltaDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["interval"].oid

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        if self.connection:
            if (
                self.connection.pgconn.parameter_status(b"IntervalStyle")
                == b"sql_standard"
            ):
                setattr(self, "dump", self._dump_sql)

    def dump(self, obj: timedelta) -> bytes:
        return str(obj).encode("utf8")

    def _dump_sql(self, obj: timedelta) -> bytes:
        # sql_standard format needs explicit signs
        # otherwise -1 day 1 sec will mean -1 sec
        return b"%+d day %+d second %+d microsecond" % (
            obj.days,
            obj.seconds,
            obj.microseconds,
        )


class DateLoader(Loader):

    format = Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._format = self._format_from_context()

    def load(self, data: Buffer) -> date:
        if isinstance(data, memoryview):
            data = bytes(data)
        try:
            return datetime.strptime(data.decode("utf8"), self._format).date()
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


class TimeLoader(Loader):

    format = Format.TEXT
    _format = "%H:%M:%S.%f"
    _format_no_micro = _format.replace(".%f", "")

    def load(self, data: Buffer) -> time:
        # check if the data contains microseconds
        if isinstance(data, memoryview):
            data = bytes(data)
        fmt = self._format if b"." in data else self._format_no_micro
        try:
            return datetime.strptime(data.decode("utf8"), fmt).time()
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


class TimeTzLoader(TimeLoader):

    format = Format.TEXT
    _format = "%H:%M:%S.%f%z"
    _format_no_micro = _format.replace(".%f", "")

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        if sys.version_info < (3, 7):
            setattr(self, "load", self._load_py36)

        super().__init__(oid, context)

    def load(self, data: Buffer) -> time:
        if isinstance(data, memoryview):
            data = bytes(data)

        # Hack to convert +HH in +HHMM
        if data[-3] in (43, 45):
            data += b"00"

        fmt = self._format if b"." in data else self._format_no_micro
        try:
            dt = datetime.strptime(data.decode("utf8"), fmt)
        except ValueError as e:
            return self._raise_error(data, e)

        return dt.time().replace(tzinfo=dt.tzinfo)

    def _load_py36(self, data: Buffer) -> time:
        if isinstance(data, memoryview):
            data = bytes(data)
        # Drop seconds from timezone for Python 3.6
        # Also, Python 3.6 doesn't support HHMM, only HH:MM
        if data[-6] in (43, 45):  # +-HH:MM -> +-HHMM
            data = data[:-3] + data[-2:]
        elif data[-9] in (43, 45):  # +-HH:MM:SS -> +-HHMM
            data = data[:-6] + data[-5:-3]

        return TimeTzLoader.load(self, data)


class TimestampLoader(DateLoader):

    format = Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._format_no_micro = self._format.replace(".%f", "")

    def load(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)

        # check if the data contains microseconds
        fmt = (
            self._format if data.find(b".", 19) >= 0 else self._format_no_micro
        )
        try:
            return datetime.strptime(data.decode("utf8"), fmt)
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


class TimestamptzLoader(TimestampLoader):

    format = Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        if sys.version_info < (3, 7):
            setattr(self, "load", self._load_py36)

        super().__init__(oid, context)

    def _format_from_context(self) -> str:
        ds = self._get_datestyle()
        if ds.startswith(b"I"):  # ISO
            return "%Y-%m-%d %H:%M:%S.%f%z"

        # These don't work: the timezone name is not always displayed
        # elif ds.startswith(b"G"):  # German
        #     return "%d.%m.%Y %H:%M:%S.%f %Z"
        # elif ds.startswith(b"S"):  # SQL
        #     return (
        #         "%d/%m/%Y %H:%M:%S.%f %Z"
        #         if ds.endswith(b"DMY")
        #         else "%m/%d/%Y %H:%M:%S.%f %Z"
        #     )
        # elif ds.startswith(b"P"):  # Postgres
        #     return (
        #         "%a %d %b %H:%M:%S.%f %Y %Z"
        #         if ds.endswith(b"DMY")
        #         else "%a %b %d %H:%M:%S.%f %Y %Z"
        #     )
        # else:
        #     raise InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")
        else:
            setattr(self, "load", self._load_notimpl)
            return ""

    def load(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)

        # Hack to convert +HH in +HHMM
        if data[-3] in (43, 45):
            data += b"00"

        return super().load(data)

    def _load_py36(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)
        # Drop seconds from timezone for Python 3.6
        # Also, Python 3.6 doesn't support HHMM, only HH:MM
        tzsep = (43, 45)  # + and - bytes
        if data[-3] in tzsep:  # +HH, -HH
            data += b"00"
        elif data[-6] in tzsep:
            data = data[:-3] + data[-2:]
        elif data[-9] in tzsep:
            data = data[:-6] + data[-5:-3]

        return super().load(data)

    def _load_notimpl(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)
        raise NotImplementedError(
            "can't parse datetimetz with DateStyle"
            f" {self._get_datestyle().decode('ascii')}: {data.decode('ascii')}"
        )


class IntervalLoader(Loader):

    format = Format.TEXT

    _re_interval = re.compile(
        br"""
        (?: (?P<years> [-+]?\d+) \s+ years? \s* )?
        (?: (?P<months> [-+]?\d+) \s+ mons? \s* )?
        (?: (?P<days> [-+]?\d+) \s+ days? \s* )?
        (?: (?P<hsign> [-+])?
            (?P<hours> \d+ )
          : (?P<minutes> \d+ )
          : (?P<seconds> \d+ (?:\.\d+)? )
        )?
        """,
        re.VERBOSE,
    )

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        if self.connection:
            ints = self.connection.pgconn.parameter_status(b"IntervalStyle")
            if ints != b"postgres":
                setattr(self, "load", self._load_notimpl)

    def load(self, data: Buffer) -> timedelta:
        m = self._re_interval.match(data)
        if not m:
            raise ValueError("can't parse interval: {data.decode('ascii')}")

        days = 0
        seconds = 0.0

        tmp = m.group("years")
        if tmp:
            days += 365 * int(tmp)

        tmp = m.group("months")
        if tmp:
            days += 30 * int(tmp)

        tmp = m.group("days")
        if tmp:
            days += int(tmp)

        if m.group("hours"):
            seconds = (
                3600 * int(m.group("hours"))
                + 60 * int(m.group("minutes"))
                + float(m.group("seconds"))
            )
            if m.group("hsign") == b"-":
                seconds = -seconds

        try:
            return timedelta(days=days, seconds=seconds)
        except OverflowError as e:
            raise DataError(str(e))

    def _load_notimpl(self, data: Buffer) -> timedelta:
        if isinstance(data, memoryview):
            data = bytes(data)
        ints = (
            self.connection
            and self.connection.pgconn.parameter_status(b"IntervalStyle")
            or b"unknown"
        )
        raise NotImplementedError(
            "can't parse interval with IntervalStyle"
            f" {ints.decode('ascii')}: {data.decode('ascii')}"
        )
