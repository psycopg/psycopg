"""
Adapters for date/time types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import sys
import struct
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, cast, Optional, Tuple, Union

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader, Format as Pg3Format
from ..proto import AdaptContext
from ..errors import InterfaceError, DataError
from .._tz import get_tzinfo

_PackInt = Callable[[int], bytes]
_UnpackInt = Callable[[bytes], Tuple[int]]

_pack_int4 = cast(_PackInt, struct.Struct("!i").pack)
_pack_int8 = cast(_PackInt, struct.Struct("!q").pack)
_unpack_int4 = cast(_UnpackInt, struct.Struct("!i").unpack)
_unpack_int8 = cast(_UnpackInt, struct.Struct("!q").unpack)

_pack_timetz = cast(Callable[[int, int], bytes], struct.Struct("!qi").pack)
_unpack_timetz = cast(
    Callable[[bytes], Tuple[int, int]], struct.Struct("!qi").unpack
)
_pack_interval = cast(
    Callable[[int, int, int], bytes], struct.Struct("!qii").pack
)
_unpack_interval = cast(
    Callable[[bytes], Tuple[int, int, int]], struct.Struct("!qii").unpack
)

_pg_date_epoch_days = date(2000, 1, 1).toordinal()
_pg_datetime_epoch = datetime(2000, 1, 1)
_pg_datetimetz_epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)
_py_date_min_days = date.min.toordinal()


class DateDumper(Dumper):

    format = Format.TEXT
    _oid = builtins["date"].oid

    def dump(self, obj: date) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return str(obj).encode("utf8")


class DateBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = builtins["date"].oid

    def dump(self, obj: date) -> bytes:
        days = obj.toordinal() - _pg_date_epoch_days
        return _pack_int4(days)


class _BaseTimeDumper(Dumper):

    # Can change to timetz type if the object dumped is naive
    _oid = builtins["time"].oid

    def get_key(
        self, obj: time, format: Pg3Format
    ) -> Union[type, Tuple[type]]:
        # Use (cls,) to report the need to upgrade to a dumper for timetz (the
        # Frankenstein of the data types).
        if not obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    def upgrade(self, obj: time, format: Pg3Format) -> "Dumper":
        raise NotImplementedError


class TimeDumper(_BaseTimeDumper):

    format = Format.TEXT

    def dump(self, obj: time) -> bytes:
        return str(obj).encode("utf8")

    def upgrade(self, obj: time, format: Pg3Format) -> "Dumper":
        if not obj.tzinfo:
            return self
        else:
            return TimeTzDumper(self.cls)


class TimeTzDumper(TimeDumper):

    _oid = builtins["timetz"].oid


class TimeBinaryDumper(_BaseTimeDumper):

    format = Format.BINARY

    def dump(self, obj: time) -> bytes:
        ms = obj.microsecond + 1_000_000 * (
            obj.second + 60 * (obj.minute + 60 * obj.hour)
        )
        return _pack_int8(ms)

    def upgrade(self, obj: time, format: Pg3Format) -> "Dumper":
        if not obj.tzinfo:
            return self
        else:
            return TimeTzBinaryDumper(self.cls)


class TimeTzBinaryDumper(TimeBinaryDumper):

    _oid = builtins["timetz"].oid

    def dump(self, obj: time) -> bytes:
        ms = obj.microsecond + 1_000_000 * (
            obj.second + 60 * (obj.minute + 60 * obj.hour)
        )
        off = obj.utcoffset()
        assert off is not None
        return _pack_timetz(ms, -int(off.total_seconds()))


class _BaseDateTimeDumper(Dumper):

    # Can change to timestamp type if the object dumped is naive
    _oid = builtins["timestamptz"].oid

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
        raise NotImplementedError


class DateTimeTzDumper(_BaseDateTimeDumper):

    format = Format.TEXT

    def dump(self, obj: datetime) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return str(obj).encode("utf8")

    def upgrade(self, obj: datetime, format: Pg3Format) -> "Dumper":
        if obj.tzinfo:
            return self
        else:
            return DateTimeDumper(self.cls)


class DateTimeDumper(DateTimeTzDumper):
    _oid = builtins["timestamp"].oid


class DateTimeTzBinaryDumper(_BaseDateTimeDumper):

    format = Format.BINARY

    # Somewhere, between year 2270 and 2275, float rounding in total_seconds
    # cause us errors: switch to an algorithm without rounding before then.
    _delta_prec_loss = (
        datetime(2250, 1, 1) - _pg_datetime_epoch
    ).total_seconds()

    def dump(self, obj: datetime) -> bytes:
        delta = obj - _pg_datetimetz_epoch
        secs = delta.total_seconds()
        if -self._delta_prec_loss < secs < self._delta_prec_loss:
            micros = int(1_000_000 * secs)
        else:
            micros = delta.microseconds + 1_000_000 * (
                86_400 * delta.days + delta.seconds
            )
        return _pack_int8(micros)

    def upgrade(self, obj: datetime, format: Pg3Format) -> "Dumper":
        if obj.tzinfo:
            return self
        else:
            return DateTimeBinaryDumper(self.cls)


class DateTimeBinaryDumper(DateTimeTzBinaryDumper):
    _oid = builtins["timestamp"].oid

    def dump(self, obj: datetime) -> bytes:
        delta = obj - _pg_datetime_epoch
        secs = delta.total_seconds()
        if -self._delta_prec_loss < secs < self._delta_prec_loss:
            micros = int(1_000_000 * secs)
        else:
            micros = (
                1_000_000 * (86_400 * delta.days + delta.seconds)
                + delta.microseconds
            )
        return _pack_int8(micros)


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


class TimeDeltaBinaryDumper(Dumper):

    format = Format.BINARY
    _oid = builtins["interval"].oid

    def dump(self, obj: timedelta) -> bytes:
        micros = 1_000_000 * obj.seconds + obj.microseconds
        return _pack_interval(micros, obj.days, 0)


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


class DateBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> date:
        days = _unpack_int4(data)[0] + _pg_date_epoch_days
        try:
            return date.fromordinal(days)
        except ValueError:
            if days < _py_date_min_days:
                raise DataError("date too small (before year 1)")
            else:
                raise DataError("date too large (after year 10K)")


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


class TimeBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> time:
        val = _unpack_int8(data)[0]
        val, ms = divmod(val, 1_000_000)
        val, s = divmod(val, 60)
        h, m = divmod(val, 60)
        try:
            return time(h, m, s, ms)
        except ValueError:
            raise DataError(f"time not supported by Python: hour={h}")


class TimeTzLoader(TimeLoader):

    format = Format.TEXT
    _format = "%H:%M:%S.%f%z"
    _format_no_micro = _format.replace(".%f", "")

    def _load(self, data: Buffer) -> time:
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

        return self._load(data)


if sys.version_info >= (3, 7):
    setattr(TimeTzLoader, "load", TimeTzLoader._load)
else:
    setattr(TimeTzLoader, "load", TimeTzLoader._load_py36)


class TimeTzBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> time:
        val, off = _unpack_timetz(data)

        val, ms = divmod(val, 1_000_000)
        val, s = divmod(val, 60)
        h, m = divmod(val, 60)

        try:
            return time(h, m, s, ms, self._tz_from_sec(off))
        except ValueError:
            raise DataError(f"time not supported by Python: hour={h}")

    def _tz_from_sec(self, sec: int) -> timezone:
        return timezone(timedelta(seconds=-sec))

    def _tz_from_sec_36(self, sec: int) -> timezone:
        if sec % 60:
            sec = round(sec / 60.0) * 60
        return timezone(timedelta(seconds=-sec))


if sys.version_info < (3, 7):
    setattr(
        TimeTzBinaryLoader, "_tz_from_sec", TimeTzBinaryLoader._tz_from_sec_36
    )


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


class TimestampBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> datetime:
        micros = _unpack_int8(data)[0]
        try:
            return _pg_datetime_epoch + timedelta(microseconds=micros)
        except OverflowError:
            if micros <= 0:
                raise DataError("timestamp too small (before year 1)")
            else:
                raise DataError("timestamp too large (after year 10K)")


class TimestampTzLoader(TimestampLoader):

    format = Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._timezone = get_tzinfo(
            self.connection.pgconn if self.connection else None
        )

    def _format_from_context(self) -> str:
        ds = self._get_datestyle()
        if ds.startswith(b"I"):  # ISO
            if sys.version_info >= (3, 7):
                return "%Y-%m-%d %H:%M:%S.%f%z"
            else:
                # No tz parsing: it will be handles separately.
                return "%Y-%m-%d %H:%M:%S.%f"

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

    _re_tz = re.compile(br"([-+])(\d+)(?::(\d+)(?::(\d+))?)?$")

    def load(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)

        # Hack to convert +HH in +HHMM
        if data[-3] in (43, 45):
            data += b"00"

        return super().load(data).astimezone(self._timezone)

    def _load_py36(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)

        # Separate the timezone from the rest
        m = self._re_tz.search(data)
        if not m:
            raise DataError(
                "failed to parse timezone from '{data.decode('ascii')}'"
            )

        sign, hour, min, sec = m.groups()
        tzoff = timedelta(
            seconds=(int(sec) if sec else 0)
            + 60 * ((int(min) if min else 0) + 60 * int(hour))
        )
        if sign == b"-":
            tzoff = -tzoff

        rv = super().load(data[: m.start()])
        return (
            (rv - tzoff)
            .replace(tzinfo=timezone.utc)
            .astimezone(self._timezone)
        )

    def _load_notimpl(self, data: Buffer) -> datetime:
        if isinstance(data, memoryview):
            data = bytes(data)
        raise NotImplementedError(
            "can't parse datetimetz with DateStyle"
            f" {self._get_datestyle().decode('ascii')}: {data.decode('ascii')}"
        )


if sys.version_info < (3, 7):
    setattr(TimestampTzLoader, "load", TimestampTzLoader._load_py36)


class TimestampTzBinaryLoader(Loader):

    format = Format.BINARY

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._timezone = get_tzinfo(
            self.connection.pgconn if self.connection else None
        )

    def load(self, data: Buffer) -> datetime:
        micros = _unpack_int8(data)[0]
        try:
            ts = _pg_datetimetz_epoch + timedelta(microseconds=micros)
            return ts.astimezone(self._timezone)
        except OverflowError:
            if micros <= 0:
                raise DataError("timestamp too small (before year 1)")
            else:
                raise DataError("timestamp too large (after year 10K)")


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


class IntervalBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> timedelta:
        micros, days, months = _unpack_interval(data)
        if months > 0:
            years, months = divmod(months, 12)
            days = days + 30 * months + 365 * years
        elif months < 0:
            years, months = divmod(-months, 12)
            days = days - 30 * months - 365 * years
        return timedelta(days=days, microseconds=micros)
