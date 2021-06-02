"""
Adapters for date/time types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import sys
import struct
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, cast, Optional, Tuple, Union, TYPE_CHECKING

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader, Format as Pg3Format
from ..proto import AdaptContext
from ..errors import InterfaceError, DataError
from .._tz import get_tzinfo

if TYPE_CHECKING:
    from ..connection import BaseConnection

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

utc = timezone.utc
_pg_date_epoch_days = date(2000, 1, 1).toordinal()
_pg_datetime_epoch = datetime(2000, 1, 1)
_pg_datetimetz_epoch = datetime(2000, 1, 1, tzinfo=utc)
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
    def get_key(
        self, obj: time, format: Pg3Format
    ) -> Union[type, Tuple[type]]:
        # Use (cls,) to report the need to upgrade to a dumper for timetz (the
        # Frankenstein of the data types).
        if not obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    def upgrade(self, obj: time, format: Pg3Format) -> Dumper:
        raise NotImplementedError


class _BaseTimeTextDumper(_BaseTimeDumper):

    format = Format.TEXT

    def dump(self, obj: time) -> bytes:
        return str(obj).encode("utf8")


class TimeDumper(_BaseTimeTextDumper):

    _oid = builtins["time"].oid

    def upgrade(self, obj: time, format: Pg3Format) -> Dumper:
        if not obj.tzinfo:
            return self
        else:
            return TimeTzDumper(self.cls)


class TimeTzDumper(_BaseTimeTextDumper):

    _oid = builtins["timetz"].oid


class TimeBinaryDumper(_BaseTimeDumper):

    format = Format.BINARY
    _oid = builtins["time"].oid

    def dump(self, obj: time) -> bytes:
        ms = obj.microsecond + 1_000_000 * (
            obj.second + 60 * (obj.minute + 60 * obj.hour)
        )
        return _pack_int8(ms)

    def upgrade(self, obj: time, format: Pg3Format) -> Dumper:
        if not obj.tzinfo:
            return self
        else:
            return TimeTzBinaryDumper(self.cls)


class TimeTzBinaryDumper(_BaseTimeDumper):

    format = Format.BINARY
    _oid = builtins["timetz"].oid

    def dump(self, obj: time) -> bytes:
        ms = obj.microsecond + 1_000_000 * (
            obj.second + 60 * (obj.minute + 60 * obj.hour)
        )
        off = obj.utcoffset()
        assert off is not None
        return _pack_timetz(ms, -int(off.total_seconds()))


class _BaseDateTimeDumper(Dumper):
    def get_key(
        self, obj: datetime, format: Pg3Format
    ) -> Union[type, Tuple[type]]:
        # Use (cls,) to report the need to upgrade (downgrade, actually) to a
        # dumper for naive timestamp.
        if obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    def upgrade(self, obj: datetime, format: Pg3Format) -> Dumper:
        raise NotImplementedError


class _BaseDateTimeTextDumper(_BaseDateTimeDumper):

    format = Format.TEXT

    def dump(self, obj: datetime) -> bytes:
        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        return str(obj).encode("utf8")


class DateTimeTzDumper(_BaseDateTimeTextDumper):

    _oid = builtins["timestamptz"].oid

    def upgrade(self, obj: datetime, format: Pg3Format) -> Dumper:
        if obj.tzinfo:
            return self
        else:
            return DateTimeDumper(self.cls)


class DateTimeDumper(_BaseDateTimeTextDumper):

    _oid = builtins["timestamp"].oid


class DateTimeTzBinaryDumper(_BaseDateTimeDumper):

    format = Format.BINARY
    _oid = builtins["timestamptz"].oid

    def dump(self, obj: datetime) -> bytes:
        delta = obj - _pg_datetimetz_epoch
        micros = delta.microseconds + 1_000_000 * (
            86_400 * delta.days + delta.seconds
        )
        return _pack_int8(micros)

    def upgrade(self, obj: datetime, format: Pg3Format) -> Dumper:
        if obj.tzinfo:
            return self
        else:
            return DateTimeBinaryDumper(self.cls)


class DateTimeBinaryDumper(_BaseDateTimeDumper):

    format = Format.BINARY
    _oid = builtins["timestamp"].oid

    def dump(self, obj: datetime) -> bytes:
        delta = obj - _pg_datetime_epoch
        micros = delta.microseconds + 1_000_000 * (
            86_400 * delta.days + delta.seconds
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

    _ORDER_YMD = 0
    _ORDER_DMY = 1
    _ORDER_MDY = 2

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        ds = _get_datestyle(self.connection)
        if ds.startswith(b"I"):  # ISO
            self._order = self._ORDER_YMD
        elif ds.startswith(b"G"):  # German
            self._order = self._ORDER_DMY
        elif ds.startswith(b"S") or ds.startswith(b"P"):  # SQL or Postgres
            self._order = (
                self._ORDER_DMY if ds.endswith(b"DMY") else self._ORDER_MDY
            )
        else:
            raise InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    def load(self, data: Buffer) -> date:
        if self._order == self._ORDER_YMD:
            ye = data[:4]
            mo = data[5:7]
            da = data[8:]
        elif self._order == self._ORDER_DMY:
            da = data[:2]
            mo = data[3:5]
            ye = data[6:]
        else:
            mo = data[:2]
            da = data[3:5]
            ye = data[6:]

        try:
            return date(int(ye), int(mo), int(da))
        except ValueError as e:
            s = bytes(data).decode("utf8", "replace")
            if len(s) != 10:
                raise DataError(f"date not supported: {s!r}") from None
            raise DataError(f"can't parse date {s!r}: {e}") from None


class DateBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> date:
        days = _unpack_int4(data)[0] + _pg_date_epoch_days
        try:
            return date.fromordinal(days)
        except ValueError:
            if days < _py_date_min_days:
                raise DataError("date too small (before year 1)") from None
            else:
                raise DataError("date too large (after year 10K)") from None


class TimeLoader(Loader):

    format = Format.TEXT

    _re_format = re.compile(rb"^(\d+):(\d+):(\d+)(?:\.(\d+))?")

    def load(self, data: Buffer) -> time:
        m = self._re_format.match(data)
        if not m:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse time {s!r}")

        ho, mi, se, ms = m.groups()

        # Pad the fraction of second to get millis
        if ms:
            if len(ms) == 6:
                ims = int(ms)
            else:
                ims = int(ms + _ms_trail[len(ms)])
        else:
            ims = 0

        try:
            return time(int(ho), int(mi), int(se), ims)
        except ValueError as e:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse time {s!r}: {e}") from None


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
            raise DataError(
                f"time not supported by Python: hour={h}"
            ) from None


class TimetzLoader(Loader):

    format = Format.TEXT
    _py37 = sys.version_info >= (3, 7)

    _re_format = re.compile(
        rb"""(?ix)
        ^
        (\d+) : (\d+) : (\d+) (?: \. (\d+) )?       # Time and micros
        ([-+]) (\d+) (?: : (\d+) )? (?: : (\d+) )?  # Timezone
        $
        """
    )

    def load(self, data: Buffer) -> time:
        m = self._re_format.match(data)
        if not m:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse timetz {s!r}")

        ho, mi, se, ms, sgn, oh, om, os = m.groups()

        # Pad the fraction of second to get millis
        if ms:
            if len(ms) == 6:
                ims = int(ms)
            else:
                ims = int(ms + _ms_trail[len(ms)])
        else:
            ims = 0

        # Calculate timezone
        off = 60 * 60 * int(oh)
        if om:
            off += 60 * int(om)
        if os and self._py37:
            off += int(os)
        tz = timezone(timedelta(0, off if sgn == b"+" else -off))

        try:
            return time(int(ho), int(mi), int(se), ims, tz)
        except ValueError as e:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse timetz {s!r}: {e}") from None


class TimetzBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> time:
        val, off = _unpack_timetz(data)

        val, ms = divmod(val, 1_000_000)
        val, s = divmod(val, 60)
        h, m = divmod(val, 60)

        try:
            return time(h, m, s, ms, self._tz_from_sec(off))
        except ValueError:
            raise DataError(
                f"time not supported by Python: hour={h}"
            ) from None

    def _tz_from_sec(self, sec: int) -> timezone:
        return timezone(timedelta(seconds=-sec))

    def _tz_from_sec_36(self, sec: int) -> timezone:
        if sec % 60:
            sec = round(sec / 60.0) * 60
        return timezone(timedelta(seconds=-sec))


if sys.version_info < (3, 7):
    setattr(
        TimetzBinaryLoader, "_tz_from_sec", TimetzBinaryLoader._tz_from_sec_36
    )


class TimestampLoader(Loader):

    format = Format.TEXT

    _re_format = re.compile(
        rb"""(?ix)
        ^
        (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)   # Date
        (?: T | [^a-z0-9] )                     # Separator, including T
        (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)   # Time
        (?: \.(\d+) )?                          # Micros
        $
        """
    )
    _re_format_pg = re.compile(
        rb"""(?ix)
        ^
        [a-z]+          [^a-z0-9]               # DoW, separator
        (\d+|[a-z]+)    [^a-z0-9]               # Month or day
        (\d+|[a-z]+)    [^a-z0-9]               # Month or day
        (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)   # Time
        (?: \.(\d+) )?                          # Micros
        [^a-z0-9] (\d+)                         # Year
        $
        """
    )

    _ORDER_YMD = 0
    _ORDER_DMY = 1
    _ORDER_MDY = 2
    _ORDER_PGDM = 3
    _ORDER_PGMD = 4

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)

        ds = _get_datestyle(self.connection)
        if ds.startswith(b"I"):  # ISO
            self._order = self._ORDER_YMD
        elif ds.startswith(b"G"):  # German
            self._order = self._ORDER_DMY
        elif ds.startswith(b"S"):  # SQL
            self._order = (
                self._ORDER_DMY if ds.endswith(b"DMY") else self._ORDER_MDY
            )
        elif ds.startswith(b"P"):  # Postgres
            self._order = (
                self._ORDER_PGDM if ds.endswith(b"DMY") else self._ORDER_PGMD
            )
            self._re_format = self._re_format_pg
        else:
            raise InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    def load(self, data: Buffer) -> datetime:
        m = self._re_format.match(data)
        if not m:
            s = bytes(data).decode("utf8", "replace")
            if s.endswith("BC"):
                raise DataError(f"BC timestamps not supported, got {s!r}")
            raise DataError(f"can't parse timestamp {s!r}")

        if self._order == self._ORDER_YMD:
            ye, mo, da, ho, mi, se, ms = m.groups()
            imo = int(mo)
        elif self._order == self._ORDER_DMY:
            da, mo, ye, ho, mi, se, ms = m.groups()
            imo = int(mo)
        elif self._order == self._ORDER_MDY:
            mo, da, ye, ho, mi, se, ms = m.groups()
            imo = int(mo)
        else:
            if self._order == self._ORDER_PGDM:
                da, mo, ho, mi, se, ms, ye = m.groups()
            else:
                mo, da, ho, mi, se, ms, ye = m.groups()
            try:
                imo = _month_abbr[mo]
            except KeyError:
                s = mo.decode("utf8", "replace")
                raise DataError(f"can't parse month: {s!r}") from None

        # Pad the fraction of second to get millis
        if ms:
            if len(ms) == 6:
                ims = int(ms)
            else:
                ims = int(ms + _ms_trail[len(ms)])
        else:
            ims = 0

        try:
            return datetime(
                int(ye), imo, int(da), int(ho), int(mi), int(se), ims
            )
        except ValueError as e:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse timestamp {s!r}: {e}") from None


class TimestampBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> datetime:
        micros = _unpack_int8(data)[0]
        try:
            return _pg_datetime_epoch + timedelta(microseconds=micros)
        except OverflowError:
            if micros <= 0:
                raise DataError(
                    "timestamp too small (before year 1)"
                ) from None
            else:
                raise DataError(
                    "timestamp too large (after year 10K)"
                ) from None


class TimestamptzLoader(Loader):

    format = Format.TEXT
    _re_format = re.compile(
        rb"""(?ix)
        ^
        (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)       # Date
        (?: T | [^a-z0-9] )                         # Separator, including T
        (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)       # Time
        (?: \.(\d+) )?                              # Micros
        ([-+]) (\d+) (?: : (\d+) )? (?: : (\d+) )?  # Timezone
        $
        """
    )

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._timezone = get_tzinfo(
            self.connection.pgconn if self.connection else None
        )

        ds = _get_datestyle(self.connection)
        if not ds.startswith(b"I"):  # not ISO
            setattr(self, "load", self._load_notimpl)

    def load(self, data: Buffer) -> datetime:
        m = self._re_format.match(data)
        if not m:
            s = bytes(data).decode("utf8", "replace")
            if s.endswith("BC"):
                raise DataError(f"BC timestamps not supported, got {s!r}")
            raise DataError(f"can't parse timestamp {s!r}")

        ye, mo, da, ho, mi, se, ms, sgn, oh, om, os = m.groups()

        # Pad the fraction of second to get millis
        if ms:
            if len(ms) == 6:
                ims = int(ms)
            else:
                ims = int(ms + _ms_trail[len(ms)])
        else:
            ims = 0

        # Calculate timezone offset
        soff = 60 * 60 * int(oh)
        if om:
            soff += 60 * int(om)
        if os:
            soff += int(os)
        tzoff = timedelta(0, soff if sgn == b"+" else -soff)

        try:
            dt = datetime(
                int(ye), int(mo), int(da), int(ho), int(mi), int(se), ims, utc
            )
            return (dt - tzoff).astimezone(self._timezone)
        except ValueError as e:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse timestamptz {s!r}: {e}") from None

    def _load_notimpl(self, data: Buffer) -> datetime:
        s = bytes(data).decode("utf8", "replace")
        ds = _get_datestyle(self.connection).decode("ascii")
        raise NotImplementedError(
            f"can't parse timestamptz with DateStyle {ds!r}: {s!r}"
        )


class TimestamptzBinaryLoader(Loader):

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
                raise DataError(
                    "timestamp too small (before year 1)"
                ) from None
            else:
                raise DataError(
                    "timestamp too large (after year 10K)"
                ) from None


class IntervalLoader(Loader):

    format = Format.TEXT

    _re_interval = re.compile(
        br"""
        (?: ([-+]?\d+) \s+ years? \s* )?                # Years
        (?: ([-+]?\d+) \s+ mons? \s* )?                 # Months
        (?: ([-+]?\d+) \s+ days? \s* )?                 # Days
        (?: ([-+])? (\d+) : (\d+) : (\d+ (?:\.\d+)?)    # Time
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
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse interval {s!r}")

        ye, mo, da, sgn, ho, mi, se = m.groups()
        days = 0
        seconds = 0.0

        if ye:
            days += 365 * int(ye)
        if mo:
            days += 30 * int(mo)
        if da:
            days += int(da)

        if ho:
            seconds = 3600 * int(ho) + 60 * int(mi) + float(se)
            if sgn == b"-":
                seconds = -seconds

        try:
            return timedelta(days=days, seconds=seconds)
        except OverflowError as e:
            s = bytes(data).decode("utf8", "replace")
            raise DataError(f"can't parse interval {s!r}: {e}") from None

    def _load_notimpl(self, data: Buffer) -> timedelta:
        s = bytes(data).decode("utf8", "replace")
        ints = (
            self.connection
            and self.connection.pgconn.parameter_status(b"IntervalStyle")
            or b"unknown"
        ).decode("utf8", "replace")
        raise NotImplementedError(
            f"can't parse interval with IntervalStyle {ints}: {s!r}"
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


def _get_datestyle(conn: Optional["BaseConnection[Any]"]) -> bytes:
    if conn:
        ds = conn.pgconn.parameter_status(b"DateStyle")
        if ds:
            return ds

    return b"ISO, DMY"


_month_abbr = {
    n: i
    for i, n in enumerate(
        b"Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1
    )
}

# Pad to get milliseconds from a fraction of seconds
_ms_trail = [b"000000", b"00000", b"0000", b"000", b"00", b"0"]
