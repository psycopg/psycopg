"""
Adapters for datetime types.
https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-TABLE
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import datetime
import struct
from typing import Tuple

from ..adapt import Dumper, Loader
from .oids import builtins

_encode = codecs.lookup("ascii").encode
_decode = codecs.lookup("ascii").decode

_min_datetime = datetime.datetime(year=1, month=1, day=1)
_middle_datetime = datetime.datetime(year=2000, month=1, day=1)
_middle_date = datetime.date(year=2000, month=1, day=1)

_date_struct = struct.Struct("!i")
_time_struct = struct.Struct("!q")
_datetime_tz_struct = struct.Struct("!2i")


@Dumper.text(datetime.date)
def dump_date(obj: datetime.date) -> Tuple[bytes, int]:
    # TODO https://stackoverflow.com/questions/13468126/a-faster-strptime
    # Optimize inspiration from isoformat
    # https://github.com/python/cpython/blob/master/Lib/datetime.py#L849
    # String Slicing
    return _encode(str(obj))[0], builtins["date"].oid


@Dumper.text(datetime.time)
def dump_time(obj: datetime.time) -> Tuple[bytes, int]:
    if obj.tzinfo is None:
        return _encode(str(obj))[0], builtins["time"].oid
    else:
        return _encode(str(obj))[0], builtins["timetz"].oid


@Dumper.text(datetime.datetime)
def dump_datetime(obj: datetime.datetime) -> Tuple[bytes, int]:
    if obj.tzinfo is None:
        return _encode(str(obj))[0], builtins["timestamp"].oid
    else:
        return _encode(str(obj))[0], builtins["timestamptz"].oid


@Dumper.text(datetime.timedelta)
def dump_timedelta(obj: datetime.timedelta) -> Tuple[bytes, int]:
    return _encode(str(obj))[0], builtins["interval"].oid


@Loader.text(builtins["date"].oid)
def load_date(data: bytes) -> datetime.date:
    return datetime.datetime.strptime(_decode(data)[0], "%Y-%m-%d").date()


@Loader.binary(builtins["date"].oid)
def load_date_binary(data: bytes) -> datetime.date:
    days_since_middle_date = _date_struct.unpack(data)[0]
    return _middle_date + datetime.timedelta(days=days_since_middle_date)


@Loader.text(builtins["time"].oid)
def load_time(data: bytes) -> datetime.time:
    return datetime.datetime.strptime(_decode(data)[0], "%H:%M:%S").time()


@Loader.binary(builtins["time"].oid)
def load_time_binary(data: bytes) -> datetime.time:
    return (
        _min_datetime
        + datetime.timedelta(microseconds=_time_struct.unpack(data)[0])
    ).time()


@Loader.text(builtins["timetz"].oid)
def load_time_tz(data: bytes) -> datetime.time:
    decoded: str = _decode(data)[0]
    if decoded[-3] in "+-":
        # Python usually expects +HHMM format for TZ
        decoded += "00"
    return datetime.datetime.strptime(decoded, "%H:%M:%S%z").timetz()


@Loader.binary(builtins["timetz"].oid)
def load_time_tz_binary(data: bytes) -> datetime.time:
    microseconds, timezone = struct.unpack("!q i", data)
    return (
        datetime.datetime(
            year=1,
            month=1,
            day=1,
            tzinfo=datetime.timezone(datetime.timedelta(seconds=-timezone)),
        )
        + datetime.timedelta(microseconds=microseconds)
    ).timetz()


@Loader.text(builtins["timestamp"].oid)
def load_datetime(data: bytes) -> datetime.datetime:
    return datetime.datetime.strptime(_decode(data)[0], "%Y-%m-%d %H:%M:%S")


@Loader.binary(builtins["timestamp"].oid)
def load_datetime_binary(data: bytes) -> datetime.datetime:
    return _middle_datetime + datetime.timedelta(
        microseconds=_time_struct.unpack(data)[0]
    )


@Loader.text(builtins["timestamptz"].oid)
def load_datetime_tz(data: bytes) -> datetime.datetime:
    decoded: str = _decode(data)[0]
    if decoded[-3] in "+-":
        # Python usually expects +HHMM format for TZ
        decoded += "00"
    return datetime.datetime.strptime(decoded, "%Y-%m-%d %H:%M:%S%z")


@Loader.binary(builtins["timestamptz"].oid)
def load_datetime_tz_binary(data: bytes) -> datetime.datetime:
    seconds, timezone = _datetime_tz_struct.unpack(data)
    return datetime.datetime(
        year=2000,
        month=1,
        day=1,
        tzinfo=datetime.timezone(datetime.timedelta(microseconds=-timezone)),
    ) + datetime.timedelta(seconds=-seconds)


@Loader.text(builtins["interval"].oid)
def load_interval(data: bytes) -> datetime.timedelta:
    raise NotImplementedError


@Loader.binary(builtins["interval"].oid)
def load_interval_binary(data: bytes) -> datetime.timedelta:
    # 16 bytes
    # https://doxygen.postgresql.org/structInterval.html
    # microseconds, days?, months? = struct.unpack("!q i i", data)
    raise NotImplementedError
