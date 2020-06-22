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


@Dumper.text(datetime.date)
@Dumper.binary(datetime.date)
def dump_date(obj: datetime.date) -> Tuple[bytes, int]:
    # TODO https://stackoverflow.com/questions/13468126/a-faster-strptime
    # Optimize inspiration from isoformat
    # https://github.com/python/cpython/blob/master/Lib/datetime.py#L849
    # String Slicing
    return _encode(str(obj))[0], builtins["date"].oid


@Dumper.text(datetime.time)
@Dumper.binary(datetime.time)
def dump_time(obj: datetime.time) -> Tuple[bytes, int]:
    if obj.tzinfo is None:
        return _encode(str(obj))[0], builtins["time"].oid
    else:
        return _encode(str(obj))[0], builtins["timetz"].oid


@Dumper.text(datetime.datetime)
@Dumper.binary(datetime.datetime)
def dump_datetime(obj: datetime.datetime) -> Tuple[bytes, int]:
    if obj.tzinfo is None:
        return _encode(str(obj))[0], builtins["timestamp"].oid
    else:
        return _encode(str(obj))[0], builtins["timestamptz"].oid


@Dumper.text(datetime.timedelta)
@Dumper.binary(datetime.timedelta)
def dump_timedelta(obj: datetime.timedelta) -> Tuple[bytes, int]:
    return _encode(str(obj))[0], builtins["interval"].oid


@Loader.text(builtins["date"].oid)
def load_date(data: bytes) -> datetime.date:
    # TODO Multiple formats perhaps could do a try-except for loop?
    return datetime.datetime.strptime(_decode(data)[0], "%Y-%m-%d").date()


@Loader.binary(builtins["date"].oid)
def load_date_binary(data: bytes) -> datetime.date:
    dateADT = struct.unpack(">I", data)[0]  # num days since 2020-01-01
    ADT_Date = datetime.date(year=2000, month=1, day=1)
    # dateADT > datetime.date - ADT_Date + constant for good measure
    if dateADT > 3000000:
        # Negative date
        return ADT_Date - datetime.timedelta(days=4294967296 - dateADT)
    else:
        return ADT_Date + datetime.timedelta(days=dateADT)


@Loader.text(builtins["time"].oid)
def load_time(data: bytes) -> datetime.time:
    return datetime.datetime.strptime(_decode(data)[0], "%H:%M:%S").time()


@Loader.text(builtins["timetz"].oid)
def load_time_tz(data: bytes) -> datetime.time:
    # FIXME It could also be try/except ValueError to combine timetz with time
    """
    try:
        datetime.datetime.strptime(_decode(data)[0], "%H:%M:%S%z").timetz()
    except ValueError:
        datetime.datetime.strptime(_decode(data)[0], "%H:%M:%S").time()
    """

    return datetime.datetime.strptime(_decode(data)[0], "%H:%M:%S%z").timetz()


@Loader.binary(builtins["time"].oid)
@Loader.binary(builtins["timetz"].oid)
def load_time_binary(data: bytes) -> datetime.time:
    raise NotImplementedError


@Loader.text(builtins["timestamp"].oid)
def load_datetime(data: bytes) -> datetime.datetime:
    return datetime.datetime.strptime(_decode(data)[0], "%Y-%m-%d%H:%M:%S")


@Loader.text(builtins["timestamptz"].oid)
def load_datetime_tz(data: bytes) -> datetime.datetime:
    return datetime.datetime.strptime(_decode(data)[0], "%Y-%m-%d%H:%M:%S%z")


@Loader.binary(builtins["timestamp"].oid)
@Loader.binary(builtins["timestamptz"].oid)
def load_datetime_binary(data: bytes) -> datetime.datetime:
    # 8 bytes
    raise NotImplementedError


@Loader.text(builtins["interval"].oid)
def load_interval(data: bytes) -> datetime.timedelta:
    raise NotImplementedError


@Loader.binary(builtins["interval"].oid)
def load_interval_binary(data: bytes) -> datetime.timedelta:
    # 16 bytes
    raise NotImplementedError
