"""
Adapters for datetime types.
https://www.postgresql.org/docs/9.4/datatype-datetime.html#DATATYPE-DATETIME-TABLE
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import datetime
from typing import Union, Optional, Tuple

from ..adapt import Dumper, Loader
from ..proto import AdaptContext, DecodeFunc
from .oids import builtins


_encode = codecs.lookup("ascii").encode
_decode = codecs.lookup("ascii").decode


_time_oids = (
    builtins["time"].oid,
    builtins["timetz"].oid,
    builtins["timestamp"].oid,
    builtins["timestamptz"].oid,
)


_timezone_oids = (
    builtins["timetz"].oid,
    builtins["timestamptz"].oid,
)


@Dumper.text(datetime.date)
@Dumper.binary(datetime.date)
def dump_date(obj: datetime.date) -> Tuple[bytes, int]:
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
@Loader.binary(builtins["date"].oid)
@Loader.text(builtins["time"].oid)
@Loader.binary(builtins["time"].oid)
@Loader.text(builtins["timetz"].oid)
@Loader.binary(builtins["timetz"].oid)
@Loader.text(builtins["timestamp"].oid)
@Loader.binary(builtins["timestamp"].oid)
@Loader.text(builtins["timestamptz"].oid)
@Loader.binary(builtins["timestamptz"].oid)
class DateTimeLoader(Loader):

    decode: Optional[DecodeFunc]

    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)

        if self.connection is not None:
            if self.connection.encoding != "SQL_ASCII":
                self.decode = self.connection.codec.decode
            else:
                self.decode = _decode
        else:
            self.decode = codecs.lookup("utf8").decode

    def load(self, data: bytes) -> Union[datetime.datetime, datetime.date, datetime.time]:
        obj = datetime.datetime.strptime(
            self.decode(data)[0],
            f"{'%Y-%m-%d' if self.oid == builtins['date'].oid else ''}"
            f"{'%H:%M:%S' if self.oid in _time_oids else ''}"
            f"{'%z' if self.oid in _timezone_oids else ''}"
        )
        if self.oid == builtins['date'].oid:
            return obj.date()
        elif self.oid in _timezone_oids:
            return obj
        else:
            return obj.time()


@Loader.text(builtins["interval"].oid)
def load_interval(data: bytes) -> datetime.timedelta:
    pass


@Loader.binary(builtins["interval"].oid)
def load_interval_binary(data: bytes) -> datetime.timedelta:
    pass
