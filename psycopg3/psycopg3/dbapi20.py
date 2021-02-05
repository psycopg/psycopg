"""
Compatibility objects with DBAPI 2.0
"""

# Copyright (C) 2020-2021 The Psycopg Team

import time
import datetime as dt
from math import floor
from typing import Any, Sequence

from .pq import Format
from .oids import postgres_types as builtins
from .adapt import Dumper


class DBAPITypeObject:
    def __init__(self, name: str, type_names: Sequence[str]):
        self.name = name
        self.values = tuple(builtins[n].oid for n in type_names)

    def __repr__(self) -> str:
        return f"psycopg3.{self.name}"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, int):
            return other in self.values
        else:
            return NotImplemented

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, int):
            return other not in self.values
        else:
            return NotImplemented


BINARY = DBAPITypeObject("BINARY", ("bytea",))
DATETIME = DBAPITypeObject(
    "DATETIME", "timestamp timestamptz date time timetz interval".split()
)
NUMBER = DBAPITypeObject(
    "NUMBER", "int2 int4 int8 float4 float8 numeric".split()
)
ROWID = DBAPITypeObject("ROWID", ("oid",))
STRING = DBAPITypeObject("STRING", "text varchar bpchar".split())


class Binary:
    def __init__(self, obj: Any):
        self.obj = obj


class BinaryDumper(Dumper):

    format = Format.TEXT
    oid = builtins["bytea"].oid

    def dump(self, obj: Binary) -> bytes:
        wrapped = obj.obj
        if isinstance(wrapped, bytes):
            return wrapped
        else:
            return bytes(wrapped)


def Date(year: int, month: int, day: int) -> dt.date:
    return dt.date(year, month, day)


def DateFromTicks(ticks: float) -> dt.date:
    return TimestampFromTicks(ticks).date()


def Time(hour: int, minute: int, second: int) -> dt.time:
    return dt.time(hour, minute, second)


def TimeFromTicks(ticks: float) -> dt.time:
    return TimestampFromTicks(ticks).time()


def Timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> dt.datetime:
    return dt.datetime(year, month, day, hour, minute, second)


def TimestampFromTicks(ticks: float) -> dt.datetime:
    secs = floor(ticks)
    frac = ticks - secs
    t = time.localtime(ticks)
    tzinfo = dt.timezone(dt.timedelta(seconds=t.tm_gmtoff))
    rv = dt.datetime(*t[:6], round(frac * 1_000_000), tzinfo=tzinfo)
    return rv
