"""
Adapers for JSON types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import json
from typing import Any, Callable, Optional

from ..pq import Format
from ..oids import builtins
from ..adapt import Buffer, Dumper, Loader
from ..errors import DataError

JsonDumpsFunction = Callable[[Any], str]


class _JsonWrapper:
    __slots__ = ("obj", "_dumps")

    def __init__(self, obj: Any, dumps: Optional[JsonDumpsFunction] = None):
        self.obj = obj
        self._dumps: JsonDumpsFunction = dumps or json.dumps

    def __repr__(self) -> str:
        sobj = repr(self.obj)
        if len(sobj) > 40:
            sobj = f"{sobj[:35]} ... ({len(sobj)} chars)"
        return f"{self.__class__.__name__}({sobj})"

    def dumps(self) -> str:
        return self._dumps(self.obj)


class Json(_JsonWrapper):
    __slots__ = ()


class Jsonb(_JsonWrapper):
    __slots__ = ()


class _JsonDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj: _JsonWrapper) -> bytes:
        return obj.dumps().encode("utf-8")


class JsonDumper(_JsonDumper):

    format = Format.TEXT
    _oid = builtins["json"].oid


class JsonBinaryDumper(JsonDumper):

    format = Format.BINARY


class JsonbDumper(_JsonDumper):

    format = Format.TEXT
    _oid = builtins["jsonb"].oid


class JsonbBinaryDumper(JsonbDumper):

    format = Format.BINARY

    def dump(self, obj: _JsonWrapper) -> bytes:
        return b"\x01" + obj.dumps().encode("utf-8")


class JsonLoader(Loader):

    format = Format.TEXT

    def load(self, data: Buffer) -> Any:
        # Json crashes on memoryview
        if isinstance(data, memoryview):
            data = bytes(data)
        return json.loads(data)


class JsonBinaryLoader(JsonLoader):

    format = Format.BINARY


class JsonbBinaryLoader(Loader):

    format = Format.BINARY

    def load(self, data: Buffer) -> Any:
        if data and data[0] != 1:
            raise DataError("unknown jsonb binary format: {data[0]}")
        data = data[1:]
        if isinstance(data, memoryview):
            data = bytes(data)
        return json.loads(data)
