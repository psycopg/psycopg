"""
Adapers for JSON types.
"""

# Copyright (C) 2020 The Psycopg Team

import json
from typing import Any, Callable, Optional

from ..oids import builtins
from ..adapt import Dumper, Loader
from ..errors import DataError

JSON_OID = builtins["json"].oid
JSONB_OID = builtins["jsonb"].oid

JsonDumpsFunction = Callable[[Any], str]


class _JsonWrapper:
    def __init__(self, obj: Any, dumps: Optional[JsonDumpsFunction] = None):
        self.obj = obj
        self._dumps: JsonDumpsFunction = dumps or json.dumps

    def dumps(self) -> str:
        return self._dumps(self.obj)


class Json(_JsonWrapper):
    pass


class Jsonb(_JsonWrapper):
    pass


class _JsonDumper(Dumper):
    def dump(self, obj: _JsonWrapper) -> bytes:
        return obj.dumps().encode("utf-8")


@Dumper.text(Json)
@Dumper.binary(Json)
class JsonDumper(_JsonDumper):
    oid = JSON_OID


@Dumper.text(Jsonb)
class JsonbDumper(_JsonDumper):
    oid = JSONB_OID


@Dumper.binary(Jsonb)
class JsonbBinaryDumper(JsonbDumper):
    def dump(self, obj: _JsonWrapper) -> bytes:
        return b"\x01" + obj.dumps().encode("utf-8")


@Loader.text(builtins["json"].oid)
@Loader.text(builtins["jsonb"].oid)
@Loader.binary(builtins["json"].oid)
class JsonLoader(Loader):
    def load(self, data: bytes) -> Any:
        return json.loads(data)


@Loader.binary(builtins["jsonb"].oid)
class JsonbBinaryLoader(Loader):
    def load(self, data: bytes) -> Any:
        if data and data[0] != 1:
            raise DataError("unknown jsonb binary format: {data[0]}")
        return json.loads(data[1:])
