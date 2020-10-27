"""
Adapers for JSON types.
"""

# Copyright (C) 2020 The Psycopg Team

import json
import codecs
from typing import Any, Callable, Optional

from .oids import builtins
from ..adapt import Dumper, Loader
from ..proto import EncodeFunc
from ..errors import DataError

_encode_utf8 = codecs.lookup("utf8").encode

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


class JsonB(_JsonWrapper):
    pass


class _JsonDumper(Dumper):
    _oid: int

    def dump(
        self, obj: _JsonWrapper, __encode: EncodeFunc = _encode_utf8
    ) -> bytes:
        return __encode(obj.dumps())[0]

    @property
    def oid(self) -> int:
        return self._oid


@Dumper.text(Json)
@Dumper.binary(Json)
class JsonDumper(_JsonDumper):
    _oid = JSON_OID


@Dumper.text(JsonB)
class JsonBDumper(_JsonDumper):
    _oid = JSONB_OID


@Dumper.binary(JsonB)
class BinaryJsonBDumper(JsonBDumper):
    def dump(
        self, obj: _JsonWrapper, __encode: EncodeFunc = _encode_utf8
    ) -> bytes:
        return b"\x01" + __encode(obj.dumps())[0]


@Loader.text(builtins["json"].oid)
@Loader.text(builtins["jsonb"].oid)
@Loader.binary(builtins["json"].oid)
class JsonLoader(Loader):
    def load(self, data: bytes) -> Any:
        return json.loads(data)


@Loader.binary(builtins["jsonb"].oid)
class BinaryJsonBLoader(Loader):
    def load(self, data: bytes) -> Any:
        if data and data[0] != 1:
            raise DataError("unknown jsonb binary format: {data[0]}")
        return json.loads(data[1:])
