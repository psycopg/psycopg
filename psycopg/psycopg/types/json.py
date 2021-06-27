"""
Adapers for JSON types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import json
from typing import Any, Callable, Optional, Union

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader
from ..proto import AdaptContext
from ..errors import DataError

JsonDumpsFunction = Callable[[Any], str]
JsonLoadsFunction = Callable[[Union[str, bytes, bytearray]], Any]


def set_json_dumps(dumps: JsonDumpsFunction) -> None:
    """
    Set a global JSON serialisation function to use by default by JSON dumpers.

    By default dumping JSON uses the builtin `json.dumps()`. You can override
    it to use a different JSON library or to use customised arguments.
    """
    _JsonDumper._dumps = dumps


def set_json_loads(loads: JsonLoadsFunction) -> None:
    """
    Set a global JSON parsing function to use by default by the JSON loaders.

    By default loading JSON uses the builtin `json.loads()`. You can override
    it to use a different JSON library or to use customised arguments.
    """
    _JsonLoader._loads = loads


class _JsonWrapper:
    __slots__ = ("obj", "dumps")

    def __init__(self, obj: Any, dumps: Optional[JsonDumpsFunction] = None):
        self.obj = obj
        self.dumps = dumps

    def __repr__(self) -> str:
        sobj = repr(self.obj)
        if len(sobj) > 40:
            sobj = f"{sobj[:35]} ... ({len(sobj)} chars)"
        return f"{self.__class__.__name__}({sobj})"


class Json(_JsonWrapper):
    __slots__ = ()


class Jsonb(_JsonWrapper):
    __slots__ = ()


class _JsonDumper(Dumper):

    format = Format.TEXT

    # The globally used JSON dumps() function. It can be changed globally (by
    # set_json_dumps) or by a subclass.
    _dumps: JsonDumpsFunction = json.dumps

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self.dumps = self.__class__._dumps

    def dump(self, obj: _JsonWrapper) -> bytes:
        dumps = obj.dumps or self.dumps
        return dumps(obj.obj).encode("utf-8")


class JsonDumper(_JsonDumper):

    format = Format.TEXT
    _oid = builtins["json"].oid


class JsonBinaryDumper(_JsonDumper):

    format = Format.BINARY
    _oid = builtins["json"].oid


class JsonbDumper(_JsonDumper):

    format = Format.TEXT
    _oid = builtins["jsonb"].oid


class JsonbBinaryDumper(_JsonDumper):

    format = Format.BINARY
    _oid = builtins["jsonb"].oid

    def dump(self, obj: _JsonWrapper) -> bytes:
        dumps = obj.dumps or self.dumps
        return b"\x01" + dumps(obj.obj).encode("utf-8")


class _JsonLoader(Loader):

    # The globally used JSON loads() function. It can be changed globally (by
    # set_json_loads) or by a subclass.
    _loads: JsonLoadsFunction = json.loads

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self.loads = self.__class__._loads

    def load(self, data: Buffer) -> Any:
        # json.loads() cannot work on memoryview.
        if isinstance(data, memoryview):
            data = bytes(data)
        return self.loads(data)


class JsonLoader(_JsonLoader):
    format = Format.TEXT


class JsonbLoader(_JsonLoader):
    format = Format.TEXT


class JsonBinaryLoader(_JsonLoader):
    format = Format.BINARY


class JsonbBinaryLoader(_JsonLoader):

    format = Format.BINARY

    def load(self, data: Buffer) -> Any:
        if data and data[0] != 1:
            raise DataError("unknown jsonb binary format: {data[0]}")
        data = data[1:]
        if isinstance(data, memoryview):
            data = bytes(data)
        return self.loads(data)


def register_default_globals(ctx: AdaptContext) -> None:
    # Currently json binary format is nothing different than text, maybe with
    # an extra memcopy we can avoid.
    JsonBinaryDumper.register(Json, ctx)
    JsonDumper.register(Json, ctx)
    JsonbBinaryDumper.register(Jsonb, ctx)
    JsonbDumper.register(Jsonb, ctx)
    JsonLoader.register("json", ctx)
    JsonbLoader.register("jsonb", ctx)
    JsonBinaryLoader.register("json", ctx)
    JsonbBinaryLoader.register("jsonb", ctx)
