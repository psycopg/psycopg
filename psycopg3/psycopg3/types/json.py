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

# Global load/dump functions, used by default.
_loads: JsonLoadsFunction = json.loads
_dumps: JsonDumpsFunction = json.dumps


def set_json_dumps(dumps: JsonDumpsFunction) -> None:
    """
    Set a global JSON serialisation function to use by default by JSON dumpers.

    Defaults to the builtin `json.dumps()`. You can override it to use a
    different JSON library or to use customised arguments.

    If you need a non-global customisation you can subclass the `!JsonDumper`
    family of classes, overriding the `!get_loads()` method, and register
    your class in the context required.
    """
    global _dumps
    _dumps = dumps


def set_json_loads(loads: JsonLoadsFunction) -> None:
    """
    Set a global JSON parsing function to use by default by the JSON loaders.

    Defaults to the builtin `json.loads()`. You can override it to use a
    different JSON library or to use customised arguments.

    If you need a non-global customisation you can subclass the `!JsonLoader`
    family of classes, overriding the `!get_loads()` method, and register
    your class in the context required.
    """
    global _loads
    _loads = loads


class _JsonWrapper:
    __slots__ = ("obj",)

    def __init__(self, obj: Any):
        self.obj = obj

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

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._dumps = self.get_dumps()

    def get_dumps(self) -> JsonDumpsFunction:
        r"""
        Return a `json.dumps()`\-compatible function to serialize the object.

        Subclasses can override this function to specify custom JSON
        serialization per context.
        """
        return _dumps

    def dump(self, obj: _JsonWrapper) -> bytes:
        return self._dumps(obj.obj).encode("utf-8")


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
        return b"\x01" + self._dumps(obj.obj).encode("utf-8")


class _JsonLoader(Loader):
    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._loads = self.get_loads()

    def get_loads(self) -> JsonLoadsFunction:
        r"""
        Return a `json.loads()`\-compatible function to de-serialize the value.

        Subclasses can override this function to specify custom JSON
        de-serialization per context.
        """
        return _loads

    def load(self, data: Buffer) -> Any:
        # json.loads() cannot work on memoryview.
        if isinstance(data, memoryview):
            data = bytes(data)
        return self._loads(data)


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
        return self._loads(data)
