"""
Adapers for JSON types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import json
from typing import Any, Callable, Optional, Type, Union

from ..pq import Format
from ..oids import postgres_types as builtins
from ..adapt import Buffer, Dumper, Loader
from ..proto import AdaptContext
from ..errors import DataError

JsonDumpsFunction = Callable[[Any], str]
JsonLoadsFunction = Callable[[Union[str, bytes, bytearray]], Any]


def set_json_dumps(
    dumps: JsonDumpsFunction, context: Optional[AdaptContext] = None
) -> None:
    """
    Set the JSON serialisation function to store JSON objects in the database.

    :param dumps: The dump function to use.
    :type dumps: `!Callable[[Any], str]`
    :param context: Where to use the *dumps* function. If not specified, use it
        globally.
    :type context: `~psycopg.Connection` or `~psycopg.Cursor`

    By default dumping JSON uses the builtin `json.dumps`. You can override
    it to use a different JSON library or to use customised arguments.

    If the `Json` wrapper specified a *dumps* function, use it in precedence
    of the one set by this function.
    """
    if context is None:
        # If changing load function globally, just change the default on the
        # global class
        _JsonDumper._dumps = dumps
    else:
        # If the scope is smaller than global, create subclassess and register
        # them in the appropriate scope.
        grid = [
            (Json, JsonDumper),
            (Json, JsonBinaryDumper),
            (Jsonb, JsonbDumper),
            (Jsonb, JsonbBinaryDumper),
        ]
        dumper: Type[_JsonDumper]
        for wrapper, base in grid:
            dumper = type(f"Custom{base.__name__}", (base,), {"_dumps": dumps})
            dumper.register(wrapper, context=context)


def set_json_loads(
    loads: JsonLoadsFunction, context: Optional[AdaptContext] = None
) -> None:
    """
    Set the JSON parsing function to fetch JSON objects from the database.

    :param loads: The load function to use.
    :type loads: `!Callable[[bytes], Any]`
    :param context: Where to use the *loads* function. If not specified, use it
        globally.
    :type context: `~psycopg.Connection` or `~psycopg.Cursor`

    By default loading JSON uses the builtin `json.loads`. You can override
    it to use a different JSON library or to use customised arguments.
    """
    if context is None:
        # If changing load function globally, just change the default on the
        # global class
        _JsonLoader._loads = loads
    else:
        # If the scope is smaller than global, create subclassess and register
        # them in the appropriate scope.
        grid = [
            ("json", JsonLoader),
            ("json", JsonBinaryLoader),
            ("jsonb", JsonbLoader),
            ("jsonb", JsonbBinaryLoader),
        ]
        loader: Type[_JsonLoader]
        for tname, base in grid:
            loader = type(f"Custom{base.__name__}", (base,), {"_loads": loads})
            loader.register(tname, context=context)


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
