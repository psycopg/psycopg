"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional
from typing import Tuple, Type, Union

from . import errors as e
from . import pq
from .cursor import BaseCursor
from .types.oids import builtins, INVALID_OID
from .connection import BaseConnection
from .utils.typing import DecodeFunc

TEXT_OID = builtins["text"].oid

# Part of the module interface (just importing it makes mypy unhappy)
Format = pq.Format

# Type system

AdaptContext = Union[None, BaseConnection, BaseCursor, "Transformer"]

MaybeOid = Union[Optional[bytes], Tuple[Optional[bytes], int]]
DumpFunc = Callable[[Any], MaybeOid]
DumperType = Union[Type["Dumper"], DumpFunc]
DumpersMap = Dict[Tuple[type, Format], DumperType]

LoadFunc = Callable[[bytes], Any]
LoaderType = Union[Type["Loader"], LoadFunc]
LoadersMap = Dict[Tuple[int, Format], LoaderType]


class Dumper:
    globals: DumpersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, src: type, context: AdaptContext = None):
        self.src = src
        self.context = context
        self.connection = _connection_from_context(context)

    def dump(self, obj: Any) -> Union[bytes, Tuple[bytes, int]]:
        raise NotImplementedError()

    @classmethod
    def register(
        cls,
        src: type,
        dumper: DumperType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> DumperType:
        if not isinstance(src, type):
            raise TypeError(
                f"dumpers should be registered on classes, got {src} instead"
            )

        if not (
            callable(dumper)
            or (isinstance(dumper, type) and issubclass(dumper, Dumper))
        ):
            raise TypeError(
                f"dumpers should be callable or Dumper subclasses,"
                f" got {dumper} instead"
            )

        where = context.dumpers if context is not None else Dumper.globals
        where[src, format] = dumper
        return dumper

    @classmethod
    def register_binary(
        cls, src: type, dumper: DumperType, context: AdaptContext = None,
    ) -> DumperType:
        return cls.register(src, dumper, context, format=Format.BINARY)

    @classmethod
    def text(cls, src: type) -> Callable[[DumperType], DumperType]:
        def text_(dumper: DumperType) -> DumperType:
            cls.register(src, dumper)
            return dumper

        return text_

    @classmethod
    def binary(cls, src: type) -> Callable[[DumperType], DumperType]:
        def binary_(dumper: DumperType) -> DumperType:
            cls.register_binary(src, dumper)
            return dumper

        return binary_


class Loader:
    globals: LoadersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, oid: int, context: AdaptContext = None):
        self.oid = oid
        self.context = context
        self.connection = _connection_from_context(context)

    def load(self, data: bytes) -> Any:
        raise NotImplementedError()

    @classmethod
    def register(
        cls,
        oid: int,
        loader: LoaderType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> LoaderType:
        if not isinstance(oid, int):
            raise TypeError(
                f"typeloaders should be registered on oid, got {oid} instead"
            )

        if not (
            callable(loader)
            or (isinstance(loader, type) and issubclass(loader, Loader))
        ):
            raise TypeError(
                f"dumpers should be callable or Loader subclasses,"
                f" got {loader} instead"
            )

        where = context.loaders if context is not None else Loader.globals
        where[oid, format] = loader
        return loader

    @classmethod
    def register_binary(
        cls, oid: int, loader: LoaderType, context: AdaptContext = None,
    ) -> LoaderType:
        return cls.register(oid, loader, context, format=Format.BINARY)

    @classmethod
    def text(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def text_(loader: LoaderType) -> LoaderType:
            cls.register(oid, loader)
            return loader

        return text_

    @classmethod
    def binary(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def binary_(loader: LoaderType) -> LoaderType:
            cls.register_binary(oid, loader)
            return loader

        return binary_


class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    def __init__(self, context: AdaptContext = None):
        self.connection: Optional[BaseConnection]
        self.dumpers: DumpersMap
        self.loaders: LoadersMap
        self._dumpers_maps: List[DumpersMap] = []
        self._loaders_maps: List[LoadersMap] = []
        self._setup_context(context)

        # mapping class, fmt -> dump function
        self._dump_funcs: Dict[Tuple[type, Format], DumpFunc] = {}

        # mapping oid, fmt -> load function
        self._load_funcs: Dict[Tuple[int, Format], LoadFunc] = {}

        # sequence of load functions from value to python
        # the length of the result columns
        self._row_loaders: List[LoadFunc] = []

    def _setup_context(self, context: AdaptContext) -> None:
        if context is None:
            self.connection = None
            self.dumpers = {}
            self.loaders = {}
            self._dumpers_maps = [self.dumpers]
            self._loaders_maps = [self.loaders]

        elif isinstance(context, Transformer):
            # A transformer created from a transformers: usually it happens
            # for nested types: share the entire state of the parent
            self.connection = context.connection
            self.dumpers = context.dumpers
            self.loaders = context.loaders
            self._dumpers_maps.extend(context._dumpers_maps)
            self._loaders_maps.extend(context._loaders_maps)
            # the global maps are already in the lists
            return

        elif isinstance(context, BaseCursor):
            self.connection = context.connection
            self.dumpers = {}
            self._dumpers_maps.extend(
                (self.dumpers, context.dumpers, self.connection.dumpers)
            )
            self.loaders = {}
            self._loaders_maps.extend(
                (self.loaders, context.loaders, self.connection.loaders)
            )

        elif isinstance(context, BaseConnection):
            self.connection = context
            self.dumpers = {}
            self._dumpers_maps.extend((self.dumpers, context.dumpers))
            self.loaders = {}
            self._loaders_maps.extend((self.loaders, context.loaders))

        self._dumpers_maps.append(Dumper.globals)
        self._loaders_maps.append(Loader.globals)

    def dump_sequence(
        self, objs: Iterable[Any], formats: Iterable[Format]
    ) -> Tuple[List[Optional[bytes]], List[int]]:
        out = []
        types = []

        for var, fmt in zip(objs, formats):
            data = self.dump(var, fmt)
            if isinstance(data, tuple):
                oid = data[1]
                data = data[0]
            else:
                oid = TEXT_OID

            out.append(data)
            types.append(oid)

        return out, types

    def dump(self, obj: None, format: Format = Format.TEXT) -> MaybeOid:
        if obj is None:
            return None, TEXT_OID

        src = type(obj)
        func = self.get_dump_function(src, format)
        return func(obj)

    def get_dump_function(self, src: type, format: Format) -> DumpFunc:
        key = (src, format)
        try:
            return self._dump_funcs[key]
        except KeyError:
            pass

        dumper = self.lookup_dumper(src, format)
        func: DumpFunc
        if isinstance(dumper, type):
            func = dumper(src, self).dump
        else:
            func = dumper

        self._dump_funcs[key] = func
        return func

    def lookup_dumper(self, src: type, format: Format) -> DumperType:
        key = (src, format)
        for amap in self._dumpers_maps:
            if key in amap:
                return amap[key]

        raise e.ProgrammingError(
            f"cannot adapt type {src.__name__} to format {Format(format).name}"
        )

    def set_row_types(self, types: Iterable[Tuple[int, Format]]) -> None:
        rc = self._row_loaders = []
        for oid, fmt in types:
            rc.append(self.get_load_function(oid, fmt))

    def load_sequence(
        self, record: Iterable[Optional[bytes]]
    ) -> Generator[Any, None, None]:
        for val, loader in zip(record, self._row_loaders):
            if val is not None:
                yield loader(val)
            else:
                yield None

    def load(self, data: bytes, oid: int, format: Format = Format.TEXT) -> Any:
        if data is not None:
            f = self.get_load_function(oid, format)
            return f(data)
        else:
            return None

    def get_load_function(self, oid: int, format: Format) -> LoadFunc:
        key = (oid, format)
        try:
            return self._load_funcs[key]
        except KeyError:
            pass

        loader = self.lookup_loader(oid, format)
        func: LoadFunc
        if isinstance(loader, type):
            func = loader(oid, self).load
        else:
            func = loader

        self._load_funcs[key] = func
        return func

    def lookup_loader(self, oid: int, format: Format) -> LoaderType:
        key = (oid, format)

        for tcmap in self._loaders_maps:
            if key in tcmap:
                return tcmap[key]

        return Loader.globals[INVALID_OID, format]


@Loader.text(INVALID_OID)
class UnknownLoader(Loader):
    """
    Fallback object to convert unknown types to Python
    """

    def __init__(self, oid: int, context: AdaptContext):
        super().__init__(oid, context)
        self.decode: DecodeFunc
        if self.connection is not None:
            self.decode = self.connection.codec.decode
        else:
            self.decode = codecs.lookup("utf8").decode

    def load(self, data: bytes) -> str:
        return self.decode(data)[0]


@Loader.binary(INVALID_OID)
def load_unknown(data: bytes) -> bytes:
    return data


def _connection_from_context(
    context: AdaptContext,
) -> Optional[BaseConnection]:
    if context is None:
        return None
    elif isinstance(context, BaseConnection):
        return context
    elif isinstance(context, BaseCursor):
        return context.connection
    elif isinstance(context, Transformer):
        return context.connection
    else:
        raise TypeError(f"can't get a connection from {type(context)}")
