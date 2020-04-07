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
AdapterFunc = Callable[[Any], MaybeOid]
AdapterType = Union[Type["Adapter"], AdapterFunc]
AdaptersMap = Dict[Tuple[type, Format], AdapterType]

TypeCasterFunc = Callable[[bytes], Any]
TypeCasterType = Union[Type["TypeCaster"], TypeCasterFunc]
TypeCastersMap = Dict[Tuple[int, Format], TypeCasterType]


class Adapter:
    globals: AdaptersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, src: type, context: AdaptContext = None):
        self.src = src
        self.context = context
        self.connection = _connection_from_context(context)

    def adapt(self, obj: Any) -> Union[bytes, Tuple[bytes, int]]:
        raise NotImplementedError()

    @classmethod
    def register(
        cls,
        src: type,
        adapter: AdapterType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> AdapterType:
        if not isinstance(src, type):
            raise TypeError(
                f"adapters should be registered on classes, got {src} instead"
            )

        if not (
            callable(adapter)
            or (isinstance(adapter, type) and issubclass(adapter, Adapter))
        ):
            raise TypeError(
                f"adapters should be callable or Adapter subclasses,"
                f" got {adapter} instead"
            )

        where = context.adapters if context is not None else Adapter.globals
        where[src, format] = adapter
        return adapter

    @classmethod
    def register_binary(
        cls, src: type, adapter: AdapterType, context: AdaptContext = None,
    ) -> AdapterType:
        return cls.register(src, adapter, context, format=Format.BINARY)

    @classmethod
    def text(cls, src: type) -> Callable[[AdapterType], AdapterType]:
        def text_(adapter: AdapterType) -> AdapterType:
            cls.register(src, adapter)
            return adapter

        return text_

    @classmethod
    def binary(cls, src: type) -> Callable[[AdapterType], AdapterType]:
        def binary_(adapter: AdapterType) -> AdapterType:
            cls.register_binary(src, adapter)
            return adapter

        return binary_


class TypeCaster:
    globals: TypeCastersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, oid: int, context: AdaptContext = None):
        self.oid = oid
        self.context = context
        self.connection = _connection_from_context(context)

    def cast(self, data: bytes) -> Any:
        raise NotImplementedError()

    @classmethod
    def register(
        cls,
        oid: int,
        caster: TypeCasterType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> TypeCasterType:
        if not isinstance(oid, int):
            raise TypeError(
                f"typecasters should be registered on oid, got {oid} instead"
            )

        if not (
            callable(caster)
            or (isinstance(caster, type) and issubclass(caster, TypeCaster))
        ):
            raise TypeError(
                f"adapters should be callable or TypeCaster subclasses,"
                f" got {caster} instead"
            )

        where = context.casters if context is not None else TypeCaster.globals
        where[oid, format] = caster
        return caster

    @classmethod
    def register_binary(
        cls, oid: int, caster: TypeCasterType, context: AdaptContext = None,
    ) -> TypeCasterType:
        return cls.register(oid, caster, context, format=Format.BINARY)

    @classmethod
    def text(cls, oid: int) -> Callable[[TypeCasterType], TypeCasterType]:
        def text_(caster: TypeCasterType) -> TypeCasterType:
            cls.register(oid, caster)
            return caster

        return text_

    @classmethod
    def binary(cls, oid: int) -> Callable[[TypeCasterType], TypeCasterType]:
        def binary_(caster: TypeCasterType) -> TypeCasterType:
            cls.register_binary(oid, caster)
            return caster

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
        self.adapters: AdaptersMap
        self.casters: TypeCastersMap
        self._adapters_maps: List[AdaptersMap] = []
        self._casters_maps: List[TypeCastersMap] = []
        self._setup_context(context)

        # mapping class, fmt -> adaptation function
        self._adapt_funcs: Dict[Tuple[type, Format], AdapterFunc] = {}

        # mapping oid, fmt -> cast function
        self._cast_funcs: Dict[Tuple[int, Format], TypeCasterFunc] = {}

        # sequence of cast function from value to python
        # the length of the result columns
        self._row_casters: List[TypeCasterFunc] = []

    def _setup_context(self, context: AdaptContext) -> None:
        if context is None:
            self.connection = None
            self.adapters = {}
            self.casters = {}
            self._adapters_maps = [self.adapters]
            self._casters_maps = [self.casters]

        elif isinstance(context, Transformer):
            # A transformer created from a transformers: usually it happens
            # for nested types: share the entire state of the parent
            self.connection = context.connection
            self.adapters = context.adapters
            self.casters = context.casters
            self._adapters_maps.extend(context._adapters_maps)
            self._casters_maps.extend(context._casters_maps)
            # the global maps are already in the lists
            return

        elif isinstance(context, BaseCursor):
            self.connection = context.conn
            self.adapters = {}
            self._adapters_maps.extend(
                (self.adapters, context.adapters, self.connection.adapters)
            )
            self.casters = {}
            self._casters_maps.extend(
                (self.casters, context.casters, self.connection.casters)
            )

        elif isinstance(context, BaseConnection):
            self.connection = context
            self.adapters = {}
            self._adapters_maps.extend((self.adapters, context.adapters))
            self.casters = {}
            self._casters_maps.extend((self.casters, context.casters))

        self._adapters_maps.append(Adapter.globals)
        self._casters_maps.append(TypeCaster.globals)

    def adapt_sequence(
        self, objs: Iterable[Any], formats: Iterable[Format]
    ) -> Tuple[List[Optional[bytes]], List[int]]:
        out = []
        types = []

        for var, fmt in zip(objs, formats):
            data = self.adapt(var, fmt)
            if isinstance(data, tuple):
                oid = data[1]
                data = data[0]
            else:
                oid = TEXT_OID

            out.append(data)
            types.append(oid)

        return out, types

    def adapt(self, obj: None, format: Format = Format.TEXT) -> MaybeOid:
        if obj is None:
            return None, TEXT_OID

        src = type(obj)
        func = self.get_adapt_function(src, format)
        return func(obj)

    def get_adapt_function(self, src: type, format: Format) -> AdapterFunc:
        key = (src, format)
        try:
            return self._adapt_funcs[key]
        except KeyError:
            pass

        adapter = self.lookup_adapter(src, format)
        func: AdapterFunc
        if isinstance(adapter, type):
            func = adapter(src, self).adapt
        else:
            func = adapter

        self._adapt_funcs[key] = func
        return func

    def lookup_adapter(self, src: type, format: Format) -> AdapterType:
        key = (src, format)
        for amap in self._adapters_maps:
            if key in amap:
                return amap[key]

        raise e.ProgrammingError(
            f"cannot adapt type {src.__name__} to format {Format(format).name}"
        )

    def set_row_types(self, types: Iterable[Tuple[int, Format]]) -> None:
        rc = self._row_casters = []
        for oid, fmt in types:
            rc.append(self.get_cast_function(oid, fmt))

    def cast_sequence(
        self, record: Iterable[Optional[bytes]]
    ) -> Generator[Any, None, None]:
        for val, caster in zip(record, self._row_casters):
            if val is not None:
                yield caster(val)
            else:
                yield None

    def cast(self, data: bytes, oid: int, format: Format = Format.TEXT) -> Any:
        if data is not None:
            f = self.get_cast_function(oid, format)
            return f(data)
        else:
            return None

    def get_cast_function(self, oid: int, format: Format) -> TypeCasterFunc:
        key = (oid, format)
        try:
            return self._cast_funcs[key]
        except KeyError:
            pass

        caster = self.lookup_caster(oid, format)
        func: TypeCasterFunc
        if isinstance(caster, type):
            func = caster(oid, self).cast
        else:
            func = caster

        self._cast_funcs[key] = func
        return func

    def lookup_caster(self, oid: int, format: Format) -> TypeCasterType:
        key = (oid, format)

        for tcmap in self._casters_maps:
            if key in tcmap:
                return tcmap[key]

        return TypeCaster.globals[INVALID_OID, format]


@TypeCaster.text(INVALID_OID)
class UnknownCaster(TypeCaster):
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

    def cast(self, data: bytes) -> str:
        return self.decode(data)[0]


@TypeCaster.binary(INVALID_OID)
def cast_unknown(data: bytes) -> bytes:
    return data


def _connection_from_context(
    context: AdaptContext,
) -> Optional[BaseConnection]:
    if context is None:
        return None
    elif isinstance(context, BaseConnection):
        return context
    elif isinstance(context, BaseCursor):
        return context.conn
    elif isinstance(context, Transformer):
        return context.connection
    else:
        raise TypeError(f"can't get a connection from {type(context)}")
