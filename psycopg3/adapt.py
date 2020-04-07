"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional
from typing import Tuple, Type, Union

from . import errors as e
from .pq import Format
from .cursor import BaseCursor
from .types.oids import builtins, INVALID_OID
from .connection import BaseConnection
from .utils.typing import DecodeFunc

TEXT_OID = builtins["text"].oid


# Type system

AdaptContext = Union[None, BaseConnection, BaseCursor]

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
    cursor: Optional[BaseCursor]

    def __init__(self, src: type, context: AdaptContext = None):
        self.src = src
        self.context = context
        self.connection, self.cursor = _solve_context(context)

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

        if context is not None and not isinstance(
            context, (BaseConnection, BaseCursor)
        ):
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context)}"
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
    cursor: Optional[BaseCursor]

    def __init__(self, oid: int, context: AdaptContext = None):
        self.oid = oid
        self.context = context
        self.connection, self.cursor = _solve_context(context)

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

        if context is not None and not isinstance(
            context, (BaseConnection, BaseCursor)
        ):
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context)}"
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

    connection: Optional[BaseConnection]
    cursor: Optional[BaseCursor]

    def __init__(self, context: AdaptContext = None):
        self.connection, self.cursor = _solve_context(context)

        # mapping class, fmt -> adaptation function
        self._adapt_funcs: Dict[Tuple[type, Format], AdapterFunc] = {}

        # mapping oid, fmt -> cast function
        self._cast_funcs: Dict[Tuple[int, Format], TypeCasterFunc] = {}

        # sequence of cast function from value to python
        # the length of the result columns
        self._row_casters: List[TypeCasterFunc] = []

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
            func = adapter(src, self.connection).adapt
        else:
            func = adapter

        self._adapt_funcs[key] = func
        return func

    def lookup_adapter(self, src: type, format: Format) -> AdapterType:
        key = (src, format)

        cur = self.cursor
        if cur is not None and key in cur.adapters:
            return cur.adapters[key]

        conn = self.connection
        if conn is not None and key in conn.adapters:
            return conn.adapters[key]

        if key in Adapter.globals:
            return Adapter.globals[key]

        raise e.ProgrammingError(
            f"cannot adapt type {src} to format {Format(format).name}"
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
            func = caster(oid, self.connection).cast
        else:
            func = caster

        self._cast_funcs[key] = func
        return func

    def lookup_caster(self, oid: int, format: Format) -> TypeCasterType:
        key = (oid, format)

        cur = self.cursor
        if cur is not None and key in cur.casters:
            return cur.casters[key]

        conn = self.connection
        if conn is not None and key in conn.casters:
            return conn.casters[key]

        if key in TypeCaster.globals:
            return TypeCaster.globals[key]

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


def _solve_context(
    context: AdaptContext,
) -> Tuple[Optional[BaseConnection], Optional[BaseCursor]]:
    if context is None:
        return None, None
    elif isinstance(context, BaseConnection):
        return context, None
    elif isinstance(context, BaseCursor):
        return context.conn, context
    else:
        raise TypeError(
            f"the context should be a connection or cursor,"
            f" got {type(context)}"
        )
