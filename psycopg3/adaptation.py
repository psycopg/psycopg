"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from functools import partial

from . import exceptions as exc
from .pq import Format, PGresult
from .cursor import BaseCursor
from .types.oids import type_oid, INVALID_OID
from .connection import BaseConnection
from .utils.typing import DecodeFunc


# Type system

AdaptContext = Union[BaseConnection, BaseCursor]

MaybeOid = Union[Optional[bytes], Tuple[Optional[bytes], int]]
AdapterFunc = Callable[[Any], MaybeOid]
AdapterType = Union["Adapter", AdapterFunc]
AdaptersMap = Dict[Tuple[type, Format], AdapterType]

TypecasterFunc = Callable[[bytes], Any]
TypecasterType = Union["Typecaster", TypecasterFunc]
TypecastersMap = Dict[Tuple[int, Format], TypecasterType]


class Adapter:
    globals: AdaptersMap = {}

    def __init__(self, cls: type, conn: BaseConnection):
        self.cls = cls
        self.conn = conn

    def adapt(self, obj: Any) -> Union[bytes, Tuple[bytes, int]]:
        raise NotImplementedError()

    @staticmethod
    def register(
        cls: type,
        adapter: Optional[AdapterType] = None,
        context: Optional[AdaptContext] = None,
        format: Format = Format.TEXT,
    ) -> AdapterType:
        if adapter is None:
            # used as decorator
            return partial(Adapter.register, cls, format=format)

        if not isinstance(cls, type):
            raise TypeError(
                f"adapters should be registered on classes, got {cls} instead"
            )

        if context is not None and not isinstance(
            context, (BaseConnection, BaseCursor)
        ):
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context).__name__}"
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
        where[cls, format] = adapter
        return adapter

    @staticmethod
    def register_binary(
        cls: type,
        adapter: Optional[AdapterType] = None,
        context: Optional[AdaptContext] = None,
    ) -> AdapterType:
        return Adapter.register(cls, adapter, context, format=Format.BINARY)


class Typecaster:
    globals: TypecastersMap = {}

    def __init__(self, oid: int, conn: Optional[BaseConnection]):
        self.oid = oid
        self.conn = conn

    def cast(self, data: bytes) -> Any:
        raise NotImplementedError()

    @staticmethod
    def register(
        oid: int,
        caster: Optional[TypecasterType] = None,
        context: Optional[AdaptContext] = None,
        format: Format = Format.TEXT,
    ) -> TypecasterType:
        if caster is None:
            # used as decorator
            return partial(Typecaster.register, oid, format=format)

        if not isinstance(oid, int):
            raise TypeError(
                f"typecasters should be registered on oid, got {oid} instead"
            )

        if context is not None and not isinstance(
            context, (BaseConnection, BaseCursor)
        ):
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context).__name__}"
            )

        if not (
            callable(caster)
            or (isinstance(caster, type) and issubclass(caster, Typecaster))
        ):
            raise TypeError(
                f"adapters should be callable or Typecaster subclasses,"
                f" got {caster} instead"
            )

        where = context.casters if context is not None else Typecaster.globals
        where[oid, format] = caster
        return caster

    @staticmethod
    def register_binary(
        oid: int,
        caster: Optional[TypecasterType] = None,
        context: Optional[AdaptContext] = None,
    ) -> TypecasterType:
        return Typecaster.register(oid, caster, context, format=Format.BINARY)


class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    connection: Optional[BaseConnection]
    cursor: Optional[BaseCursor]

    def __init__(self, context: Optional[AdaptContext]):
        if context is None:
            self.connection = None
            self.cursor = None
        elif isinstance(context, BaseConnection):
            self.connection = context
            self.cursor = None
        elif isinstance(context, BaseCursor):
            self.connection = context.conn
            self.cursor = context
        else:
            raise TypeError(
                f"the context should be a connection or cursor,"
                f" got {type(context).__name__}"
            )

        # mapping class, fmt -> adaptation function
        self._adapt_funcs: Dict[Tuple[type, Format], AdapterFunc] = {}

        # mapping oid, fmt -> cast function
        self._cast_funcs: Dict[Tuple[int, Format], TypecasterFunc] = {}

        # The result to return values from
        self._result: Optional[PGresult] = None

        # sequence of cast function from value to python
        # the length of the result columns
        self._row_casters: List[TypecasterFunc] = []

    @property
    def result(self) -> Optional[PGresult]:
        return self._result

    @result.setter
    def result(self, result: PGresult) -> None:
        if self._result is result:
            return

        rc = self._row_casters = []
        for c in range(result.nfields):
            oid = result.ftype(c)
            fmt = result.fformat(c)
            func = self.get_cast_function(oid, fmt)
            rc.append(func)

    def adapt_sequence(
        self, objs: Sequence[Any], fmts: Sequence[Format]
    ) -> Tuple[List[Optional[bytes]], List[int]]:
        out = []
        types = []

        for var, fmt in zip(objs, fmts):
            data = self.adapt(var, fmt)
            if isinstance(data, tuple):
                oid = data[1]
                data = data[0]
            else:
                oid = type_oid["text"]

            out.append(data)
            types.append(oid)

        return out, types

    def adapt(self, obj: None, fmt: Format) -> MaybeOid:
        if obj is None:
            return None, type_oid["text"]

        cls = type(obj)
        func = self.get_adapt_function(cls, fmt)
        return func(obj)

    def get_adapt_function(self, cls: type, fmt: Format) -> AdapterFunc:
        try:
            return self._adapt_funcs[cls, fmt]
        except KeyError:
            pass

        adapter = self.lookup_adapter(cls, fmt)
        if isinstance(adapter, type):
            return adapter(cls, self.connection).adapt
        else:
            return cast(AdapterFunc, adapter)

    def lookup_adapter(self, cls: type, fmt: Format) -> AdapterType:
        key = (cls, fmt)

        cur = self.cursor
        if cur is not None and key in cur.adapters:
            return cur.adapters[key]

        conn = self.connection
        if conn is not None and key in conn.adapters:
            return conn.adapters[key]

        if key in Adapter.globals:
            return Adapter.globals[key]

        raise exc.ProgrammingError(
            f"cannot adapt type {cls.__name__} to format {Format(fmt).name}"
        )

    def cast_row(self, result: PGresult, n: int) -> Generator[Any, None, None]:
        self.result = result

        for col, func in enumerate(self._row_casters):
            v = result.get_value(n, col)
            if v is not None:
                v = func(v)
            yield v

    def get_cast_function(self, oid: int, fmt: Format) -> TypecasterFunc:
        try:
            return self._cast_funcs[oid, fmt]
        except KeyError:
            pass

        caster = self.lookup_caster(oid, fmt)
        if isinstance(caster, type):
            return caster(oid, self.connection).cast
        else:
            return cast(TypecasterFunc, caster)

    def lookup_caster(self, oid: int, fmt: Format) -> TypecasterType:
        key = (oid, fmt)

        cur = self.cursor
        if cur is not None and key in cur.casters:
            return cur.casters[key]

        conn = self.connection
        if conn is not None and key in conn.casters:
            return conn.casters[key]

        if key in Typecaster.globals:
            return Typecaster.globals[key]

        return Typecaster.globals[INVALID_OID, fmt]


@Typecaster.register(INVALID_OID)
class UnknownCaster(Typecaster):
    """
    Fallback object to convert unknown types to Python
    """

    def __init__(self, oid: int, conn: Optional[BaseConnection]):
        super().__init__(oid, conn)
        self.decode: DecodeFunc
        if conn is not None:
            self.decode = conn.codec.decode
        else:
            self.decode = codecs.lookup("utf8").decode

    def cast(self, data: bytes) -> str:
        return self.decode(data)[0]


@Typecaster.register_binary(INVALID_OID)
def cast_unknown(data: bytes) -> bytes:
    return data
