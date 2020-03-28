"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs

from . import exceptions as exc
from .pq import Format
from .cursor import BaseCursor
from .types.oids import type_oid
from .connection import BaseConnection

INVALID_OID = 0

ascii_encode = codecs.lookup("ascii").encode
ascii_decode = codecs.lookup("ascii").decode
utf8_codec = codecs.lookup("utf-8")

global_adapters = {}
global_casters = {}


def register_adapter(cls, adapter, context=None, format=Format.TEXT):
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
            f"adapters should be callable or Adapter subclasses, got {adapter}"
        )

    where = context.adapters if context is not None else global_adapters
    where[cls, format] = adapter


def register_binary_adapter(cls, adapter, context=None):
    register_adapter(cls, adapter, context, format=Format.BINARY)


def register_caster(oid, caster, context=None, format=Format.TEXT):
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
            f"adapters should be callable or Typecaster subclasses, got {caster}"
        )

    where = context.adapters if context is not None else global_casters
    where[oid, format] = caster


def register_binary_caster(oid, caster, context=None):
    register_caster(oid, caster, context, format=Format.BINARY)


def adapter(oid):
    def adapter_(obj):
        register_adapter(oid, obj)
        return obj

    return adapter_


def binary_adapter(oid):
    def binary_adapter_(obj):
        register_binary_adapter(oid, obj)
        return obj

    return binary_adapter_


def caster(oid):
    def caster_(obj):
        register_caster(oid, obj)
        return obj

    return caster_


def binary_caster(oid):
    def binary_caster_(obj):
        register_binary_caster(oid, obj)
        return obj

    return binary_caster_


class Transformer:
    """
    An object that can adapt efficiently between Python and PostgreSQL.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    def __init__(self, context):
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
        self._adapt_funcs = {}

        # mapping oid, fmt -> cast function
        self._cast_funcs = {}

        # The result to return values from
        self._result = None

        # sequence of cast function from value to python
        # the length of the result columns
        self._row_casters = None

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, result):
        if self._result is result:
            return

        rc = self._row_casters = []
        for c in range(result.nfields):
            oid = result.ftype(c)
            fmt = result.fformat(c)
            func = self.get_cast_function(oid, fmt)
            rc.append(func)

    def adapt_sequence(self, objs, fmts):
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

    def adapt(self, obj, fmt):
        if obj is None:
            return None, type_oid["text"]

        cls = type(obj)
        func = self.get_adapt_function(cls, fmt)
        return func(obj)

    def get_adapt_function(self, cls, fmt):
        try:
            return self._adapt_funcs[cls, fmt]
        except KeyError:
            pass

        adapter = self.lookup_adapter(cls, fmt)
        if isinstance(adapter, type):
            adapter = adapter(cls, self.connection).adapt

        return adapter

    def lookup_adapter(self, cls, fmt):
        key = (cls, fmt)

        cur = self.cursor
        if cur is not None and key in cur.adapters:
            return cur.adapters[key]

        conn = self.connection
        if conn is not None and key in conn.adapters:
            return conn.adapters[key]

        if key in global_adapters:
            return global_adapters[key]

        raise exc.ProgrammingError(
            f"cannot adapt type {cls.__name__} to format {fmt}"
        )

    def cast_row(self, result, n):
        self.result = result

        for col, func in enumerate(self._row_casters):
            v = result.get_value(n, col)
            if v is not None:
                v = func(v)
            yield v

    def get_cast_function(self, oid, fmt):
        try:
            return self._cast_funcs[oid, fmt]
        except KeyError:
            pass

        caster = self.lookup_caster(oid, fmt)
        if isinstance(caster, type):
            caster = caster(oid, self.connection).cast

        return caster

    def lookup_caster(self, oid, fmt):
        key = (oid, fmt)

        cur = self.cursor
        if cur is not None and key in cur.casters:
            return cur.casters[key]

        conn = self.connection
        if conn is not None and key in conn.casters:
            return conn.casters[key]

        if key in global_casters:
            return global_casters[key]

        return global_casters[INVALID_OID, fmt]


class Adapter:
    def __init__(self, cls, conn):
        self.cls = cls
        self.conn = conn

    def adapt(self, obj):
        raise NotImplementedError()


class Typecaster:
    def __init__(self, oid, conn):
        self.oid = oid
        self.conn = conn

    def cast(self, data):
        raise NotImplementedError()


@adapter(str)
@binary_adapter(str)
class StringAdapter(Adapter):
    def __init__(self, cls, conn):
        super().__init__(cls, conn)
        self.encode = (conn.codec if conn is not None else utf8_codec).encode

    def adapt(self, obj):
        return self.encode(obj)[0]


@caster(type_oid["text"])
@binary_caster(type_oid["text"])
class StringCaster(Typecaster):
    def __init__(self, oid, conn):
        super().__init__(oid, conn)
        if conn is not None:
            if conn.pgenc != b"SQL_ASCII":
                self.decode = conn.codec.decode
            else:
                self.decode = None
        else:
            self.decode = utf8_codec.decode

    def cast(self, data):
        if self.decode is not None:
            return self.decode(data)[0]
        else:
            # return bytes for SQL_ASCII db
            return data


@adapter(int)
def adapt_int(obj):
    return ascii_encode(str(obj))[0], type_oid["numeric"]


@caster(type_oid["numeric"])
def cast_int(data):
    return int(ascii_decode(data)[0])


@caster(INVALID_OID)
class UnknownCaster(Typecaster):
    """
    Fallback object to convert unknown types to Python
    """

    def __init__(self, oid, conn):
        super().__init__(oid, conn)
        if conn is not None:
            self.decode = conn.codec.decode
        else:
            self.decode = utf8_codec.decode

    def cast(self, data):
        return self.decode(data)[0]


@binary_caster(INVALID_OID)
def cast_unknown(data):
    return data
