"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from functools import partial

from . import exceptions as exc
from .pq import Format

INVALID_OID = 0
TEXT_OID = 25
NUMERIC_OID = 1700
FLOAT8_INT = 701

ascii_encode = codecs.lookup("ascii").encode
ascii_decode = codecs.lookup("ascii").decode
utf8_codec = codecs.lookup("utf-8")

global_adapters = {}
global_casters = {}


class ValuesTransformer:
    """
    An object that can adapt efficiently a number of value.

    The life cycle of the object is the query, so it is assumed that stuff like
    the server version or connection encoding will not change. It can have its
    state so adapting several values of the same type can use optimisations.
    """

    def __init__(self, context):
        from .connection import BaseConnection
        from .cursor import BaseCursor

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
            data, oid = self.adapt(var, fmt)
            out.append(data)
            types.append(oid)

        return out, types

    def adapt(self, obj, fmt):
        if obj is None:
            return None, TEXT_OID

        cls = type(obj)
        func = self.get_adapt_function(cls, fmt)
        return func(obj)

    def get_adapt_function(self, cls, fmt):
        try:
            return self._adapt_funcs[cls, fmt]
        except KeyError:
            pass

        xf = self.lookup_adapter(cls)
        if fmt == Format.TEXT:
            func = self._adapt_funcs[cls, fmt] = xf.get_text_adapter(
                cls, self.connection
            )
        else:
            assert fmt == Format.BINARY
            func = self._adapt_funcs[cls, fmt] = xf.get_binary_adapter(
                cls, self.connection
            )

        return func

    def lookup_adapter(self, cls):
        cur = self.cursor
        if cur is not None and cls in cur.adapters:
            return cur.adapters[cls]

        conn = self.connection
        if conn is not None and cls in conn.adapters:
            return conn.adapters[cls]

        if cls in global_adapters:
            return global_adapters[cls]

        raise exc.ProgrammingError(f"cannot adapt type {cls.__name__}")

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

        xf = self.lookup_caster(oid)
        if fmt == Format.TEXT:
            func = self._cast_funcs[oid, fmt] = xf.get_text_caster(
                oid, self.connection
            )
        else:
            assert fmt == Format.BINARY
            func = self._cast_funcs[oid, fmt] = xf.get_binary_caster(
                oid, self.connection
            )

        return func

    def lookup_caster(self, oid):
        cur = self.cursor
        if cur is not None and oid in cur.casters:
            return cur.casters[oid]

        conn = self.connection
        if conn is not None and oid in conn.casters:
            return conn.casters[oid]

        if oid in global_casters:
            return global_casters[oid]
        else:
            return UnknownCaster()


class Adapter:
    def get_text_adapter(self, cls, conn):
        raise exc.NotSupportedError(
            f"the type {cls.__name__} doesn't support text adaptation"
        )

    def get_binary_adapter(self, cls, conn):
        raise exc.NotSupportedError(
            f"the type {cls.__name__} doesn't support binary adaptation"
        )


class Typecaster:
    def get_text_caster(self, oid, conn):
        raise exc.NotSupportedError(
            f"the PostgreSQL type {oid} doesn't support cast from text"
        )

    def get_binary_caster(self, oid, conn):
        raise exc.NotSupportedError(
            f"the PostgreSQL type {oid} doesn't support cast from binary"
        )

    @staticmethod
    def cast_to_bytes(value):
        return value

    @staticmethod
    def cast_to_str(codec, value):
        return codec.decode(value)[0]


class StringAdapter(Adapter):
    def get_text_adapter(self, cls, conn):
        codec = conn.codec if conn is not None else utf8_codec
        return partial(self.adapt_str, codec)

    # format is the same in binary and text
    get_binary_adapter = get_text_adapter

    @staticmethod
    def adapt_str(codec, value):
        return codec.encode(value)[0], TEXT_OID


class StringCaster(Typecaster):
    def get_text_caster(self, oid, conn):
        if conn is None or conn.pgenc == b"SQL_ASCII":
            # we don't have enough info to decode bytes
            return self.unparsed_bytes

        codec = conn.codec
        return partial(self.cast_to_str, codec)

    # format is the same in binary and text
    get_binary_caster = get_text_caster


global_adapters[str] = StringAdapter()
global_casters[TEXT_OID] = StringCaster()


class IntAdapter(Adapter):
    def get_text_adapter(self, cls, conn):
        return self.adapt_int

    @staticmethod
    def adapt_int(value):
        return ascii_encode(str(value))[0], NUMERIC_OID


class IntCaster(Typecaster):
    def get_text_caster(self, oid, conn):
        return self.cast_int

    @staticmethod
    def cast_int(value):
        return int(ascii_decode(value)[0])


global_adapters[int] = IntAdapter()
global_casters[NUMERIC_OID] = IntCaster()


class UnknownCaster(Typecaster):
    """
    Fallback object to convert unknown types to Python
    """

    def get_text_caster(self, oid, conn):
        if conn is None:
            # we don't have enough info to decode bytes
            return self.cast_to_bytes

        codec = conn.codec
        return partial(self.cast_to_str, codec)

    def get_binary_caster(self, oid, conn):
        return self.cast_to_bytes

    @staticmethod
    def cast_to_str(codec, value):
        return codec.decode(value)[0]
