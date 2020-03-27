"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs

from . import exceptions as exc
from .pq import Format

INVALID_OID = 0
TEXT_OID = 25
NUMERIC_OID = 1700
FLOAT8_INT = 701

ascii_encode = codecs.lookup("ascii").encode
utf8_codec = codecs.lookup("utf-8")


class ValuesAdapter:
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
                f" got {type(context).__name__}")

        # mapping class -> adaptation function
        self._adapt_funcs = {}

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
        try:
            func = self._adapt_funcs[cls, fmt]
        except KeyError:
            pass
        else:
            return func(obj)

        adapter = self.lookup_adapter(cls)
        if fmt == Format.TEXT:
            func = self._adapt_funcs[cls, fmt] = adapter.get_text_adapter(
                cls, self.connection
            )
        else:
            assert fmt == Format.BINARY
            func = self._adapt_funcs[cls, fmt] = adapter.get_binary_adapter(
                cls, self.connection
            )

        return func(obj)

    def lookup_adapter(self, cls):
        cur = self.cursor
        if (
            cur is not None
            and cls in cur.adapters
        ):
            return cur.adapters[cls]

        conn = self.connection
        if (
            conn is not None
            and cls in conn.adapters
        ):
            return conn.adapters[cls]

        if cls in global_adapters:
            return global_adapters[cls]

        raise exc.ProgrammingError(f"cannot adapt type {cls.__name__}")


global_adapters = {}


class Adapter:
    def get_text_adapter(self, cls, conn):
        raise exc.NotSupportedError(
            f"the type {cls.__name__} doesn't support text adaptation"
        )

    def get_binary_adapter(self, cls, conn):
        raise exc.NotSupportedError(
            f"the type {cls.__name__} doesn't support binary adaptation"
        )


class StringAdapter(Adapter):
    def get_text_adapter(self, cls, conn):
        codec = conn.codec if conn is not None else utf8_codec

        def adapt_text(value):
            return codec.encode(value)[0], TEXT_OID

        return adapt_text

    # format is the same in binary and text
    get_binary_adapter = get_text_adapter


global_adapters[str] = StringAdapter()


class IntAdapter(Adapter):
    def get_text_adapter(self, cls, conn):
        return self.adapt_int

    def adapt_int(self, value):
        return ascii_encode(str(value))[0], NUMERIC_OID


global_adapters[int] = IntAdapter()
