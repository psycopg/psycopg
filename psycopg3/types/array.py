"""
Adapters for arrays
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from typing import Any, Generator, List, Optional

from .. import errors as e
from ..pq import Format
from ..adapt import Adapter, TypeCaster, Transformer, UnknownCaster
from ..adapt import AdaptContext, TypeCasterType, TypeCasterFunc


# from https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
#
# The array output routine will put double quotes around element values if they
# are empty strings, contain curly braces, delimiter characters, double quotes,
# backslashes, or white space, or match the word NULL.
# TODO: recognise only , as delimiter. Should be configured
_re_needs_quote = re.compile(
    br"""(?xi)
      ^$              # the empty string
    | ["{},\\\s]      # or a char to escape
    | ^null$          # or the word NULL
    """
)

# Double quotes and backslashes embedded in element values will be
# backslash-escaped.
_re_escape = re.compile(br'(["\\])')
_re_unescape = re.compile(br"\\(.)")


# Tokenize an array representation into item and brackets
# TODO: currently recognise only , as delimiter. Should be configured
_re_parse = re.compile(
    br"""(?xi)
    (     [{}]                        # open or closed bracket
        | " (?: [^"\\] | \\. )* "     # or a quoted string
        | [^"{},\\]+                  # or an unquoted non-empty string
    ) ,?
    """
)


def escape_item(item: Optional[bytes]) -> bytes:
    if item is None:
        return b"NULL"
    if _re_needs_quote.search(item) is None:
        return item
    else:
        return b'"' + _re_escape.sub(br"\\\1", item) + b'"'


@Adapter.text(list)
class ListAdapter(Adapter):
    def __init__(self, src: type, context: AdaptContext = None):
        super().__init__(src, context)
        self.tx = Transformer(context)

    def adapt(self, obj: List[Any]) -> bytes:
        tokens: List[bytes] = []
        self.adapt_list(obj, tokens)
        return b"".join(tokens)

    def adapt_list(self, obj: List[Any], tokens: List[bytes]) -> None:
        if not obj:
            tokens.append(b"{}")
            return

        tokens.append(b"{")
        for item in obj:
            if isinstance(item, list):
                self.adapt_list(item, tokens)
            elif item is None:
                tokens.append(b"NULL")
            else:
                ad = self.tx.adapt(item)
                if isinstance(ad, tuple):
                    ad = ad[0]
                tokens.append(escape_item(ad))

            tokens.append(b",")

        tokens[-1] = b"}"


class ArrayCasterBase(TypeCaster):
    base_caster: TypeCasterType

    def __init__(self, oid: int, context: AdaptContext = None):
        super().__init__(oid, context)

        self.caster_func: TypeCasterFunc
        if isinstance(self.base_caster, type):
            self.caster_func = self.base_caster(oid, context).cast
        else:
            self.caster_func = type(self).base_caster


class ArrayCasterText(ArrayCasterBase):
    def cast(self, data: bytes) -> List[Any]:
        rv = None
        stack: List[Any] = []
        for m in _re_parse.finditer(data):
            t = m.group(1)
            if t == b"{":
                a: List[Any] = []
                if rv is None:
                    rv = a
                if stack:
                    stack[-1].append(a)
                stack.append(a)

            elif t == b"}":
                if not stack:
                    raise e.DataError("malformed array, unexpected '}'")
                rv = stack.pop()

            else:
                if not stack:
                    wat = (
                        t[:10].decode("utf8", "replace") + "..."
                        if len(t) > 10
                        else ""
                    )
                    raise e.DataError(f"malformed array, unexpected '{wat}'")
                if t == b"NULL":
                    v = None
                else:
                    if t.startswith(b'"'):
                        t = _re_unescape.sub(br"\1", t[1:-1])
                    v = self.caster_func(t)

                stack[-1].append(v)

        assert rv is not None
        return rv


_unpack_head = struct.Struct("!III").unpack_from
_unpack_dim = struct.Struct("!II").unpack_from
_unpack_len = struct.Struct("!i").unpack_from


class ArrayCasterBinary(ArrayCasterBase):
    def __init__(self, oid: int, context: AdaptContext = None):
        super().__init__(oid, context)
        self.tx = Transformer(context)

    def cast(self, data: bytes) -> List[Any]:
        ndims, hasnull, oid = _unpack_head(data[:12])
        if not ndims:
            return []

        fcast = self.tx.get_cast_function(oid, Format.BINARY)

        p = 12 + 8 * ndims
        dims = [_unpack_dim(data, i)[0] for i in list(range(12, p, 8))]

        def consume(p: int) -> Generator[Any, None, None]:
            while 1:
                size = _unpack_len(data, p)[0]
                p += 4
                if size != -1:
                    yield fcast(data[p : p + size])
                    p += size
                else:
                    yield None

        items = consume(p)

        def agg(dims: List[int]) -> List[Any]:
            if not dims:
                return next(items)
            else:
                dim, dims = dims[0], dims[1:]
                return [agg(dims) for _ in range(dim)]

        return agg(dims)


class ArrayCaster(TypeCaster):
    @staticmethod
    def register(
        oid: int,  # array oid
        caster: TypeCasterType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> TypeCasterType:
        base = ArrayCasterText if format == Format.TEXT else ArrayCasterBinary
        name = f"{caster.__name__}_{format.name.lower()}_array"
        t = type(name, (base,), {"base_caster": caster})
        return TypeCaster.register(oid, t, context=context, format=format)


class UnknownArrayCaster(ArrayCasterText):
    base_caster = UnknownCaster
