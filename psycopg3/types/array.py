"""
Adapters for arrays
"""

# Copyright (C) 2020 The Psycopg Team

import re
from typing import Any, Callable, List, Optional, cast, TYPE_CHECKING

from .. import errors as e
from ..pq import Format
from ..adapt import Adapter, TypeCaster, Transformer, UnknownCaster
from ..adapt import AdaptContext, TypeCasterType, TypeCasterFunc

if TYPE_CHECKING:
    from ..connection import BaseConnection


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
    def __init__(self, cls: type, conn: "BaseConnection"):
        super().__init__(cls, conn)
        self.tx = Transformer(conn)

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

    def __init__(
        self, oid: int, conn: Optional["BaseConnection"],
    ):
        super().__init__(oid, conn)
        self.caster_func = TypeCasterFunc  # type: ignore

        if isinstance(self.base_caster, type):
            self.caster_func = self.base_caster(oid, conn).cast
        else:
            self.caster_func = cast(TypeCasterFunc, type(self).base_caster)

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
                    raise e.DataError(
                        f"malformed array, unexpected"
                        f" '{t.decode('utf8', 'replace')}'"
                    )
                if t == b"NULL":
                    v = None
                else:
                    if t.startswith(b'"'):
                        t = _re_unescape.sub(br"\1", t[1:-1])
                    v = self.caster_func(t)

                stack[-1].append(v)

        assert rv is not None
        return rv


class ArrayCaster(TypeCaster):
    @staticmethod
    def register(
        oid: int,  # array oid
        caster: TypeCasterType,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> TypeCasterType:
        t = type(
            caster.__name__ + "_array",  # type: ignore
            (ArrayCasterBase,),
            {"base_caster": caster},
        )
        return TypeCaster.register(oid, t, context=context, format=format)

    @staticmethod
    def text(oid: int) -> Callable[[Any], Any]:
        def text_(caster: TypeCasterType) -> TypeCasterType:
            ArrayCaster.register(oid, caster, format=Format.TEXT)
            return caster

        return text_


class UnknownArrayCaster(ArrayCasterBase):
    base_caster = UnknownCaster
