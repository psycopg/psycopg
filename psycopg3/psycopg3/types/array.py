"""
Adapters for arrays
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from typing import Any, Iterator, List, Optional, Type

from .. import errors as e
from ..oids import builtins, TEXT_OID, TEXT_ARRAY_OID
from ..adapt import Format, Dumper, Loader, Transformer
from ..proto import AdaptContext


class BaseListDumper(Dumper):

    _oid = TEXT_ARRAY_OID

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._tx = Transformer(context)

    def _get_array_oid(self, base_oid: int) -> int:
        """
        Return the oid of the array from the oid of the base item.

        Fall back on text[].
        TODO: we shouldn't consider builtins only, but other adaptation
        contexts too
        """
        oid = 0
        if base_oid:
            info = builtins.get(base_oid)
            if info:
                oid = info.array_oid

        return oid or TEXT_ARRAY_OID


class ListDumper(BaseListDumper):
    # from https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
    #
    # The array output routine will put double quotes around element values if
    # they are empty strings, contain curly braces, delimiter characters,
    # double quotes, backslashes, or white space, or match the word NULL.
    # TODO: recognise only , as delimiter. Should be configured

    format = Format.TEXT

    _re_needs_quotes = re.compile(
        br"""(?xi)
          ^$              # the empty string
        | ["{},\\\s]      # or a char to escape
        | ^null$          # or the word NULL
        """
    )

    # Double quotes and backslashes embedded in element values will be
    # backslash-escaped.
    _re_esc = re.compile(br'(["\\])')

    def dump(self, obj: List[Any]) -> bytes:
        tokens: List[bytes] = []
        oid = 0

        def dump_list(obj: List[Any]) -> None:
            nonlocal oid

            if not obj:
                tokens.append(b"{}")
                return

            tokens.append(b"{")
            for item in obj:
                if isinstance(item, list):
                    dump_list(item)
                elif item is not None:
                    dumper = self._tx.get_dumper(item, Format.TEXT)
                    ad = dumper.dump(item)
                    if self._re_needs_quotes.search(ad):
                        ad = (
                            b'"' + self._re_esc.sub(br"\\\1", bytes(ad)) + b'"'
                        )
                    tokens.append(ad)
                    if not oid:
                        oid = dumper.oid
                else:
                    tokens.append(b"NULL")

                tokens.append(b",")

            tokens[-1] = b"}"

        dump_list(obj)

        if oid:
            self.oid = self._get_array_oid(oid)

        return b"".join(tokens)


class ListBinaryDumper(BaseListDumper):

    format = Format.BINARY

    def dump(self, obj: List[Any]) -> bytes:
        if not obj:
            return _struct_head.pack(0, 0, TEXT_OID)

        data: List[bytes] = [b"", b""]  # placeholders to avoid a resize
        dims: List[int] = []
        hasnull = 0
        oid = 0

        def calc_dims(L: List[Any]) -> None:
            if isinstance(L, self.cls):
                if not L:
                    raise e.DataError("lists cannot contain empty lists")
                dims.append(len(L))
                calc_dims(L[0])

        calc_dims(obj)

        def dump_list(L: List[Any], dim: int) -> None:
            nonlocal oid, hasnull
            if len(L) != dims[dim]:
                raise e.DataError("nested lists have inconsistent lengths")

            if dim == len(dims) - 1:
                for item in L:
                    if item is not None:
                        dumper = self._tx.get_dumper(item, Format.BINARY)
                        ad = dumper.dump(item)
                        data.append(_struct_len.pack(len(ad)))
                        data.append(ad)
                        if not oid:
                            oid = dumper.oid
                    else:
                        hasnull = 1
                        data.append(b"\xff\xff\xff\xff")
            else:
                for item in L:
                    if not isinstance(item, self.cls):
                        raise e.DataError(
                            "nested lists have inconsistent depths"
                        )
                    dump_list(item, dim + 1)  # type: ignore

        dump_list(obj, 0)

        if not oid:
            oid = TEXT_OID

        self.oid = self._get_array_oid(oid)

        data[0] = _struct_head.pack(len(dims), hasnull, oid)
        data[1] = b"".join(_struct_dim.pack(dim, 1) for dim in dims)
        return b"".join(data)


class BaseArrayLoader(Loader):
    base_oid: int

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)


class ArrayLoader(BaseArrayLoader):

    format = Format.TEXT

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

    def load(self, data: bytes) -> List[Any]:
        rv = None
        stack: List[Any] = []
        cast = self._tx.get_loader(self.base_oid, Format.TEXT).load

        for m in self._re_parse.finditer(data):
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
                        t = self._re_unescape.sub(br"\1", t[1:-1])
                    v = cast(t)

                stack[-1].append(v)

        assert rv is not None
        return rv

    _re_unescape = re.compile(br"\\(.)")


_struct_head = struct.Struct("!III")
_struct_dim = struct.Struct("!II")
_struct_len = struct.Struct("!i")


class ArrayBinaryLoader(BaseArrayLoader):

    format = Format.BINARY

    def load(self, data: bytes) -> List[Any]:
        ndims, hasnull, oid = _struct_head.unpack_from(data[:12])
        if not ndims:
            return []

        fcast = self._tx.get_loader(oid, Format.BINARY).load

        p = 12 + 8 * ndims
        dims = [
            _struct_dim.unpack_from(data, i)[0] for i in list(range(12, p, 8))
        ]

        def consume(p: int) -> Iterator[Any]:
            while 1:
                size = _struct_len.unpack_from(data, p)[0]
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


def register(
    array_oid: int,
    base_oid: int,
    context: Optional[AdaptContext] = None,
    name: Optional[str] = None,
) -> None:
    if not name:
        name = f"oid{base_oid}"

    for base in (ArrayLoader, ArrayBinaryLoader):
        lname = f"{name.title()}Array{'Binary' if format else ''}Loader"
        loader: Type[Loader] = type(lname, (base,), {"base_oid": base_oid})
        loader.register(array_oid, context=context)


def register_all_arrays() -> None:
    """
    Associate the array oid of all the types in Loader.globals.

    This function is designed to be called once at import time, after having
    registered all the base loaders.
    """
    for t in builtins:
        # TODO: handle different delimiters (box)
        if t.array_oid and getattr(t, "delimiter", None) == ",":
            register(t.array_oid, t.oid, name=t.name)
