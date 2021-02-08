"""
Adapters for arrays
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import struct
from typing import Any, Iterator, List, Optional, Set, Tuple, Type

from .. import pq
from .. import errors as e
from ..oids import postgres_types, TEXT_OID, TEXT_ARRAY_OID, INVALID_OID
from ..adapt import Buffer, Dumper, Loader, Transformer
from ..adapt import Format as Pg3Format
from ..proto import AdaptContext
from .._typeinfo import TypeInfo


class BaseListDumper(Dumper):
    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        super().__init__(cls, context)
        self._tx = Transformer(context)
        self.sub_dumper: Optional[Dumper] = None
        self._types = context.adapters.types if context else postgres_types

    def get_key(self, obj: List[Any], format: Pg3Format) -> Tuple[type, ...]:
        item = self._find_list_element(obj)
        if item is not None:
            sd = self._tx.get_dumper(item, format)
            return (self.cls, sd.cls)
        else:
            return (self.cls,)

    def upgrade(self, obj: List[Any], format: Pg3Format) -> "BaseListDumper":
        item = self._find_list_element(obj)
        if item is None:
            # Empty lists can only be dumped as text if the type is unknown.
            return ListDumper(self.cls, self._tx)

        sd = self._tx.get_dumper(item, format)
        dcls = ListDumper if sd.format == pq.Format.TEXT else ListBinaryDumper
        dumper = dcls(self.cls, self._tx)
        dumper.sub_dumper = sd

        # We consider an array of unknowns as unknown, so we can dump empty
        # lists or lists containing only None elements.
        if sd.oid != INVALID_OID:
            dumper.oid = self._get_array_oid(sd.oid)
        else:
            dumper.oid = INVALID_OID

        return dumper

    def _find_list_element(self, L: List[Any]) -> Any:
        """
        Find the first non-null element of an eventually nested list
        """
        it = self._flatiter(L, set())
        try:
            item = next(it)
        except StopIteration:
            return None

        if not isinstance(item, int):
            return item

        imax = max((i if i >= 0 else -i - 1 for i in it), default=0)
        imax = max(item if item >= 0 else -item, imax)
        return imax

    def _flatiter(self, L: List[Any], seen: Set[int]) -> Any:
        if id(L) in seen:
            raise e.DataError("cannot dump a recursive list")

        seen.add(id(L))

        for item in L:
            if type(item) is list:
                for subit in self._flatiter(item, seen):
                    yield subit
            elif item is not None:
                yield item

        return None

    def _get_array_oid(self, base_oid: int) -> int:
        """
        Return the oid of the array from the oid of the base item.

        Fall back on text[].
        """
        oid = 0
        if base_oid:
            info = self._types.get(base_oid)
            if info:
                oid = info.array_oid

        return oid or TEXT_ARRAY_OID


class ListDumper(BaseListDumper):

    format = pq.Format.TEXT

    # from https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
    #
    # The array output routine will put double quotes around element values if
    # they are empty strings, contain curly braces, delimiter characters,
    # double quotes, backslashes, or white space, or match the word NULL.
    # TODO: recognise only , as delimiter. Should be configured
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

        def dump_list(obj: List[Any]) -> None:
            if not obj:
                tokens.append(b"{}")
                return

            tokens.append(b"{")
            for item in obj:
                if isinstance(item, list):
                    dump_list(item)
                elif item is not None:
                    # If we get here, the sub_dumper must have been set
                    ad = self.sub_dumper.dump(item)  # type: ignore[union-attr]
                    if self._re_needs_quotes.search(ad):
                        ad = (
                            b'"' + self._re_esc.sub(br"\\\1", bytes(ad)) + b'"'
                        )
                    tokens.append(ad)
                else:
                    tokens.append(b"NULL")

                tokens.append(b",")

            tokens[-1] = b"}"

        dump_list(obj)

        return b"".join(tokens)


class ListBinaryDumper(BaseListDumper):

    format = pq.Format.BINARY

    def dump(self, obj: List[Any]) -> bytes:
        # Postgres won't take unknown for element oid: fall back on text
        sub_oid = self.sub_dumper and self.sub_dumper.oid or TEXT_OID

        if not obj:
            return _struct_head.pack(0, 0, sub_oid)

        data: List[bytes] = [b"", b""]  # placeholders to avoid a resize
        dims: List[int] = []
        hasnull = 0

        def calc_dims(L: List[Any]) -> None:
            if isinstance(L, self.cls):
                if not L:
                    raise e.DataError("lists cannot contain empty lists")
                dims.append(len(L))
                calc_dims(L[0])

        calc_dims(obj)

        def dump_list(L: List[Any], dim: int) -> None:
            nonlocal hasnull
            if len(L) != dims[dim]:
                raise e.DataError("nested lists have inconsistent lengths")

            if dim == len(dims) - 1:
                for item in L:
                    if item is not None:
                        # If we get here, the sub_dumper must have been set
                        ad = self.sub_dumper.dump(item)  # type: ignore[union-attr]
                        data.append(_struct_len.pack(len(ad)))
                        data.append(ad)
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

        data[0] = _struct_head.pack(len(dims), hasnull, sub_oid)
        data[1] = b"".join(_struct_dim.pack(dim, 1) for dim in dims)
        return b"".join(data)


class BaseArrayLoader(Loader):
    base_oid: int

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)


class ArrayLoader(BaseArrayLoader):

    format = pq.Format.TEXT

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

    def load(self, data: Buffer) -> List[Any]:
        rv = None
        stack: List[Any] = []
        cast = self._tx.get_loader(self.base_oid, self.format).load

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


_struct_head = struct.Struct("!III")  # ndims, hasnull, elem oid
_struct_dim = struct.Struct("!II")  # dim, lower bound
_struct_len = struct.Struct("!i")


class ArrayBinaryLoader(BaseArrayLoader):

    format = pq.Format.BINARY

    def load(self, data: Buffer) -> List[Any]:
        ndims, hasnull, oid = _struct_head.unpack_from(data[:12])
        if not ndims:
            return []

        fcast = self._tx.get_loader(oid, self.format).load

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


def register_adapters(
    info: TypeInfo, context: Optional["AdaptContext"]
) -> None:
    for base in (ArrayLoader, ArrayBinaryLoader):
        lname = f"{info.name.title()}{base.__name__}"
        loader: Type[BaseArrayLoader] = type(
            lname, (base,), {"base_oid": info.oid}
        )
        loader.register(info.array_oid, context=context)


def register_all_arrays(ctx: AdaptContext) -> None:
    """
    Associate the array oid of all the types in Loader.globals.

    This function is designed to be called once at import time, after having
    registered all the base loaders.
    """
    for t in ctx.adapters.types:
        # TODO: handle different delimiters (box)
        if t.array_oid and getattr(t, "delimiter", None) == ",":
            t.register(ctx)
