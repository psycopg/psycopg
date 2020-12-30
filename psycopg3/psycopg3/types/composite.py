"""
Support for composite types adaptation.
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from collections import namedtuple
from typing import Any, Callable, Iterator, List, NamedTuple, Optional
from typing import Sequence, Tuple, Type, Union, TYPE_CHECKING

from .. import sql
from .. import errors as e
from ..oids import builtins, TypeInfo, TEXT_OID
from ..adapt import Format, Dumper, Loader, Transformer
from ..proto import AdaptContext
from . import array

if TYPE_CHECKING:
    from ..connection import Connection, AsyncConnection


class CompositeInfo(TypeInfo):
    """Manage information about a composite type.

    The class allows to:

    - read information about a composite type using `fetch()` and `fetch_async()`
    - configure a composite type adaptation using `register()`
    """

    def __init__(
        self,
        name: str,
        oid: int,
        array_oid: int,
        fields: Sequence["CompositeInfo.FieldInfo"],
    ):
        super().__init__(name, oid, array_oid)
        self.fields = list(fields)

    class FieldInfo(NamedTuple):
        """Information about a single field in a composite type."""

        name: str
        type_oid: int

    @classmethod
    def fetch(
        cls, conn: "Connection", name: Union[str, sql.Identifier]
    ) -> Optional["CompositeInfo"]:
        if isinstance(name, sql.Composable):
            name = name.as_string(conn)
        cur = conn.cursor(format=Format.BINARY)
        cur.execute(cls._info_query, {"name": name})
        recs = cur.fetchall()
        return cls._from_records(recs)

    @classmethod
    async def fetch_async(
        cls, conn: "AsyncConnection", name: Union[str, sql.Identifier]
    ) -> Optional["CompositeInfo"]:
        if isinstance(name, sql.Composable):
            name = name.as_string(conn)
        cur = await conn.cursor(format=Format.BINARY)
        await cur.execute(cls._info_query, {"name": name})
        recs = await cur.fetchall()
        return cls._from_records(recs)

    def register(
        self,
        context: Optional[AdaptContext] = None,
        factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        if not factory:
            factory = namedtuple(  # type: ignore
                self.name, [f.name for f in self.fields]
            )

        loader: Type[Loader]

        # generate and register a customized text loader
        loader = type(
            f"{self.name.title()}Loader",
            (CompositeLoader,),
            {
                "factory": factory,
                "fields_types": [f.type_oid for f in self.fields],
            },
        )
        loader.register(self.oid, context=context, format=Format.TEXT)

        # generate and register a customized binary loader
        loader = type(
            f"{self.name.title()}BinaryLoader",
            (CompositeBinaryLoader,),
            {"factory": factory},
        )
        loader.register(self.oid, context=context, format=Format.BINARY)

        if self.array_oid:
            array.register(
                self.array_oid, self.oid, context=context, name=self.name
            )

    @classmethod
    def _from_records(cls, recs: List[Any]) -> Optional["CompositeInfo"]:
        if not recs:
            return None
        if len(recs) > 1:
            raise e.ProgrammingError(
                f"found {len(recs)} different types named {recs[0][0]}"
            )

        name, oid, array_oid, fnames, ftypes = recs[0]
        fields = [cls.FieldInfo(*p) for p in zip(fnames, ftypes)]
        return cls(name, oid, array_oid, fields)

    _info_query = """\
select
    t.typname as name, t.oid as oid, t.typarray as array_oid,
    coalesce(a.fnames, '{}') as fnames,
    coalesce(a.ftypes, '{}') as ftypes
from pg_type t
left join (
    select
        attrelid,
        array_agg(attname) as fnames,
        array_agg(atttypid) as ftypes
    from (
        select a.attrelid, a.attname, a.atttypid
        from pg_attribute a
        join pg_type t on t.typrelid = a.attrelid
        where t.oid = %(name)s::regtype
        and a.attnum > 0
        and not a.attisdropped
        order by a.attnum
    ) x
    group by attrelid
) a on a.attrelid = t.typrelid
where t.oid = %(name)s::regtype
"""


class SequenceDumper(Dumper):

    format = Format.TEXT

    def __init__(self, src: type, context: Optional[AdaptContext] = None):
        super().__init__(src, context)
        self._tx = Transformer(context)

    def _dump_sequence(
        self, obj: Sequence[Any], start: bytes, end: bytes, sep: bytes
    ) -> bytes:
        if not obj:
            return b"()"

        parts = [start]

        for item in obj:
            if item is None:
                parts.append(sep)
                continue

            dumper = self._tx.get_dumper(item, Format.TEXT)
            ad = dumper.dump(item)
            if not ad:
                ad = b'""'
            elif self._re_needs_quotes.search(ad):
                ad = b'"' + self._re_esc.sub(br"\1\1", ad) + b'"'

            parts.append(ad)
            parts.append(sep)

        parts[-1] = end

        return b"".join(parts)

    _re_needs_quotes = re.compile(br'[",\\\s()]')
    _re_esc = re.compile(br"([\\\"])")


@Dumper.text(tuple)
class TupleDumper(SequenceDumper):

    # Should be this, but it doesn't work
    # _oid = builtins["record"].oid

    def dump(self, obj: Tuple[Any, ...]) -> bytes:
        return self._dump_sequence(obj, b"(", b")", b",")


class BaseCompositeLoader(Loader):

    format = Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)

    def _parse_record(self, data: bytes) -> Iterator[Optional[bytes]]:
        """
        Split a non-empty representation of a composite type into components.

        Terminators shouldn't be used in *data* (so that both record and range
        representations can be parsed).
        """
        for m in self._re_tokenize.finditer(data):
            if m.group(1):
                yield None
            elif m.group(2) is not None:
                yield self._re_undouble.sub(br"\1", m.group(2))
            else:
                yield m.group(3)

        # If the final group ended in `,` there is a final NULL in the record
        # that the regexp couldn't parse.
        if m and m.group().endswith(b","):
            yield None

    _re_tokenize = re.compile(
        br"""(?x)
          (,)                       # an empty token, representing NULL
        | " ((?: [^"] | "")*) " ,?  # or a quoted string
        | ([^",)]+) ,?              # or an unquoted string
        """
    )

    _re_undouble = re.compile(br'(["\\])\1')


@Loader.text(builtins["record"].oid)
class RecordLoader(BaseCompositeLoader):
    def load(self, data: bytes) -> Tuple[Any, ...]:
        if data == b"()":
            return ()

        cast = self._tx.get_loader(TEXT_OID, format=Format.TEXT).load
        return tuple(
            cast(token) if token is not None else None
            for token in self._parse_record(data[1:-1])
        )


_struct_len = struct.Struct("!i")
_struct_oidlen = struct.Struct("!Ii")


@Loader.binary(builtins["record"].oid)
class RecordBinaryLoader(Loader):

    format = Format.BINARY
    _types_set = False

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)

    def load(self, data: bytes) -> Tuple[Any, ...]:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        return self._tx.load_sequence(
            tuple(
                data[offset : offset + length] if length != -1 else None
                for _, offset, length in self._walk_record(data)
            )
        )

    def _walk_record(self, data: bytes) -> Iterator[Tuple[int, int, int]]:
        """
        Yield a sequence of (oid, offset, length) for the content of the record
        """
        nfields = _struct_len.unpack_from(data, 0)[0]
        i = 4
        for _ in range(nfields):
            oid, length = _struct_oidlen.unpack_from(data, i)
            yield oid, i + 8, length
            i += (8 + length) if length > 0 else 8

    def _config_types(self, data: bytes) -> None:
        oids = [r[0] for r in self._walk_record(data)]
        self._tx.set_row_types(oids, [Format.BINARY] * len(oids))


class CompositeLoader(RecordLoader):

    format = Format.TEXT
    factory: Callable[..., Any]
    fields_types: List[int]
    _types_set = False

    def load(self, data: bytes) -> Any:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        if data == b"()":
            return type(self).factory()

        return type(self).factory(
            *self._tx.load_sequence(tuple(self._parse_record(data[1:-1])))
        )

    def _config_types(self, data: bytes) -> None:
        self._tx.set_row_types(
            self.fields_types, [Format.TEXT] * len(self.fields_types)
        )


class CompositeBinaryLoader(RecordBinaryLoader):

    format = Format.BINARY
    factory: Callable[..., Any]

    def load(self, data: bytes) -> Any:
        r = super().load(data)
        return type(self).factory(*r)
