"""
Support for composite types adaptation.
"""

# Copyright (C) 2020 The Psycopg Team

import re
import struct
from collections import namedtuple
from typing import Any, Callable, Iterator, List, Sequence, Tuple, Type, Union
from typing import Optional, TYPE_CHECKING

from .. import sql
from .. import errors as e
from ..oids import builtins, TypeInfo
from ..adapt import Format, Dumper, Loader, Transformer
from ..proto import AdaptContext
from . import array

if TYPE_CHECKING:
    from ..connection import Connection, AsyncConnection


TEXT_OID = builtins["text"].oid


class FieldInfo:
    def __init__(self, name: str, type_oid: int):
        self.name = name
        self.type_oid = type_oid


class CompositeTypeInfo(TypeInfo):
    def __init__(
        self, name: str, oid: int, array_oid: int, fields: Sequence[FieldInfo]
    ):
        super().__init__(name, oid, array_oid)
        self.fields = list(fields)

    @classmethod
    def _from_records(cls, recs: List[Any]) -> Optional["CompositeTypeInfo"]:
        if not recs:
            return None
        if len(recs) > 1:
            raise e.ProgrammingError(
                f"found {len(recs)} different types named {recs[0][0]}"
            )

        name, oid, array_oid, fnames, ftypes = recs[0]
        fields = [FieldInfo(*p) for p in zip(fnames, ftypes)]
        return CompositeTypeInfo(name, oid, array_oid, fields)


def fetch_info(
    conn: "Connection", name: Union[str, sql.Identifier]
) -> Optional[CompositeTypeInfo]:
    if isinstance(name, sql.Composable):
        name = name.as_string(conn)
    cur = conn.cursor(format=Format.BINARY)
    cur.execute(_type_info_query, {"name": name})
    recs = cur.fetchall()
    return CompositeTypeInfo._from_records(recs)


async def fetch_info_async(
    conn: "AsyncConnection", name: Union[str, sql.Identifier]
) -> Optional[CompositeTypeInfo]:
    if isinstance(name, sql.Composable):
        name = name.as_string(conn)
    cur = await conn.cursor(format=Format.BINARY)
    await cur.execute(_type_info_query, {"name": name})
    recs = await cur.fetchall()
    return CompositeTypeInfo._from_records(recs)


def register(
    info: CompositeTypeInfo,
    context: AdaptContext = None,
    factory: Optional[Callable[..., Any]] = None,
) -> None:
    if not factory:
        factory = namedtuple(  # type: ignore
            info.name, [f.name for f in info.fields]
        )

    loader: Type[Loader]

    # generate and register a customized text loader
    loader = type(
        f"{info.name.title()}Loader",
        (CompositeLoader,),
        {
            "factory": factory,
            "fields_types": tuple(f.type_oid for f in info.fields),
        },
    )
    loader.register(info.oid, context=context, format=Format.TEXT)

    # generate and register a customized binary loader
    loader = type(
        f"{info.name.title()}BinaryLoader",
        (CompositeBinaryLoader,),
        {"factory": factory},
    )
    loader.register(info.oid, context=context, format=Format.BINARY)

    if info.array_oid:
        array.register(
            info.array_oid, info.oid, context=context, name=info.name
        )


_type_info_query = """\
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
    def __init__(self, src: type, context: AdaptContext = None):
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
                ad = b'"' + self._re_escape.sub(br"\1\1", ad) + b'"'

            parts.append(ad)
            parts.append(sep)

        parts[-1] = end

        return b"".join(parts)

    _re_needs_quotes = re.compile(br'[",\\\s()]')
    _re_escape = re.compile(br"([\\\"])")


@Dumper.text(tuple)
class TupleDumper(SequenceDumper):
    def dump(self, obj: Tuple[Any, ...]) -> bytes:
        return self._dump_sequence(obj, b"(", b")", b",")


class BaseCompositeLoader(Loader):
    def __init__(self, oid: int, context: AdaptContext = None):
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
class RecordBinaryLoader(BaseCompositeLoader):
    _types_set = False

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
        self._tx.set_row_types(
            [(oid, Format.BINARY) for oid, _, _ in self._walk_record(data)]
        )


class CompositeLoader(RecordLoader):
    factory: Callable[..., Any]
    fields_types: Tuple[int, ...]
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
            [(oid, Format.TEXT) for oid in self.fields_types]
        )


class CompositeBinaryLoader(RecordBinaryLoader):
    factory: Callable[..., Any]

    def load(self, data: bytes) -> Any:
        r = super().load(data)
        return type(self).factory(*r)
