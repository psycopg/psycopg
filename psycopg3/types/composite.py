"""
Support for composite types adaptation.
"""

import re
import struct
from collections import namedtuple
from typing import Any, Callable, Generator, List, Sequence, Tuple, Union
from typing import Optional, TYPE_CHECKING

from ..adapt import Format, TypeCaster, Transformer, AdaptContext
from .oids import builtins, TypeInfo
from .array import register_array

if TYPE_CHECKING:
    from ..connection import Connection


TEXT_OID = builtins["text"].oid


class FieldInfo:
    def __init__(self, name: str, type_oid: int):
        self.name = name
        self.type_oid = type_oid


class CompositeTypeInfo(TypeInfo):
    def __init__(
        self,
        name: str,
        oid: int,
        array_oid: int,
        fields: Sequence[Union[FieldInfo, Tuple[str, int]]],
    ):
        super().__init__(name, oid, array_oid)
        self.fields: List[FieldInfo] = []
        for f in fields:
            if isinstance(f, FieldInfo):
                self.fields.append(f)
            elif isinstance(f, tuple):
                self.fields.append(FieldInfo(f[0], f[1]))
            else:
                raise TypeError(f"bad field info: {f}")


def fetch_info(conn: "Connection", name: str) -> Optional[CompositeTypeInfo]:
    cur = conn.cursor(binary=True)
    cur.execute(_type_info_query, (name,))
    rec = cur.fetchone()
    if rec is not None:
        return CompositeTypeInfo(*rec)
    else:
        return None


def register(
    info: CompositeTypeInfo,
    context: AdaptContext = None,
    factory: Optional[Callable[..., Any]] = None,
) -> None:
    if factory is None:
        factory = namedtuple(  # type: ignore
            info.name, [f.name for f in info.fields]
        )

    # generate and register a customized text typecaster
    caster = type(
        f"{info.name.title()}Caster",
        (CompositeCaster,),
        {
            "factory": factory,
            "fields_types": tuple(f.type_oid for f in info.fields),
        },
    )
    TypeCaster.register(info.oid, caster, context=context, format=Format.TEXT)

    # generate and register a customized binary typecaster
    caster = type(
        f"{info.name.title()}BinaryCaster",
        (CompositeBinaryCaster,),
        {"factory": factory},
    )
    TypeCaster.register(
        info.oid, caster, context=context, format=Format.BINARY
    )

    if info.array_oid:
        register_array(
            info.array_oid, info.oid, context=context, name=info.name
        )


_type_info_query = """\
select
    name, oid, array_oid,
    array_agg(row(field_name, field_type)) as fields
from (
    select
        typname as name,
        t.oid as oid,
        t.typarray as array_oid,
        a.attname as field_name,
        a.atttypid as field_type
    from pg_type t
    left join pg_attribute a on a.attrelid = t.typrelid
    where t.typname = %s
    and a.attnum > 0
    order by a.attnum
) x
group by name, oid, array_oid
"""


class BaseCompositeCaster(TypeCaster):
    def __init__(self, oid: int, context: AdaptContext = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)


@TypeCaster.text(builtins["record"].oid)
class RecordCaster(BaseCompositeCaster):
    def cast(self, data: bytes) -> Tuple[Any, ...]:
        cast = self._tx.get_cast_function(TEXT_OID, format=Format.TEXT)
        return tuple(
            cast(token) if token is not None else None
            for token in self._parse_record(data)
        )

    def _parse_record(
        self, data: bytes
    ) -> Generator[Optional[bytes], None, None]:
        if data == b"()":
            return

        for m in self._re_tokenize.finditer(data):
            if m.group(1) is not None:
                yield None
            elif m.group(2) is not None:
                yield self._re_undouble.sub(br"\1", m.group(2))
            else:
                yield m.group(3)

    _re_tokenize = re.compile(
        br"""(?x)
          \(? ([,)])                        # an empty token, representing NULL
        | \(? " ((?: [^"] | "")*) " [,)]    # or a quoted string
        | \(? ([^",)]+) [,)]                # or an unquoted string
        """
    )

    _re_undouble = re.compile(br'(["\\])\1')


_struct_len = struct.Struct("!i")
_struct_oidlen = struct.Struct("!Ii")


@TypeCaster.binary(builtins["record"].oid)
class RecordBinaryCaster(BaseCompositeCaster):
    _types_set = False

    def cast(self, data: bytes) -> Tuple[Any, ...]:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        return tuple(
            self._tx.cast_sequence(
                data[offset : offset + length] if length != -1 else None
                for _, offset, length in self._walk_record(data)
            )
        )

    def _walk_record(
        self, data: bytes
    ) -> Generator[Tuple[int, int, int], None, None]:
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
            (oid, Format.BINARY) for oid, _, _ in self._walk_record(data)
        )


class CompositeCaster(RecordCaster):
    factory: Callable[..., Any]
    fields_types: Tuple[int, ...]
    _types_set = False

    def cast(self, data: bytes) -> Any:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        return type(self).factory(
            *self._tx.cast_sequence(self._parse_record(data))
        )

    def _config_types(self, data: bytes) -> None:
        self._tx.set_row_types((oid, Format.TEXT) for oid in self.fields_types)


class CompositeBinaryCaster(RecordBinaryCaster):
    factory: Callable[..., Any]

    def cast(self, data: bytes) -> Any:
        r = super().cast(data)
        return type(self).factory(*r)
