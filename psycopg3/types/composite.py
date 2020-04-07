"""
Support for composite types adaptation.
"""

import re
import struct
from typing import Any, Generator, Optional, Tuple

from ..adapt import Format, TypeCaster, Transformer, AdaptContext
from .oids import builtins


TEXT_OID = builtins["text"].oid


_re_tokenize = re.compile(
    br"""(?x)
      \(? ([,)])                        # an empty token, representing NULL
    | \(? " ((?: [^"] | "")*) " [,)]    # or a quoted string
    | \(? ([^",)]+) [,)]                # or an unquoted string
    """
)

_re_undouble = re.compile(br'(["\\])\1')


class BaseCompositeCaster(TypeCaster):
    def __init__(self, oid: int, context: AdaptContext = None):
        super().__init__(oid, context)
        self.tx = Transformer(context)


@TypeCaster.text(builtins["record"].oid)
class RecordCaster(BaseCompositeCaster):
    def cast(self, data: bytes) -> Tuple[Any, ...]:
        cast = self.tx.get_cast_function(TEXT_OID, format=Format.TEXT)
        return tuple(
            cast(item) if item is not None else None
            for item in self._parse_record(data)
        )

    def _parse_record(
        self, data: bytes
    ) -> Generator[Optional[bytes], None, None]:
        if data == b"()":
            return

        for m in _re_tokenize.finditer(data):
            if m.group(1) is not None:
                yield None
            elif m.group(2) is not None:
                yield _re_undouble.sub(br"\1", m.group(2))
            else:
                yield m.group(3)


_struct_len = struct.Struct("!i")
_struct_oidlen = struct.Struct("!Ii")


@TypeCaster.binary(builtins["record"].oid)
class BinaryRecordCaster(BaseCompositeCaster):
    _types_set = False

    def cast(self, data: bytes) -> Tuple[Any, ...]:
        if not self._types_set:
            self._config_types(data)
            self._types_set = True

        return tuple(
            self.tx.cast_sequence(
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
        self.tx.set_row_types(
            (oid, Format.BINARY) for oid, _, _ in self._walk_record(data)
        )
