"""
Support for composite types adaptation.
"""

import re
from typing import Any, Generator, Optional, Tuple

from ..pq import Format
from ..adapt import TypeCaster, Transformer, AdaptContext
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


@TypeCaster.text(builtins["record"].oid)
class RecordCaster(TypeCaster):
    def __init__(self, oid: int, context: AdaptContext = None):
        super().__init__(oid, context)
        self.tx = Transformer(context)

    def cast(self, data: bytes) -> Tuple[Any, ...]:
        cast = self.tx.get_cast_function(TEXT_OID, format=Format.TEXT)
        return tuple(
            cast(item) if item is not None else None
            for item in self.parse_record(data)
        )

    def parse_record(
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
