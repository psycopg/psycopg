"""
Adapters of numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Tuple

from ..adaptation import Adapter, Typecaster
from .oids import type_oid

_encode = codecs.lookup("ascii").encode
_decode = codecs.lookup("ascii").decode


@Adapter.text(int)
def adapt_int(obj: int) -> Tuple[bytes, int]:
    return _encode(str(obj))[0], type_oid["numeric"]


@Typecaster.text(type_oid["numeric"])
def cast_int(data: bytes) -> int:
    return int(_decode(data)[0])
