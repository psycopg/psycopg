"""
Adapters of numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs

from ..adaptation import Adapter, Typecaster
from .oids import type_oid

_encode = codecs.lookup("ascii").encode
_decode = codecs.lookup("ascii").decode


@Adapter.register(int)
def adapt_int(obj):
    return _encode(str(obj))[0], type_oid["numeric"]


@Typecaster.register(type_oid["numeric"])
def cast_int(data):
    return int(_decode(data)[0])
