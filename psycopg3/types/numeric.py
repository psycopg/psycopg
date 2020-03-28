"""
Adapters of numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

import codecs

from ..adaptation import Adapter, Typecaster
from .oids import type_oid


@Adapter.register(int)
def adapt_int(obj, encode=codecs.lookup("ascii").encode):
    return encode(str(obj))[0], type_oid["numeric"]


@Typecaster.register(type_oid["numeric"])
def cast_int(data, decode=codecs.lookup("ascii").decode):
    return int(decode(data)[0])
