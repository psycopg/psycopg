"""
Utility module to access fast encoders/decoders
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
from typing import Callable, Tuple

EncodeFunc = Callable[[str], Tuple[bytes, int]]
DecodeFunc = Callable[[bytes], Tuple[str, int]]

encode_ascii = codecs.lookup("ascii").encode
decode_ascii = codecs.lookup("ascii").decode
encode_utf8 = codecs.lookup("utf8").encode
decode_utf8 = codecs.lookup("utf8").decode
