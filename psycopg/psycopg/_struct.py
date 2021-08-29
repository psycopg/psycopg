"""
Utility functions to deal with binary structs.
"""

# Copyright (C) 2020-2021 The Psycopg Team

import struct
from typing import Callable, cast, Optional, Tuple

from .abc import Buffer
from ._compat import Protocol

PackInt = Callable[[int], bytes]
UnpackInt = Callable[[bytes], Tuple[int]]
PackFloat = Callable[[float], bytes]
UnpackFloat = Callable[[bytes], Tuple[float]]


class UnpackLen(Protocol):
    def __call__(self, data: Buffer, start: Optional[int]) -> Tuple[int]:
        ...


pack_int2 = cast(PackInt, struct.Struct("!h").pack)
pack_uint2 = cast(PackInt, struct.Struct("!H").pack)
pack_int4 = cast(PackInt, struct.Struct("!i").pack)
pack_uint4 = cast(PackInt, struct.Struct("!I").pack)
pack_int8 = cast(PackInt, struct.Struct("!q").pack)
pack_float4 = cast(PackFloat, struct.Struct("!f").pack)
pack_float8 = cast(PackFloat, struct.Struct("!d").pack)

unpack_int2 = cast(UnpackInt, struct.Struct("!h").unpack)
unpack_uint2 = cast(UnpackInt, struct.Struct("!H").unpack)
unpack_int4 = cast(UnpackInt, struct.Struct("!i").unpack)
unpack_uint4 = cast(UnpackInt, struct.Struct("!I").unpack)
unpack_int8 = cast(UnpackInt, struct.Struct("!q").unpack)
unpack_float4 = cast(UnpackFloat, struct.Struct("!f").unpack)
unpack_float8 = cast(UnpackFloat, struct.Struct("!d").unpack)

_struct_len = struct.Struct("!i")
pack_len = cast(Callable[[int], bytes], _struct_len.pack)
unpack_len = cast(UnpackLen, _struct_len.unpack_from)
