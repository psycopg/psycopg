"""
Adapters for PostgreSQL catalog types.

Covers: cid, xid, xid8, pg_lsn, tid, int2vector, oidvector.
"""

from __future__ import annotations

import struct
from typing import Any, Callable, Generic, cast, overload

from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Loader
from .._compat import Self, TypeVar
from .._struct import unpack_int2, unpack_len, unpack_uint4, unpack_uint8

Value_T = TypeVar("Value_T", int, list[int], tuple[int, int])


class _StrSubclass(str, Generic[Value_T]):
    __slots__: tuple[str, ...] = ("value",)
    value: Value_T

    def _get_value(self) -> Value_T:
        # making this abstract causes type errors below
        raise NotImplementedError

    def _set_value(self, value: Value_T) -> None:
        self.value = value

    def __getattr__(self, name: str) -> Any:
        if name == "value":
            value = self._get_value()
            self._set_value(value)
            return value
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __lt__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.value < type(self)(other).value
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.value <= type(self)(other).value
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.value > type(self)(other).value
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.value >= type(self)(other).value
        return NotImplemented


class _IntStr(_StrSubclass[int]):
    __slots__ = ()
    value: int

    def _get_value(self) -> int:
        return int(str(self))

    @classmethod
    def from_int(cls, value: int) -> Self:
        obj = cls(value)
        obj._set_value(value)

        return obj

    def __int__(self) -> int:
        return self.value

    def __bool__(self) -> bool:
        return bool(self.value)

    def __add__(self, other: int) -> Self:  # type: ignore[override]
        return type(self).from_int(self.value + other)

    @overload
    def __sub__(self, other: Self) -> int: ...

    @overload
    def __sub__(self, other: int) -> Self: ...

    def __sub__(self, other: int | Self) -> Self | int:
        if isinstance(other, int):
            return type(self).from_int(self.value - other)
        return self.value - other.value


class _IntVectorStr(_StrSubclass[list[int]]):
    __slots__ = ()
    value: list[int]

    def _get_value(self) -> list[int]:
        return [int(val) for val in self.split()]

    @classmethod
    def from_list(cls, value: list[int]) -> Self:
        obj = cls(" ".join(str(i) for i in value))
        obj._set_value(value)
        return obj


Str_T = TypeVar("Str_T", bound=_StrSubclass[Any])
IntStr_T = TypeVar("IntStr_T", bound=_IntStr)
IntVectorStr_T = TypeVar("IntVectorStr_T", bound=_IntVectorStr)


class _StrSubclassLoader(Loader, Generic[Str_T]):
    cls: type[Str_T]

    def load(self, data: Buffer) -> Str_T:
        return self.cls(data, encoding="ascii")


class _Int4IntStrBinaryLoader(Loader, Generic[IntStr_T]):
    cls: type[IntStr_T]
    format = Format.BINARY

    def load(self, data: Buffer) -> IntStr_T:
        return self.cls.from_int(unpack_uint4(data)[0])


class _Int8IntStrBinaryLoader(Loader, Generic[IntStr_T]):
    cls: type[IntStr_T]
    format = Format.BINARY

    def load(self, data: Buffer) -> IntStr_T:
        return self.cls.from_int(unpack_uint8(data)[0])


class _VectorBinaryLoader(Loader, Generic[IntVectorStr_T]):
    cls: type[IntVectorStr_T]
    format = Format.BINARY
    itemsize: int
    unpack_item: Callable[[Buffer], tuple[int]]

    def load(self, data: Buffer) -> IntVectorStr_T:
        if len(data) <= 20:
            return self.cls.from_list([])

        offset = 12  # skip ndim, dataoffset, and elemtype (constant)
        n = unpack_len(data, offset)[0]
        offset += 8  # skip lbound1 (always 0)

        unpack_item = self.unpack_item
        itemsize = self.itemsize
        value = [0] * n
        for i in range(n):
            offset += 4  # skip length
            (value[i],) = unpack_item(data[offset : offset + itemsize])
            offset += itemsize
        return self.cls.from_list(value)


class CID(_IntStr):
    __slots__ = ()

    def _set_value(self, value: int) -> None:
        if not 0 <= value < 2**32:
            raise OverflowError("CID value must be in the unsigned 32 bits range")
        self.value = value


class CidLoader(_StrSubclassLoader[CID]):
    """Load cid text values as `CID` (a string subclass)"""

    cls = CID


class CidBinaryLoader(_Int4IntStrBinaryLoader[CID]):
    """Load cid binary values as `CID` (a string subclass)"""

    cls = CID


class Int2Vector(_IntVectorStr):
    __slots__ = ()

    def _set_value(self, value: list[int]) -> None:
        for i in value:
            if not -(2**15) <= value[0] < 2**15:
                raise OverflowError(
                    "Int2Vector values must be in the signed 16 bits range"
                )
        self.value = value


class Int2VectorLoader(_StrSubclassLoader[Int2Vector]):
    """Load int2vector text values as `Int2Vector` (a string subclass)"""

    cls = Int2Vector


class Int2VectorBinaryLoader(_VectorBinaryLoader[Int2Vector]):
    """Load int2vector binary values as `Int2Vector` (a string subclass)"""

    cls = Int2Vector
    itemsize = 2
    unpack_item = staticmethod(unpack_int2)


class LSN(_IntStr):
    __slots__ = ()

    def _get_value(self) -> int:
        hi, lo = self.split("/")
        lo_in = int(lo, 16)
        if (lo_int := lo_in & 0xFFFFFFFF) != lo_in:
            # hi overflow is check in _set_value
            raise OverflowError("LSN low bytes must be in the unsigned 32 bits range")
        return (int(hi, 16) << 32) | lo_int

    def _set_value(self, value: int) -> None:
        if not 0 <= value < 2**64:
            raise OverflowError("LSN value must be in the unsigned 64 bits range")
        self.value = value

    @classmethod
    def from_int(cls, value: int) -> Self:
        high = (value >> 32) & 0xFFFFFFFF
        low = value & 0xFFFFFFFF
        obj = cls(f"{high:X}/{low:X}")
        obj._set_value(value)

        return obj

    def __eq__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return self.value == other.value
        if isinstance(other, str):
            return self.upper() == other.upper()
        if isinstance(other, int):
            return self.value == other

        return False

    @property
    def high(self) -> int:
        return (self.value >> 32) & 0xFFFFFFFF

    @property
    def low(self) -> int:
        return self.value & 0xFFFFFFFF


class LsnLoader(_StrSubclassLoader[LSN]):
    """Load pg_lsn text values as `LSN` (a string subclass)"""

    cls = LSN


class LsnBinaryLoader(_Int8IntStrBinaryLoader[LSN]):
    """Load pg_lsn binary values as `LSN` (a string subclass)"""

    cls = LSN


class OidVector(_IntVectorStr):
    __slots__ = ()

    def _set_value(self, value: list[int]) -> None:
        for i in value:
            if not 0 <= value[0] < 2**32:
                raise OverflowError(
                    "OidVector values must be in the unsigned 32 bits range"
                )
        self.value = value


class OidVectorLoader(_StrSubclassLoader[OidVector]):
    """Load oidvector text values as `OidVector` (a string subclass)"""

    cls = OidVector


class OidVectorBinaryLoader(_VectorBinaryLoader[OidVector]):
    """Load oidvector binary values as `OidVector` (a string subclass)"""

    cls = OidVector
    itemsize = 4
    unpack_item = staticmethod(unpack_uint4)


unpack_tid = cast(Callable[[Buffer], tuple[int, int]], struct.Struct("!IH").unpack)


class TID(_StrSubclass[tuple[int, int]]):
    __slots__ = ()
    value: tuple[int, int]

    def _get_value(self) -> tuple[int, int]:
        i = self.index(",")
        return (int(self[1:i]), int(self[i + 1 : -1]))

    def _set_value(self, value: tuple[int, int]) -> None:
        if not 0 <= value[0] < 2**32:
            raise OverflowError("TID block must be in the unsigned 32 bits range")
        if not 0 <= value[1] < 2**16:
            raise OverflowError("TID offset must be in the unsigned 16 bits range")
        self.value = value

    def __bool__(self) -> bool:
        return bool(self.value[0] or self.value[1])

    @property
    def block(self) -> int:
        return self.value[0]

    @property
    def offset(self) -> int:
        return self.value[1]

    @classmethod
    def from_tuple(cls, value: tuple[int, int]) -> Self:
        obj = cls(f"({value[0]},{value[1]})")
        obj._set_value(value)
        return obj


class TidLoader(_StrSubclassLoader[TID]):
    """Load tid text values as `TID` (a string subclass)"""

    cls = TID


class TidBinaryLoader(Loader):
    """Load tid binary values as `TID` (a string subclass)"""

    format = Format.BINARY

    def load(self, data: Buffer) -> TID:
        return TID.from_tuple(unpack_tid(data))


class XID(_IntStr):
    __slots__ = ()

    def _set_value(self, value: int) -> None:
        if not 0 <= value < 2**32:
            raise OverflowError("XID value must be in the unsigned 32 bits range")
        self.value = value


class XID8(_IntStr):
    __slots__ = ()

    def _set_value(self, value: int) -> None:
        if not 0 <= value < 2**64:
            raise OverflowError("XID8 value must be in the unsigned 64 bits range")
        self.value = value

    @property
    def xid(self) -> XID:
        return XID.from_int(self.value & 0xFFFFFFFF)

    @property
    def epoch(self) -> int:
        return (self.value >> 32) & 0xFFFFFFFF


class XidLoader(_StrSubclassLoader[XID]):
    """Load xid text values as `XID` (a string subclass)"""

    cls = XID


class XidBinaryLoader(_Int4IntStrBinaryLoader[XID]):
    """Load xid binary values as `XID` (a string subclass)"""

    cls = XID


class Xid8Loader(_StrSubclassLoader[XID8]):
    """Load xid or xid8 text values as `XID8` (a string subclass)"""

    cls = XID8


class Xid8BinaryLoader(_Int8IntStrBinaryLoader[XID8]):
    """Load xid8 binary values as `XID8` (a string subclass)"""

    cls = XID8


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("cid", CidLoader)
    adapters.register_loader("cid", CidBinaryLoader)
    adapters.register_loader("int2vector", Int2VectorLoader)
    adapters.register_loader("int2vector", Int2VectorBinaryLoader)
    adapters.register_loader("oidvector", OidVectorLoader)
    adapters.register_loader("oidvector", OidVectorBinaryLoader)
    adapters.register_loader("pg_lsn", LsnLoader)
    adapters.register_loader("pg_lsn", LsnBinaryLoader)
    adapters.register_loader("tid", TidLoader)
    adapters.register_loader("tid", TidBinaryLoader)
    adapters.register_loader("xid", XidLoader)
    adapters.register_loader("xid", XidBinaryLoader)
    adapters.register_loader("xid8", Xid8Loader)
    adapters.register_loader("xid8", Xid8BinaryLoader)
