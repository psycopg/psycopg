"""
Adapters for PostgreSQL catalog types.

Covers: cid, xid, xid8, pg_lsn, tid, int2vector, oidvector.
"""

from __future__ import annotations

from typing import Any, Callable, Generic

from ..pq import Format
from ..abc import Buffer
from ..adapt import Loader
from .._compat import Self, TypeVar
from .._struct import unpack_len, unpack_uint4, unpack_uint8

Value_T = TypeVar("Value_T", int, list[int], tuple[int, int])


class _StrSubclass(str, Generic[Value_T]):
    __slots__: tuple[str, ...] = ("value",)
    value: Value_T

    def _get_value(self) -> Value_T:
        # making this abstract causes type errors below
        raise NotImplementedError

    def __getattr__(self, name: str) -> Any:
        if name == "value":
            value = self._get_value()
            self.value = value
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
    def from_int(cls, val: int) -> Self:
        obj = cls(val)
        obj.value = val

        return obj

    def __int__(self) -> int:
        return self.value

    def __bool__(self) -> bool:
        return bool(self.value)

    def __add__(self, other: int) -> Self:  # type: ignore[override]
        return type(self).from_int(self.value + other)

    def __sub__(self, other: Self) -> int:
        return self.value - other.value


class _IntVectorStr(_StrSubclass[list[int]]):
    __slots__ = ()
    value: list[int]

    def _get_value(self) -> list[int]:
        return [int(val) for val in self.split()]

    @classmethod
    def from_list(cls, val: list[int]) -> Self:
        obj = cls(" ".join(str(i) for i in val))
        obj.value = val
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
