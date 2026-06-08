"""
Adapters for PostgreSQL catalog types.

Covers: cid, xid, xid8, pg_lsn, tid, int2vector, oidvector.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Callable, Generic
from functools import cached_property

from ..pq import Format
from ..abc import Buffer
from ..adapt import Loader
from .._compat import Self, TypeVar
from .._struct import unpack_len, unpack_uint4, unpack_uint8


class _StrSubclass(str):
    @classmethod
    @abstractmethod
    def from_buffer(cls, val: Buffer) -> Self: ...

    def __repr__(self) -> str:
        return f"{type(self).__name__}('{self}')"


class _IntStr(_StrSubclass):
    @cached_property
    def value(self) -> int:
        return int(self)

    @classmethod
    def from_buffer(cls, val: Buffer) -> Self:
        value = int(val)
        obj = cls(value)
        obj.value = value

        return obj

    @classmethod
    def from_int(cls, val: int) -> Self:
        obj = cls(val)
        obj.value = val

        return obj


class _IntVectorStr(_StrSubclass):
    @cached_property
    def value(self) -> list[int]:
        return [int(val) for val in self.split()]

    @classmethod
    def from_buffer(cls, val: Buffer) -> Self:
        if isinstance(val, memoryview):
            val = bytes(val)
        data = val.strip()
        if not data:
            value = []
        else:
            value = [int(b) for b in data.split()]
        obj = cls(f"{data.decode('ascii')}")
        obj.value = value

        return obj

    @classmethod
    def from_list(cls, val: list[int]) -> Self:
        obj = cls(" ".join(str(i) for i in val))
        obj.value = val
        return obj


Str_T = TypeVar("Str_T", bound=_StrSubclass)
IntStr_T = TypeVar("IntStr_T", bound=_IntStr)
IntVectorStr_T = TypeVar("IntVectorStr_T", bound=_IntVectorStr)


class _StrSubclassLoader(Loader, Generic[Str_T]):
    cls: type[Str_T]

    def load(self, data: Buffer) -> Str_T:
        return self.cls.from_buffer(data)


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
            _ = unpack_len(data, offset)
            value[i] = unpack_item(data[offset + 4 : offset + 4 + itemsize])[0]
            offset += 4 + itemsize
        return self.cls.from_list(value)
