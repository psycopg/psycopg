import struct
from typing import Callable, cast

from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Loader
from .._compat import Self
from ._catalog import _StrSubclass, _StrSubclassLoader

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


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("tid", TidLoader)
    adapters.register_loader("tid", TidBinaryLoader)
