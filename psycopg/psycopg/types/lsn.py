from ..abc import AdaptContext
from .._compat import Self
from ._catalog import _Int8IntStrBinaryLoader, _IntStr, _StrSubclassLoader


class LSN(_IntStr):
    __slots__ = ()

    def _get_value(self) -> int:
        hi, lo = self.split("/")
        return (int(hi, 16) << 32) | int(lo, 16)

    @classmethod
    def from_int(cls, val: int) -> Self:
        high = (val >> 32) & 0xFFFFFFFF
        low = val & 0xFFFFFFFF
        obj = cls(f"{high:X}/{low:X}")
        obj.value = val

        return obj

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


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("pg_lsn", LsnLoader)
    adapters.register_loader("pg_lsn", LsnBinaryLoader)
