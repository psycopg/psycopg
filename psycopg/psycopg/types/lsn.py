from ..abc import AdaptContext
from .._compat import Self
from ._catalog import _Int8IntStrBinaryLoader, _IntStr, _StrSubclassLoader


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
