from .catalog import _Int4IntStrBinaryLoader, _Int8IntStrBinaryLoader, _IntStr
from .catalog import _StrSubclassLoader


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
