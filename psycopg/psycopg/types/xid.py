from ..abc import AdaptContext
from ._catalog import _Int4IntStrBinaryLoader, _Int8IntStrBinaryLoader, _IntStr
from ._catalog import _StrSubclassLoader


class XID(_IntStr):
    __slots__ = ()


class XID8(_IntStr):
    __slots__ = ()

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

    adapters.register_loader("xid", XidLoader)
    adapters.register_loader("xid", XidBinaryLoader)
    adapters.register_loader("xid8", Xid8Loader)
    adapters.register_loader("xid8", Xid8BinaryLoader)
