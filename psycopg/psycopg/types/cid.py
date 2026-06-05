from ..abc import AdaptContext
from ._catalog import _Int4IntStrBinaryLoader, _IntStr, _StrSubclassLoader


class CID(_IntStr):
    pass


class CidLoader(_StrSubclassLoader[CID]):
    """Load cid text values as `CID` (a string subclass)"""

    cls = CID


class CidBinaryLoader(_Int4IntStrBinaryLoader[CID]):
    """Load cid binary values as `CID` (a string subclass)"""

    cls = CID


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("cid", CidLoader)
    adapters.register_loader("cid", CidBinaryLoader)
