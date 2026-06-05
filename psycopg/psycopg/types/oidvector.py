from ..abc import AdaptContext
from .._struct import unpack_uint4
from ._catalog import _IntVectorStr, _StrSubclassLoader, _VectorBinaryLoader


class OidVector(_IntVectorStr):
    pass


class OidVectorLoader(_StrSubclassLoader[OidVector]):
    """Load oidvector text values as `OidVector` (a string subclass)"""

    cls = OidVector


class OidVectorBinaryLoader(_VectorBinaryLoader[OidVector]):
    """Load oidvector binary values as `OidVector` (a string subclass)"""

    cls = OidVector
    itemsize = 4
    unpack_item = staticmethod(unpack_uint4)


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("oidvector", OidVectorLoader)
    adapters.register_loader("oidvector", OidVectorBinaryLoader)
