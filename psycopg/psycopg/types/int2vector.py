from ..abc import AdaptContext
from .._struct import unpack_int2
from ._catalog import _IntVectorStr, _StrSubclassLoader, _VectorBinaryLoader


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


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("int2vector", Int2VectorLoader)
    adapters.register_loader("int2vector", Int2VectorBinaryLoader)
