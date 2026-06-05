from ..pq import Format
from ..abc import AdaptContext, Buffer
from ..adapt import Loader
from .._compat import Self
from .._struct import unpack_uint4_uint2
from ._catalog import _StrSubclass, _StrSubclassLoader


class TID(_StrSubclass):
    block: int
    offset: int

    @classmethod
    def from_buffer(cls, val: Buffer) -> Self:
        block, offset = bytes(val)[1:-1].split(b",")
        obj = cls(f"({block.decode('ascii')},{offset.decode('ascii')})")
        obj.block = int(block)
        obj.offset = int(offset)

        return obj

    @classmethod
    def from_block_and_offset(cls, block: int, offset: int) -> Self:
        obj = cls(f"({block},{offset})")
        obj.block = block
        obj.offset = offset
        return obj

    def __repr__(self) -> str:
        return f"TID('{self}')"


class TidLoader(_StrSubclassLoader[TID]):
    """Load tid text values as `TID` (a string subclass)"""

    cls = TID


class TidBinaryLoader(Loader):
    """Load tid binary values as `TID` (a string subclass)"""

    format = Format.BINARY

    def load(self, data: Buffer) -> TID:
        return TID.from_block_and_offset(*unpack_uint4_uint2(data))


def register_default_adapters(context: AdaptContext) -> None:
    adapters = context.adapters

    adapters.register_loader("tid", TidLoader)
    adapters.register_loader("tid", TidBinaryLoader)
