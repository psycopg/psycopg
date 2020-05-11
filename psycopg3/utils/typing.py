"""
Additional types for checking
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple
from typing import Type, Union, TYPE_CHECKING

from .. import pq

if TYPE_CHECKING:
    from ..connection import BaseConnection  # noqa
    from ..cursor import BaseCursor  # noqa
    from ..adapt import Dumper, Loader  # noqa
    from ..proto import Transformer  # noqa

# Part of the module interface (just importing it makes mypy unhappy)
Format = pq.Format


EncodeFunc = Callable[[str], Tuple[bytes, int]]
DecodeFunc = Callable[[bytes], Tuple[str, int]]

Query = Union[str, bytes]
Params = Union[Sequence[Any], Mapping[str, Any]]

AdaptContext = Union[None, "BaseConnection", "BaseCursor", "Transformer"]

MaybeOid = Union[Optional[bytes], Tuple[Optional[bytes], int]]
DumpFunc = Callable[[Any], MaybeOid]
DumperType = Union[Type["Dumper"], DumpFunc]
DumpersMap = Dict[Tuple[type, Format], DumperType]

LoadFunc = Callable[[bytes], Any]
LoaderType = Union[Type["Loader"], LoadFunc]
LoadersMap = Dict[Tuple[int, Format], LoaderType]
