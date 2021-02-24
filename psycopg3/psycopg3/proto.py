"""
Protocol objects representing different implementations of the same classes.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any, Callable, Generator, Mapping
from typing import List, Optional, Sequence, Tuple, TypeVar, Union
from typing import TYPE_CHECKING
from typing_extensions import Protocol

from . import pq
from ._enums import Format

if TYPE_CHECKING:
    from .connection import BaseConnection
    from .cursor import BaseCursor
    from .adapt import Dumper, Loader, AdaptersMap
    from .waiting import Wait, Ready
    from .sql import Composable

# An object implementing the buffer protocol
Buffer = Union[bytes, bytearray, memoryview]

Query = Union[str, bytes, "Composable"]
Params = Union[Sequence[Any], Mapping[str, Any]]
ConnectionType = TypeVar("ConnectionType", bound="BaseConnection")


# Waiting protocol types

RV = TypeVar("RV")

PQGenConn = Generator[Tuple[int, "Wait"], "Ready", RV]
"""Generator for processes where the connection file number can change.

This can happen in connection and reset, but not in normal querying.
"""

PQGen = Generator["Wait", "Ready", RV]
"""Generator for processes where the connection file number won't change.

The first item generated is the file descriptor; following items are be the
Wait states.
"""


# Row factories

Row = TypeVar("Row", Tuple[Any, ...], Any)


class RowMaker(Protocol):
    def __call__(self, __values: Sequence[Any]) -> Any:
        ...


class RowFactory(Protocol):
    def __call__(self, __cursor: "BaseCursor[Any]") -> RowMaker:
        ...


# Adaptation types

DumpFunc = Callable[[Any], bytes]
LoadFunc = Callable[[bytes], Any]

# TODO: Loader, Dumper should probably become protocols
# as there are both C and a Python implementation


class AdaptContext(Protocol):
    """
    A context describing how types are adapted.

    Example of AdaptContext are connections, cursors, transformers.
    """

    @property
    def adapters(self) -> "AdaptersMap":
        ...

    @property
    def connection(self) -> Optional["BaseConnection"]:
        ...


class Transformer(Protocol):
    def __init__(self, context: Optional[AdaptContext] = None):
        ...

    make_row: RowMaker

    @property
    def connection(self) -> Optional["BaseConnection"]:
        ...

    @property
    def adapters(self) -> "AdaptersMap":
        ...

    @property
    def pgresult(self) -> Optional[pq.proto.PGresult]:
        ...

    def set_pgresult(
        self, result: Optional[pq.proto.PGresult], set_loaders: bool = True
    ) -> None:
        ...

    def set_row_types(
        self, types: Sequence[int], formats: Sequence[pq.Format]
    ) -> None:
        ...

    def dump_sequence(
        self, params: Sequence[Any], formats: Sequence[Format]
    ) -> Tuple[List[Any], Tuple[int, ...], Sequence[pq.Format]]:
        ...

    def get_dumper(self, obj: Any, format: Format) -> "Dumper":
        ...

    def load_rows(self, row0: int, row1: int) -> List[Row]:
        ...

    def load_row(self, row: int) -> Optional[Row]:
        ...

    def load_sequence(
        self, record: Sequence[Optional[bytes]]
    ) -> Tuple[Any, ...]:
        ...

    def get_loader(self, oid: int, format: pq.Format) -> "Loader":
        ...
