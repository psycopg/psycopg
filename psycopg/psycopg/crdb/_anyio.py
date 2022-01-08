"""
CockroachDB-specific connections for AnyIO.
"""

# Copyright (C) 2022 The Psycopg Team


from typing import Any, Optional, Union, Type, overload, TYPE_CHECKING

from ..abc import AdaptContext
from ..rows import AsyncRowFactory, Row, TupleRow
from .._anyio.connection import AnyIOConnection
from .connection import _CrdbConnectionMixin

if TYPE_CHECKING:
    from ..cursor_async import AsyncCursor


class AnyIOCrdbConnection(_CrdbConnectionMixin, AnyIOConnection[Row]):
    """
    Wrapper for an async connection to a CockroachDB database using AnyIO
    asynchronous library.
    """

    __module__ = "psycopg.crdb"

    # TODO: this method shouldn't require re-definition if the base class
    # implements a generic self.
    # https://github.com/psycopg/psycopg/issues/308
    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        row_factory: AsyncRowFactory[Row],
        cursor_factory: "Optional[Type[AsyncCursor[Row]]]" = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AnyIOCrdbConnection[Row]":
        ...

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: "Optional[Type[AsyncCursor[Any]]]" = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AnyIOCrdbConnection[TupleRow]":
        ...

    @classmethod
    async def connect(
        cls, conninfo: str = "", **kwargs: Any
    ) -> "AnyIOCrdbConnection[Any]":
        return await super().connect(conninfo, **kwargs)  # type: ignore [no-any-return]
