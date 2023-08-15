"""
psycopg raw queries cursors
"""

# Copyright (C) 2023 The Psycopg Team

from typing import Any, Optional, Sequence, Tuple, List, TYPE_CHECKING
from functools import lru_cache

from ._queries import PostgresQuery, QueryPart

from .abc import ConnectionType, Query, Params
from .rows import Row
from .cursor import BaseCursor, Cursor
from ._enums import PyFormat
from .cursor_async import AsyncCursor

if TYPE_CHECKING:
    from .connection import Connection  # noqa: F401
    from .connection_async import AsyncConnection  # noqa: F401


class RawPostgresQuery(PostgresQuery):
    @staticmethod
    @lru_cache()
    def query2pg(
        query: bytes, encoding: str
    ) -> Tuple[bytes, Optional[List[PyFormat]], Optional[List[str]], List[QueryPart]]:
        """
        Noop; Python raw query is already in the format Postgres understands.
        """
        return query, None, None, []

    @staticmethod
    def validate_and_reorder_params(
        parts: List[QueryPart], vars: Params, order: Optional[List[str]]
    ) -> Sequence[Any]:
        """
        Verify the compatibility; params must be a sequence for raw query.
        """
        if not PostgresQuery.is_params_sequence(vars):
            raise TypeError("raw query require a sequence of parameters")
        return vars


class RawCursorMixin(BaseCursor[ConnectionType, Row]):
    def _convert_query(
        self, query: Query, params: Optional[Params] = None
    ) -> PostgresQuery:
        pgq = RawPostgresQuery(self._tx)
        pgq.convert(query, params)
        return pgq


class RawCursor(RawCursorMixin["Connection[Any]", Row], Cursor[Row]):
    __module__ = "psycopg"


class AsyncRawCursor(RawCursorMixin["AsyncConnection[Any]", Row], AsyncCursor[Row]):
    __module__ = "psycopg"
