"""
psycopg client-side binding cursors
"""

# Copyright (C) 2022 The Psycopg Team

from typing import Optional, Tuple, TYPE_CHECKING
from functools import partial

from ._queries import PostgresQuery, PostgresClientQuery

from . import adapt
from . import errors as e
from .pq import Format
from .abc import ConnectionType, Query, Params
from .rows import Row
from .cursor import BaseCursor, Cursor
from ._preparing import Prepare
from .cursor_async import AsyncCursor

if TYPE_CHECKING:
    from .connection import Connection  # noqa: F401
    from .connection_async import AsyncConnection  # noqa: F401


class ClientCursorMixin(BaseCursor[ConnectionType, Row]):
    def mogrify(self, query: Query, params: Optional[Params] = None) -> str:
        """
        Return the query to be executed with parameters merged.
        """
        self._tx = adapt.Transformer(self)
        pgq = self._convert_query(query, params)
        return pgq.query.decode(self._tx.encoding)

    def _execute_send(
        self,
        query: PostgresQuery,
        *,
        no_pqexec: bool = False,
        binary: Optional[bool] = None,
    ) -> None:
        if binary is None:
            fmt = self.format
        else:
            fmt = Format.BINARY if binary else Format.TEXT

        if fmt == Format.BINARY:
            raise e.NotSupportedError(
                "client-side cursors don't support binary results"
            )

        if no_pqexec:
            raise e.NotSupportedError(
                "PQexec operations not supported by client-side cursors"
            )

        self._query = query
        # if we don't have to, let's use exec_ as it can run more than
        # one query in one go
        if self._conn._pipeline:
            self._conn._pipeline.command_queue.append(
                partial(self._pgconn.send_query, query.query)
            )
        else:
            self._pgconn.send_query(query.query)

    def _convert_query(
        self, query: Query, params: Optional[Params] = None
    ) -> PostgresQuery:
        pgq = PostgresClientQuery(self._tx)
        pgq.convert(query, params)
        return pgq

    def _get_prepared(
        self, pgq: PostgresQuery, prepare: Optional[bool] = None
    ) -> Tuple[Prepare, bytes]:
        return (Prepare.NO, b"")

    def _is_pipeline_supported(self) -> bool:
        return False


class ClientCursor(ClientCursorMixin["Connection[Row]", Row], Cursor[Row]):
    pass


class AsyncClientCursor(
    ClientCursorMixin["AsyncConnection[Row]", Row], AsyncCursor[Row]
):
    pass
