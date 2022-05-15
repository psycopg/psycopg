"""
psycopg client-side binding cursors
"""

# Copyright (C) 2022 The Psycopg Team

from typing import Optional, Tuple, TYPE_CHECKING
from functools import partial

from ._queries import PostgresQuery, PostgresClientQuery

from . import pq
from . import adapt
from . import errors as e
from .abc import ConnectionType, Query, Params
from .rows import Row
from .cursor import BaseCursor, Cursor
from ._preparing import Prepare
from .cursor_async import AsyncCursor

if TYPE_CHECKING:
    from typing import Any  # noqa: F401
    from .connection import Connection  # noqa: F401
    from .connection_async import AsyncConnection  # noqa: F401

TEXT = pq.Format.TEXT
BINARY = pq.Format.BINARY


class ClientCursorMixin(BaseCursor[ConnectionType, Row]):
    def mogrify(self, query: Query, params: Optional[Params] = None) -> str:
        """
        Return the query and parameters merged.

        Parameters are adapted and merged to the query the same way that
        `!execute()` would do.

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
            fmt = BINARY if binary else TEXT

        if fmt == BINARY:
            raise e.NotSupportedError(
                "client-side cursors don't support binary results"
            )

        self._query = query
        if no_pqexec:
            if self._conn._pipeline:
                self._conn._pipeline.command_queue.append(
                    partial(self._pgconn.send_query_params, None)
                )
            else:
                self._pgconn.send_query_params(query.query, None)
        else:
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


class ClientCursor(ClientCursorMixin["Connection[Any]", Row], Cursor[Row]):
    __module__ = "psycopg"


class AsyncClientCursor(
    ClientCursorMixin["AsyncConnection[Any]", Row], AsyncCursor[Row]
):
    __module__ = "psycopg"
