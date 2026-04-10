from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from ..abc import AdaptContext, ConnParam
from ..rows import AsyncRowFactory, Row, tuple_row
from .._compat import Self
from ..cursor_async import AsyncCursor
from ..client_cursor import AsyncClientCursor
from ..connection_async import AsyncConnection
from .._server_cursor_async import AsyncServerCursor
from .logical_replication_cursor_async import (
    AsyncLogicalReplicationCursor,
)
from .physical_replication_cursor_async import (
    AsyncPhysicalReplicationCursor,
)

if TYPE_CHECKING:
    from ..pq.abc import PGconn


logger = logging.getLogger("psycopg")


class BaseReplicationConnection(AsyncConnection[Row]):
    __module__ = "psycopg.replication"

    # Only simple query protocol is supported in ReplicationConnections
    # so use ClientCursors only
    cursor_factory: type[AsyncClientCursor[Row]]
    server_cursor_factory: type[AsyncServerCursor[Row]]
    row_factory: AsyncRowFactory[Row]

    _replication_connection_string: str

    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: int | None = 5,
        context: AdaptContext | None = None,
        row_factory: AsyncRowFactory[Row] | None = None,
        cursor_factory: type[AsyncCursor[Row]] | None = None,
        **kwargs: ConnParam,
    ) -> Self:
        """
        Connect to a database server and return a new
        `AsyncLogicalReplicationConnection` instance.
        """
        if "replication" in kwargs or "replication=" in conninfo:
            raise ValueError(
                "Unexpected connection parameter: replication."
                + "ReplicationConnections manage their own replication type."
            )
        kwargs["replication"] = cls._replication_connection_string
        return await super().connect(
            conninfo,
            autocommit=autocommit,
            prepare_threshold=prepare_threshold,
            context=context,
            row_factory=row_factory,
            cursor_factory=cursor_factory,
            **kwargs,
        )


class AsyncLogicalReplicationConnection(BaseReplicationConnection[Row]):
    """
    Wrapper for a logical replication connection to the database
    (i.e. replication=database).
    """

    __module__ = "psycopg.replication"

    _replication_connection_string = "database"

    def __init__(
        self,
        pgconn: PGconn,
        row_factory: AsyncRowFactory[Row] = cast(AsyncRowFactory[Row], tuple_row),
    ):
        super().__init__(pgconn, row_factory)
        self.cursor_factory = AsyncLogicalReplicationCursor


class AsyncPhysicalReplicationConnection(BaseReplicationConnection[Row]):
    """
    Wrapper for a physical replication connection to the database
    (i.e. replication=true).
    """

    __module__ = "psycopg.replication"

    _replication_connection_string = "true"

    def __init__(
        self,
        pgconn: PGconn,
        row_factory: AsyncRowFactory[Row] = cast(AsyncRowFactory[Row], tuple_row),
    ):
        super().__init__(pgconn, row_factory)
        self.cursor_factory = AsyncPhysicalReplicationCursor
