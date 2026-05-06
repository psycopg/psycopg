from __future__ import annotations

import pytest

from psycopg.replication import (
    AsyncLogicalReplicationConnection,
    AsyncLogicalReplicationCursor,
    AsyncPhysicalReplicationConnection,
    AsyncPhysicalReplicationCursor,
)

from .params import repl_class_param

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


@pytest.mark.parametrize(
    "conn_cls,replication_val",
    [
        repl_class_param(AsyncPhysicalReplicationConnection, b"true"),
        repl_class_param(AsyncLogicalReplicationConnection, b"database"),
    ],
)
async def test_connect(dsn, conn_cls, replication_val):
    async with await conn_cls.connect(dsn, autocommit=True) as conn:
        assert not conn.closed
        assert (
            replication_val
            == next(
                conn_option
                for conn_option in conn.pgconn.info
                if conn_option.keyword == b"replication"
            ).val
        )
    assert conn.closed


@pytest.mark.parametrize(
    "conn_cls",
    [
        repl_class_param(AsyncPhysicalReplicationConnection),
        repl_class_param(AsyncLogicalReplicationConnection),
    ],
)
async def test_connect_replication_param_rejected(dsn, conn_cls):
    """Passing 'replication' explicitly should raise ValueError."""
    for val in ("true", "database"):
        with pytest.raises(ValueError, match="replication"):
            await conn_cls.connect(dsn, autocommit=True, replication=val)
        with pytest.raises(ValueError, match="replication"):
            await conn_cls.connect(dsn + f" replication={val}")


@pytest.mark.parametrize(
    "conn_cls,cur_cls",
    [
        repl_class_param(
            AsyncPhysicalReplicationConnection,
            AsyncPhysicalReplicationCursor,
        ),
        repl_class_param(
            AsyncLogicalReplicationConnection,
            AsyncLogicalReplicationCursor,
        ),
    ],
)
async def test_default_cursor_factory(dsn, conn_cls, cur_cls):
    async with await conn_cls.connect(dsn) as conn:
        assert conn.cursor_factory is cur_cls
        async with conn.cursor() as cur:
            assert isinstance(cur, cur_cls)
