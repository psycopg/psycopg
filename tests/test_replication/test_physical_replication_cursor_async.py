from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

import psycopg
from psycopg.replication.replication_messages import (
    PrimaryKeepaliveMessage,
    XLogDataMessage,
)

from .utils_async import get_last_flushed_lsn, get_restart_lsn

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


@asynccontextmanager
async def generate_traffic(aconn, n=1):
    async with aconn.cursor() as cur:
        await cur.execute(
            "CREATE TABLE IF NOT EXISTS traffic (id BIGSERIAL PRIMARY KEY, data TEXT);"
        )
        await cur.execute(
            "INSERT INTO traffic (data)"
            + " SELECT md5(random()::text)"
            + f" FROM generate_series(1, {n}) AS s(i);"
        )

        try:
            yield
        finally:
            await cur.execute("DROP TABLE traffic;")


async def test_start_replication_slotless(aconn, aphysical_conn):
    async with aphysical_conn.cursor() as cur:
        await cur.start_replication(start_lsn=await get_last_flushed_lsn(cur))
        async with generate_traffic(aconn):
            msg = await cur.read_message()
    assert isinstance(msg, (XLogDataMessage, PrimaryKeepaliveMessage))


# DISCUSS: should we set client_encoding by default in PhysicalReplicationConnection?
# The bytes behaviour of SQL-ASCII is probably unexpected for users of psycopg
@pytest.mark.parametrize("client_encoding", ["utf-8"])
async def test_start_replication_with_slot(aconn, aphysical_conn, slot_name):
    async with aphysical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name, reserve_wal=True)
        restart_lsn = await get_restart_lsn(aconn, slot_name, cur)

        await cur.start_replication(slot_name=slot_name, start_lsn=restart_lsn)
        async with generate_traffic(aconn):
            msg = await cur.read_message()
    assert isinstance(msg, (XLogDataMessage, PrimaryKeepaliveMessage))


async def test_start_replication_missing_wal_raises(aphysical_conn):
    async with aphysical_conn.cursor() as cur:
        await cur.start_replication(start_lsn="0/0")
        with pytest.raises(psycopg.errors.UndefinedFile):
            await cur.read_message()


@pytest.mark.pg(">=15")
async def test_read_replication_slot_dne(aphysical_conn, slot_name):
    async with aphysical_conn.cursor() as cur:
        slot_type, restart_lsn, restart_tli = await cur.read_replication_slot(slot_name)

    assert slot_type is None
    assert restart_lsn is None
    assert restart_tli is None


@pytest.mark.pg(">=15")
async def test_read_replication_slot(aphysical_conn, slot_name):
    async with aphysical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name)

        slot_type, restart_lsn, restart_tli = await cur.read_replication_slot(slot_name)

    assert slot_type == b"physical"
    # haven't passed reserve_wal
    assert restart_lsn is None
    assert restart_tli is None


@pytest.mark.pg("<15")
async def test_read_replication_slot_raises(aphysical_conn, slot_name):
    async with aphysical_conn.cursor() as cur:
        with pytest.raises(ValueError, match="read_replication_slot()"):
            await cur.read_replication_slot(slot_name)
