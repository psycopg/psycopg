from __future__ import annotations

from typing import Any
from contextlib import asynccontextmanager

import pytest

import psycopg
from psycopg import errors as e
from psycopg.rows import scalar_row
from psycopg.replication.replication_messages import XLogDataMessage
from psycopg.replication.logical_output_plugins.pgoutput.pgoutput_messages import (
    PgOutputMessage,
)

from ..acompat import gather, spawn


async def collect_xlogdata_messages(
    cur: Any, until: type[PgOutputMessage] | None = None, n: int = 400, **kwargs: Any
) -> list[XLogDataMessage[Any]]:
    """
    Consume the replication stream until n XLogData messages have been
    collected or message payload is of type `until`
    Returns the list of XLogDataMessage objects.
    """
    kwargs["return_keepalive_messages"] = False
    messages: list[XLogDataMessage[Any]] = []
    while len(messages) < n:
        msg = await cur.read_message(**kwargs, timeout=2)
        messages.append(msg)
        if until is not None and isinstance(msg.payload, until):
            break
    return messages


async def replica_identity_full(aconn, table, logical_cur):
    await aconn.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    if aconn.info.server_version < 150000:
        # consume empty begin/commit pairs generated on old versions
        await collect_xlogdata_messages(logical_cur, n=2)


async def replica_identity_default(aconn, table, logical_cur):
    pass


async def replica_identity_index(aconn, table, logical_cur):
    index_name = f"data_uniq_{table}"
    async with aconn.transaction():
        await aconn.execute(f"CREATE UNIQUE INDEX {index_name} ON {table} (data)")
        await aconn.execute(
            f"ALTER TABLE {table} REPLICA IDENTITY USING index {index_name}"
        )
    if aconn.info.server_version < 150000:
        # consume empty begin/commit pairs generated on old versions
        await collect_xlogdata_messages(logical_cur, n=2)


async def insert_data(aconn, table, value, return_xid=False, returning=None):
    async with aconn.cursor() as cur:
        statement = f"INSERT INTO {table} (data) VALUES (%s)"
        if return_xid:
            async with aconn.transaction():
                await cur.execute(statement, [value])
                await cur.execute("SELECT pg_current_xact_id()")
                (xid,) = await cur.fetchone()
                return int(xid)
        else:
            if returning:
                statement += f" RETURNING {returning}"
            await cur.execute(statement, [value])
            if returning:
                return (await cur.fetchone())[0]


async def execute_streaming_insert(has_execute, table, extra_data=None, n=320):
    columns = "data"
    extra_vals = ""
    if extra_data is not None:
        columns = f"{columns}, {', '.join(extra_data.keys())}"
        extra_vals = ", " + ", ".join(extra_data.values())

    await has_execute.execute(
        f"INSERT INTO {table} ({columns})"
        + " SELECT i ||"
        + " ': this is a pretty long string for testing streaming transactions"
        + f" :' || i{extra_vals}"
        + f" FROM generate_series(1,{n}) AS t(i);"
    )


async def get_xid(cur):
    await cur.execute("SELECT pg_current_xact_id()")
    (xid,) = await cur.fetchone()

    return int(xid)


@asynccontextmanager
async def start_streaming_insert(
    aconn, table, exec_first=None, extra_data=None, n=320, rollback=False
):
    async with aconn.cursor() as cur:
        async with aconn.transaction():
            if exec_first is not None:
                await exec_first()
            await execute_streaming_insert(aconn, table, extra_data=extra_data, n=n)

            yield await get_xid(cur)

            if rollback:
                raise psycopg.Rollback()


async def streaming_insert(aconn, table, exec_first=None):
    async with start_streaming_insert(aconn, table, exec_first) as xid:
        return xid


@asynccontextmanager
async def two_phase_insert(aconn, xname, table, streaming, rollback=False):
    await aconn.set_autocommit(False)

    # DISCUSS: test hangs if `xname`` already exists; shouldn't psycopg handle this?
    await aconn.tpc_begin(xname)
    async with aconn.cursor() as cur:
        xid = await get_xid(cur)
    if streaming == "on":
        await execute_streaming_insert(aconn, table)
    else:
        await insert_data(aconn, table, "two phase")

    try:
        await aconn.tpc_prepare()
    except e.NotSupportedError as err:
        pytest.skip(str(err))

    try:
        yield xid
    finally:
        if rollback:
            await aconn.tpc_rollback()
        else:
            await aconn.tpc_commit()


@asynccontextmanager
async def generate_table_traffic(has_execute, table, n=100):
    tasks = []
    for _ in range(n // 10):
        tasks.append(
            spawn(
                has_execute.execute,
                [
                    f"INSERT INTO {table} (data) SELECT md5(random()::text)"
                    + "FROM generate_series(1, 10)"
                ],
            )
        )
    try:
        yield
    finally:
        await gather(*tasks)


async def get_last_flushed_lsn(physical_cur):
    _, _, lsn, _ = await physical_cur.identify_system()
    if isinstance(lsn, bytes):
        lsn = lsn.decode("ascii")
    return lsn


async def get_replication_info(aconn, application_name, fields):
    if isinstance(fields, str):
        fields = [fields]
    fields_str = ", ".join(
        (f"{f} - '0/0'" if f.endswith("_lsn") else f) for f in fields
    )
    async with aconn.cursor(binary=True) as cur:
        # cur.adapters.register_loader(
        #     "timestamptz", psycopg.types.numeric.Int8BinaryLoader
        # )
        await cur.execute(
            f"SELECT {fields_str} FROM"
            + " pg_stat_replication WHERE application_name=%s",
            [application_name],
        )
        result = await cur.fetchone()
        if len(fields) == 1:
            result = result[0]
        return result


async def get_restart_lsn(aconn, slot_name, physical_cur=None):
    if physical_cur is None or aconn.info.server_version < 150000:
        async with aconn.cursor(row_factory=scalar_row) as cur:
            await cur.execute(
                "SELECT restart_lsn from pg_replication_slots WHERE slot_name = %s",
                [slot_name],
            )
            restart_lsn = await cur.fetchone()
    else:
        _, restart_lsn, _ = await physical_cur.read_replication_slot(slot_name)
        if isinstance(restart_lsn, bytes):
            restart_lsn = restart_lsn.decode("ascii")
    return restart_lsn
