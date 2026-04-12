from __future__ import annotations

from typing import Any

import pytest

from psycopg import AsyncConnection
from psycopg import errors as e
from psycopg import pq
from psycopg.replication import (
    AsyncLogicalReplicationConnection,
    AsyncLogicalReplicationCursor,
    AsyncPhysicalReplicationConnection,
)
from psycopg.replication.logical_output_plugins.decoder import (
    DispatchingDecoder,
)

from ..fix_db import maybe_trace
from .utils_async import get_restart_lsn


@pytest.fixture
def application_name():
    return "psycopg_testing"


@pytest.fixture
async def aconn(aconn):
    await aconn.set_autocommit(True)
    yield aconn


@pytest.fixture
async def alogical_conn(dsn, slot_name, application_name, tracefile, request):
    # NOTE: slot_name is a dependency so conn is closed before slot is cleaned up
    conn = await AsyncLogicalReplicationConnection.connect(
        dsn, autocommit=True, application_name=application_name
    )
    async with conn.cursor() as cur:
        await cur.execute("SHOW wal_level")
        assert (row := await cur.fetchone()) is not None
        wal_level = row[0]
    if wal_level != "logical":
        await conn.close()
        pytest.skip(
            "wal_level must be 'logical' to use logical replication,"
            + f" got '{wal_level}'"
        )
    with maybe_trace(conn.pgconn, tracefile, request.function):
        yield conn
    await conn.close()


@pytest.fixture
def client_encoding():
    return None


@pytest.fixture
async def aphysical_conn(
    dsn, slot_name, application_name, client_encoding, request, tracefile
):
    # NOTE: slot_name is a dependency so conn is closed before slot is cleaned up
    extra_connect_kwargs = {}
    if client_encoding is not None:
        extra_connect_kwargs["client_encoding"] = client_encoding
    conn = await AsyncPhysicalReplicationConnection.connect(
        dsn,
        autocommit=True,
        application_name=application_name,
        **extra_connect_kwargs,
    )
    with maybe_trace(conn.pgconn, tracefile, request.function):
        yield conn
    await conn.close()


@pytest.fixture()
async def aset_origin(aconn, origin):
    await aconn.execute("SELECT pg_replication_origin_session_setup(%s)", [origin])
    try:
        yield origin
    finally:
        try:
            await aconn.execute("SELECT pg_replication_origin_session_reset()")
        except e.ObjectNotInPrerequisiteState:
            pass


@pytest.fixture
def row_factory():
    return None


@pytest.fixture
def format():
    return pq.Format.TEXT


@pytest.fixture
def streaming():
    return None


@pytest.fixture
def two_phase():
    return None


@pytest.fixture
async def aphysical_started_cur(
    aphysical_conn,
    aconn,
    slot_name,
):
    async with aphysical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name, temporary=True, reserve_wal=True)
        restart_lsn = await get_restart_lsn(aconn, slot_name, cur)
        await cur.start_replication(slot_name, start_lsn=restart_lsn)
        yield cur


@pytest.fixture
def decoder(row_factory):
    decoder_kwargs = {}
    if row_factory is not None:
        decoder_kwargs["row_factory"] = row_factory

    return DispatchingDecoder(**decoder_kwargs)


@pytest.fixture
async def alogical_started_cur(
    alogical_conn,
    slot_name,
    publication,
    format,
    streaming,
    two_phase,
    decoder,
):
    start_replication_kwargs: dict[str, Any] = {}

    if format is pq.Format.BINARY:
        start_replication_kwargs["binary"] = True
    if streaming is not None:
        start_replication_kwargs["streaming"] = streaming
        if streaming == "parallel":
            start_replication_kwargs["proto_version"] = 4
    if two_phase is not None:
        start_replication_kwargs["two_phase"] = two_phase
        proto_version = start_replication_kwargs.get("proto_version", 2)
        start_replication_kwargs["proto_version"] = max(proto_version, 3)

    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name, temporary=True)

        if streaming is not None and streaming != "off":
            # determines when transactions start to be streamed
            await cur.execute("SET logical_decoding_work_mem = '64kB'")
        await cur.start_replication(
            slot_name,
            publication_names=publication,
            messages=True,
            decoder=decoder,
            **start_replication_kwargs,
        )
        yield cur


@pytest.fixture
def arepl_conn_cls():
    return AsyncLogicalReplicationConnection


@pytest.fixture
def arepl_cur_cls():
    return AsyncLogicalReplicationCursor


@pytest.fixture
async def arepl_cur(dsn, arepl_conn_cls, arepl_cur_cls, slot_name, tracefile, request):
    # NOTE: slot_name is a dependency so conn is closed before slot is cleaned up
    extra_connect_kwargs = {}
    if arepl_conn_cls is AsyncConnection:
        extra_connect_kwargs["replication"] = "database"
    conn = await arepl_conn_cls.connect(
        dsn, autocommit=True, cursor_factory=arepl_cur_cls, **extra_connect_kwargs
    )
    async with conn.cursor() as cur:
        with maybe_trace(conn.pgconn, tracefile, request.function):
            yield cur
    await conn.close()


LOGICAL_STARTED_CUR = "logical_started_cur"
PHYSICAL_STARTED_CUR = "physical_started_cur"

if True:  # ASYNC
    LOGICAL_STARTED_CUR = f"a{LOGICAL_STARTED_CUR}"
    PHYSICAL_STARTED_CUR = f"a{PHYSICAL_STARTED_CUR}"


@pytest.fixture(
    params=[
        pytest.param(LOGICAL_STARTED_CUR, id="logical"),
        pytest.param(PHYSICAL_STARTED_CUR, id="physical"),
    ]
)
def arepl_started_cur(request, decoder, slot_name):
    # NOTE: decoder is used by logical_started_cur
    # NOTE: slot_name is a dependency so conn is closed before slot is cleaned up
    cur = request.getfixturevalue(request.param)
    yield cur
