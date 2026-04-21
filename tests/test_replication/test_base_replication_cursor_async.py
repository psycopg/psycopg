"""
Tests for BaseLogicalReplicationCursor.

Tests connection setup, identify_system, show, timeline_history,
and drop_replication_slot for logical replication.
"""

from __future__ import annotations

import io
import json
import tarfile
from typing import Any
from unittest import mock

import pytest

import psycopg
from psycopg import errors as e
from psycopg import pq
from psycopg.rows import scalar_row
from psycopg.replication import (
    AsyncBaseReplicationCursor,
    AsyncLogicalReplicationConnection,
    AsyncLogicalReplicationCursor,
    AsyncPhysicalReplicationConnection,
    AsyncPhysicalReplicationCursor,
)
from psycopg.replication.base_backup_options import (
    CheckpointMode,
    ManifestOption,
)
from psycopg.replication.replication_options import ReplicationType
from psycopg.replication.base_backup_messages import (
    BackupData,
    BackupManifestStart,
    BackupNewArchive,
    BackupProgress,
)

from .data import assemble_example_manifest
from .params import parametrize_no_decoder, repl_class_param
from ..acompat import skip_sync
from .conftest import get_text_type
from .utils_async import (
    collect_xlogdata_messages,
    generate_table_traffic,
    get_replication_info,
    get_restart_lsn,
)

if True:  # ASYNC
    import asyncio

    pytestmark = [pytest.mark.anyio]


REPL_CONN_CLS = "repl_conn_cls"
REPL_CUR_CLS = "repl_cur_cls"

if True:  # ASYNC
    REPL_CONN_CLS = f"a{REPL_CONN_CLS}"
    REPL_CUR_CLS = f"a{REPL_CUR_CLS}"


parametrize_all_replication_cursors = pytest.mark.parametrize(
    REPL_CUR_CLS,
    [
        repl_class_param(AsyncBaseReplicationCursor),
        repl_class_param(AsyncLogicalReplicationCursor),
        repl_class_param(AsyncPhysicalReplicationCursor),
    ],
)
parametrize_all_connections = pytest.mark.parametrize(
    REPL_CONN_CLS,
    [
        repl_class_param(AsyncLogicalReplicationConnection),
        repl_class_param(AsyncPhysicalReplicationConnection),
        repl_class_param(psycopg.AsyncConnection),
    ],
)
parametrize_replication_connections = pytest.mark.parametrize(
    REPL_CONN_CLS,
    [
        repl_class_param(AsyncLogicalReplicationConnection),
        repl_class_param(AsyncPhysicalReplicationConnection),
    ],
)


def to_str(val):
    if isinstance(val, bytes):
        return str(val, encoding="ascii")
    return val


@parametrize_all_replication_cursors
@parametrize_all_connections
async def test_identify_system(arepl_cur):
    text_type = get_text_type(arepl_cur.connection)

    row = await arepl_cur.identify_system()
    assert row is not None
    # IDENTIFY_SYSTEM returns systemid, timeline, xlogpos, dbname
    assert len(row) == 4
    systemid, timeline, xlogpos, dbname = row
    assert isinstance(systemid, text_type)
    assert isinstance(timeline, int)
    assert timeline >= 1
    assert isinstance(xlogpos, text_type)
    assert "/" in to_str(xlogpos)  # LSN format like "0/1234"
    if text_type is str:
        assert isinstance(dbname, text_type)
    else:
        assert dbname is None


@parametrize_all_replication_cursors
@parametrize_all_connections
async def test_show(arepl_cur):
    text_type = get_text_type(arepl_cur.connection)

    row = await arepl_cur.show("wal_level")
    assert row is not None
    assert len(row) == 1
    wal_level = row[0]
    assert isinstance(wal_level, text_type)


@pytest.mark.pg(">=9.4")
@parametrize_all_replication_cursors
@parametrize_all_connections
async def test_timeline_history(arepl_cur):
    """TIMELINE_HISTORY for timeline 1 should work or raise UndefinedFile."""
    text_type = get_text_type(arepl_cur.connection)

    # timeline 1 history file might not exist; error is acceptable
    try:
        row = await arepl_cur.timeline_history(1)
        assert len(row) == 2
        filename, content = row
        assert isinstance(filename, text_type)
        assert isinstance(content, text_type)
    except e.UndefinedFile:
        pass  # This is expected if the history file doesn't exist


@pytest.mark.parametrize(
    "replication_type", [ReplicationType.LOGICAL, ReplicationType.PHYSICAL, None]
)
@parametrize_all_replication_cursors
@parametrize_all_connections
async def test_create_drop_replication_slot(
    arepl_cur, aconn, slot_name, replication_type
):
    text_type = get_text_type(arepl_cur.connection)

    try:
        row = await arepl_cur.create_replication_slot(
            slot_name, replication_type=replication_type
        )
    except e.ObjectNotInPrerequisiteState:
        if replication_type is not None:
            assert replication_type == ReplicationType.LOGICAL
        else:
            assert type(arepl_cur) is AsyncLogicalReplicationCursor
        assert type(arepl_cur.connection) is AsyncPhysicalReplicationConnection
        return
    except TypeError:
        assert replication_type is None
        assert type(arepl_cur) is AsyncBaseReplicationCursor
        return

    assert len(row) == 4
    for val in row:
        if val is not None:
            assert isinstance(val, text_type)
    rslot_name, consistent_point, snapshot_name, output_plugin = row

    assert to_str(rslot_name) == slot_name
    assert "/" in to_str(consistent_point)

    async with aconn.cursor(row_factory=scalar_row) as cur:
        await cur.execute(
            "SELECT slot_type from pg_replication_slots WHERE slot_name=%s",
            [slot_name],
        )
        slot_type = await cur.fetchone()

    if replication_type is None:
        match arepl_cur:
            case AsyncLogicalReplicationCursor():
                assert slot_type == "logical"
            case AsyncPhysicalReplicationCursor():
                assert slot_type == "physical"
            case AsyncBaseReplicationCursor():
                pytest.fail(
                    "AsyncBaseReplicationCursor.create_replication_slot()"
                    + " without 'replication_type' should raise TypeError"
                )
    else:
        assert slot_type == replication_type.lower()

    if slot_type == "logical":
        assert snapshot_name is not None
        assert to_str(output_plugin) == "pgoutput"
    else:
        assert snapshot_name is None
        assert output_plugin is None

    await arepl_cur.drop_replication_slot(slot_name)


@pytest.mark.parametrize(
    "replication_type", [ReplicationType.LOGICAL, ReplicationType.PHYSICAL]
)
async def test_create_replication_slot_temporary(
    arepl_cur, slot_name, replication_type
):
    await arepl_cur.create_replication_slot(
        slot_name, replication_type=replication_type, temporary=True
    )

    await arepl_cur.execute(
        "SELECT temporary FROM pg_replication_slots WHERE slot_name = %s",
        [slot_name],
    )
    temporary = (await arepl_cur.fetchone())[0]
    assert temporary is True


@pytest.mark.parametrize("replication_type", [ReplicationType.LOGICAL])
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_create_replication_slot_invalid_snapshot_option(
    arepl_cur, slot_name, replication_type
):
    with pytest.raises(ValueError, match="snapshot"):
        await arepl_cur.create_replication_slot(
            slot_name, replication_type=replication_type, snapshot="invalid_option"
        )


@pytest.mark.pg("<15")
@pytest.mark.parametrize("replication_type", [ReplicationType.LOGICAL])
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_create_slot_failover_raises(arepl_cur, slot_name, replication_type):
    """On PG < 15, failover=True should raise ValueError (no support in old syntax)."""
    with pytest.raises(ValueError, match="FAILOVER"):
        await arepl_cur.create_replication_slot(
            slot_name, replication_type=replication_type, failover=True
        )


@pytest.mark.parametrize("replication_type", [ReplicationType.PHYSICAL])
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_create_physical_slot_reserve_wal(
    arepl_cur, aconn, slot_name, replication_type
):
    await arepl_cur.create_replication_slot(
        slot_name, replication_type=replication_type, reserve_wal=False
    )
    assert (await get_restart_lsn(aconn, slot_name)) is None
    await arepl_cur.drop_replication_slot(slot_name)

    await arepl_cur.create_replication_slot(
        slot_name, replication_type=replication_type, reserve_wal=True
    )

    assert "/" in to_str(await get_restart_lsn(aconn, slot_name))


@parametrize_no_decoder
async def test_start_replication_cancel(arepl_started_cur):
    assert (
        arepl_started_cur.connection.info.transaction_status
        == pq.TransactionStatus.ACTIVE
    )

    await arepl_started_cur.connection.cancel_safe()

    with pytest.raises(e.QueryCanceled):
        while True:
            await arepl_started_cur.send_feedback(request_reply=True)
            await arepl_started_cur.read_message()

    assert (
        arepl_started_cur.connection.info.transaction_status
        == pq.TransactionStatus.IDLE
    )


@skip_sync
async def test_read_message_asyncio_cancel(arepl_started_cur):
    assert (
        arepl_started_cur.connection.info.transaction_status
        == pq.TransactionStatus.ACTIVE
    )
    try:
        while True:
            await arepl_started_cur.read_message(timeout=0.1)
    except e.ReadMessageTimeout:
        pass

    task = asyncio.create_task(arepl_started_cur.read_message())
    await asyncio.sleep(0.01)  # give task a chance to enter wait loop
    cancelled = task.cancel()

    assert cancelled

    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        pytest.fail("Didn't get cancelled error")

    # Verify the START_REPLICATION command was not cancelled
    for _ in range(3):
        await arepl_started_cur.send_feedback(request_reply=True)
        await arepl_started_cur.read_message()

    assert (
        arepl_started_cur.connection.info.transaction_status
        == pq.TransactionStatus.ACTIVE
    )


@pytest.mark.slow
async def test_read_message_keyboard_interrupt(arepl_started_cur):

    assert (
        arepl_started_cur.connection.info.transaction_status
        == pq.TransactionStatus.ACTIVE
    )

    original_read_gen = arepl_started_cur._read_gen

    def interrupting_read_gen(*args, **kwargs):
        raise KeyboardInterrupt()
        yield from original_read_gen(*args, **kwargs)

    klass = "BaseReplicationCursor"

    if True:  # ASYNC
        klass = f"Async{klass}"

    with mock.patch(
        f"psycopg.replication.{klass}._read_gen",
        side_effect=interrupting_read_gen,
    ) as mock_method:
        assert arepl_started_cur._read_gen is mock_method
        try:

            await arepl_started_cur.read_message()
        except KeyboardInterrupt:
            pass
        else:
            pytest.fail("Didn't get keyboard interrupt")

    # Verify the START_REPLICATION command was not cancelled
    for _ in range(3):
        await arepl_started_cur.send_feedback(request_reply=True)
        await arepl_started_cur.read_message()

    assert (
        arepl_started_cur.connection.info.transaction_status
        == pq.TransactionStatus.ACTIVE
    )


@parametrize_no_decoder
async def test_read_message_auto_flushed(aconn, arepl_started_cur, test_table):
    async with generate_table_traffic(aconn, test_table):
        msg = (await collect_xlogdata_messages(arepl_started_cur, n=10))[-1]
        if isinstance(arepl_started_cur, AsyncLogicalReplicationCursor):
            # saw some failures here on old versions due to extraneous begin/commit
            # pairs: bumped `n` and added the following.
            assert msg.payload[0].to_bytes(1, byteorder="big") == b"I"

        assert arepl_started_cur.last_flushed_lsn < msg.data_start
        assert arepl_started_cur._last_received_lsn == msg.data_start

        msg_flush = (
            await collect_xlogdata_messages(arepl_started_cur, n=1, auto_flushed=True)
        )[0]

    assert msg.data_start < msg_flush.data_start
    assert arepl_started_cur.last_applied_lsn < msg_flush.data_start
    assert arepl_started_cur.last_flushed_lsn == msg_flush.data_start
    assert arepl_started_cur._last_received_lsn == msg_flush.data_start


@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_read_message_no_start(arepl_cur):
    # DISCUSS: should we raise a more informative error here?
    with pytest.raises(e.OperationalError, match="no COPY in progress"):
        await arepl_cur.read_message()


@parametrize_no_decoder
async def test_send_feedback_implicit(
    aconn, arepl_started_cur, test_table, application_name
):
    async with generate_table_traffic(aconn, test_table):
        msgs = await collect_xlogdata_messages(arepl_started_cur, n=5)
    msgs = msgs[2:]  # Need to skip the relation msg for logical

    applied_lsn, flushed_lsn, written_lsn = (msg.data_start for msg in msgs)
    arepl_started_cur.last_flushed_lsn = flushed_lsn
    arepl_started_cur.last_applied_lsn = applied_lsn
    status_update = await arepl_started_cur.send_feedback()
    flush_lsn, write_lsn, replay_lsn, server_reply_time = await get_replication_info(
        aconn,
        application_name,
        ["flush_lsn", "write_lsn", "replay_lsn", "reply_time"],
    )
    assert applied_lsn == replay_lsn
    assert flushed_lsn == flush_lsn
    assert arepl_started_cur._last_received_lsn == write_lsn
    assert written_lsn == write_lsn
    assert status_update.send_time == server_reply_time


@parametrize_no_decoder
async def test_send_feedback_explicit(
    aconn, arepl_started_cur, test_table, application_name
):
    async with generate_table_traffic(aconn, test_table):
        msgs = await collect_xlogdata_messages(arepl_started_cur, n=6)
    msgs = msgs[2:]  # Need to skip the relation msg for logical

    applied_lsn, flushed_lsn, written_lsn, _ = (msg.data_start for msg in msgs)
    status_update = await arepl_started_cur.send_feedback(
        applied_lsn=applied_lsn,
        flushed_lsn=flushed_lsn,
        received_lsn=written_lsn,
    )
    assert arepl_started_cur.last_flushed_lsn == flushed_lsn
    assert arepl_started_cur.last_applied_lsn == applied_lsn
    server_flush_lsn, write_lsn, replay_lsn, server_reply_time = (
        await get_replication_info(
            aconn,
            application_name,
            ["flush_lsn", "write_lsn", "replay_lsn", "reply_time"],
        )
    )
    assert applied_lsn == replay_lsn
    assert flushed_lsn == server_flush_lsn
    assert arepl_started_cur._last_received_lsn != write_lsn
    assert written_lsn == write_lsn
    assert status_update.send_time == server_reply_time


@parametrize_no_decoder
async def test_send_feedback_request_reply(arepl_started_cur):
    await arepl_started_cur.send_feedback(request_reply=True)
    msg = await arepl_started_cur.read_message()
    status_update = await arepl_started_cur.send_feedback(request_reply=True)
    time1 = msg.send_time_microseconds_since_2000
    msg = await arepl_started_cur.read_message()
    time2 = msg.send_time_microseconds_since_2000
    assert status_update.reply_asap is True
    assert time2 - time1 < 200_000  # 200 milliseconds


async def collect_backup(cur: Any) -> dict[str, io.BytesIO]:
    """
    Drive read_backup_message() to completion.

    Returns a dict mapping archive_name -> raw tar bytes (concatenated BackupData
    chunks for that archive), plus the key ``"__manifest__"`` for manifest data
    if a manifest was included.
    """
    archives: dict[str, io.BytesIO] = {}
    current_archive: str = ""

    while (msg := await cur.read_backup_message()) is not None:
        if isinstance(msg, BackupNewArchive):
            current_archive = msg.archive_name
            archives[current_archive] = io.BytesIO()
        elif isinstance(msg, BackupManifestStart):
            current_archive = "__manifest__"
            archives[current_archive] = io.BytesIO()
        elif isinstance(msg, BackupData):
            archives[current_archive].write(msg.data)
        elif isinstance(msg, BackupProgress):
            pass  # Progress messages are informational
        else:
            assert len(msg) == 2, "Expected a tuple containing ('end_lsn', 'timeline')"
            end_lsn, timeline = msg
            assert isinstance(end_lsn, bytes)
            assert b"/" in end_lsn
            assert isinstance(timeline, int)
    for archive in archives.values():
        archive.seek(0)
    return archives


def assert_valid_tar(
    data: io.BytesIO, archive_name: str, expected_names: list[str] = []
) -> None:
    try:
        with tarfile.open(fileobj=data, mode="r:") as tf:
            names = tf.getnames()
            assert len(names) > 0, f"Archive {archive_name!r} contains no members"
            for name in expected_names:
                assert name in names, f"{name} not found in {archive_name} members"
    except tarfile.TarError as exc:
        pytest.fail(f"Archive {archive_name!r} is not a valid tar file: {exc}")


@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_read_backup_message_no_start(arepl_cur):
    # DISCUSS: should we raise an error here?
    assert (await arepl_cur.read_backup_message()) is None


@pytest.mark.pg(">=15")
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_base_backup_cancel(arepl_cur):
    await arepl_cur.start_base_backup(
        label="psycopg_test_cancel",
        checkpoint=CheckpointMode.FAST,
    )

    assert arepl_cur.connection.info.transaction_status == pq.TransactionStatus.ACTIVE

    await arepl_cur.connection.cancel_safe()

    with pytest.raises(e.QueryCanceled):
        while True:
            await arepl_cur.read_backup_message()

    assert arepl_cur.connection.info.transaction_status == pq.TransactionStatus.IDLE


@pytest.mark.pg(">=15")
@skip_sync
@pytest.mark.slow
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_read_backup_message_asyncio_cancel(arepl_cur):
    await arepl_cur.start_base_backup(
        label="psycopg_test_cancel",
        checkpoint=CheckpointMode.FAST,
    )
    assert arepl_cur.connection.info.transaction_status == pq.TransactionStatus.ACTIVE

    try:
        # Read any quickly available messages
        while True:
            msg = await arepl_cur.read_backup_message(timeout=0.0000001)
            if msg is None:
                pytest.fail(reason="Didn't get timeout. Test needs tuning.")
    except e.ReadMessageTimeout:
        pass

    task = asyncio.create_task(arepl_cur.read_backup_message())
    await asyncio.sleep(0.00000001)  # give task a chance to enter wait loop
    cancelled = task.cancel()

    assert cancelled

    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        pytest.fail("Didn't get cancelled error")

    # Verify the BASE_BACKUP command was not cancelled
    assert arepl_cur.connection.info.transaction_status == pq.TransactionStatus.ACTIVE

    # There's likely only one message left, but cycle through the remainder
    # to ensure we aren't cancelled just in case.
    while (await arepl_cur.read_backup_message()) is not None:
        pass


@pytest.mark.pg(">=15")
@pytest.mark.slow
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_read_backup_message_keyboard_interrupt(arepl_cur):
    await arepl_cur.start_base_backup(
        label="psycopg_test_cancel",
        checkpoint=CheckpointMode.FAST,
    )
    assert arepl_cur.connection.info.transaction_status == pq.TransactionStatus.ACTIVE

    original_read_backup_gen = arepl_cur._read_backup_gen

    def interrupting_read_backup_gen(*args, **kwargs):
        raise KeyboardInterrupt()
        yield from original_read_backup_gen(*args, **kwargs)

    klass = "BaseReplicationCursor"

    if True:  # ASYNC
        klass = f"Async{klass}"

    with mock.patch(
        f"psycopg.replication.{klass}._read_backup_gen",
        side_effect=interrupting_read_backup_gen,
    ) as mock_method:
        assert arepl_cur._read_backup_gen is mock_method
        try:

            await arepl_cur.read_backup_message()
        except KeyboardInterrupt:
            pass
        else:
            pytest.fail("Didn't get keyboard interrupt")

    # Verify the BASE_BACKUP command was not cancelled
    for _ in range(300):
        await arepl_cur.read_backup_message()

    assert arepl_cur.connection.info.transaction_status == pq.TransactionStatus.ACTIVE

    # Takes some number of messages to cancel
    # read the remainder in case 300 wasn't enough.
    while (await arepl_cur.read_backup_message()) is not None:
        pass


@parametrize_all_replication_cursors
@parametrize_all_connections
async def test_base_backup_returns_start_position(arepl_cur):
    """start_base_backup should return (start_lsn_row, tablespace_list)."""
    text_type = get_text_type(arepl_cur.connection)

    start_pos, tablespaces = await arepl_cur.start_base_backup(
        label="psycopg_test",
        checkpoint=CheckpointMode.FAST,
        progress=True,
    )
    assert start_pos is not None
    assert len(start_pos) == 2
    lsn, timeline = start_pos
    assert isinstance(lsn, text_type)
    assert "/" in to_str(lsn)
    assert isinstance(timeline, int)
    assert timeline >= 1
    assert isinstance(tablespaces, list)
    assert len(tablespaces) >= 1
    for spcoid, spclocation, size in tablespaces:
        assert isinstance(size, int)
        if spcoid is None:
            assert spclocation is None
        else:
            assert isinstance(spcoid, int)
            assert isinstance(spclocation, text_type)


@pytest.mark.slow
@pytest.mark.pg(">=15")
async def test_base_backup_produces_valid_tar(aphysical_conn):
    """Base backup data should be parseable as a tar archive."""
    async with aphysical_conn.cursor() as cur:
        await cur.start_base_backup(
            label="psycopg_test_tar",
            checkpoint=CheckpointMode.FAST,
        )
        archives = await collect_backup(cur)

    assert len(archives) > 0, "Expected at least one archive"
    assert "__manifest__" not in archives
    assert "base.tar" in archives, "Expected base.tar in archives, got: " + str(
        list(archives.keys())
    )
    for archive_name, data in archives.items():
        assert_valid_tar(
            data,
            archive_name,
            expected_names=["PG_VERSION"] if archive_name == "base.tar" else [],
        )


@pytest.mark.slow
@pytest.mark.pg(">=15")
async def test_base_backup_progress_messages(aphysical_conn):
    """Enabling progress should produce BackupProgress messages."""
    progress_seen = False

    async with aphysical_conn.cursor() as cur:
        await cur.start_base_backup(
            label="psycopg_test_progress",
            checkpoint=CheckpointMode.FAST,
            progress=True,
        )
        # Manually drain so we can inspect message types
        while (msg := await cur.read_backup_message()) is not None:
            if isinstance(msg, BackupProgress):
                progress_seen = True
                assert isinstance(msg.total_bytes, int)
                assert msg.total_bytes > 0
                break

    assert progress_seen, "Expected at least one BackupProgress message"


@pytest.mark.slow
@pytest.mark.pg(">=15")
async def test_base_backup_with_manifest(aphysical_conn):
    """Requesting a manifest should produce BackupManifestStart + data."""
    manifest_seen = False
    manifest_data = b""

    async with aphysical_conn.cursor() as cur:
        await cur.start_base_backup(
            label="psycopg_test_manifest",
            checkpoint=CheckpointMode.FAST,
            manifest=ManifestOption.YES,
        )
        while (msg := await cur.read_backup_message()) is not None:
            if isinstance(msg, BackupManifestStart):
                manifest_seen = True
            elif isinstance(msg, BackupData) and manifest_seen:
                manifest_data += bytes(msg.data)
            elif manifest_seen and not isinstance(msg, (BackupData, BackupProgress)):
                # Got the whole manifest, no need to continue
                break
    assert manifest_seen, "Expected a BackupManifestStart message"
    assert len(manifest_data) > 0, "Expected non-empty manifest data"
    # Manifest is JSON
    manifest_obj = json.loads(manifest_data.decode())
    assert "PostgreSQL-Backup-Manifest-Version" in manifest_obj


@pytest.mark.pg(">=17")
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_upload_manifest(arepl_cur):
    system_identifier, *_ = await arepl_cur.identify_system()
    if isinstance(system_identifier, str):
        system_identifier = system_identifier.encode()
    await arepl_cur.upload_manifest([assemble_example_manifest(system_identifier)])
    assert arepl_cur.pgresult.status == pq.ExecStatus.COMMAND_OK


@pytest.mark.pg(">=17")
@parametrize_all_replication_cursors
@parametrize_replication_connections
async def test_upload_manifest_invalid(arepl_cur):
    with pytest.raises(e.InternalError_, match="could not parse backup manifest"):
        await arepl_cur.upload_manifest([b"test", b"invalid"])


@pytest.mark.slow
@pytest.mark.pg(">=17")
async def test_upload_manifest_and_incremental_backup(aphysical_conn):
    """
    upload_manifest followed by start_base_backup(incremental=True) should
    produce a valid incremental backup.

    1. Take a full backup with manifest=YES to get the manifest bytes.
    2. upload_manifest() the manifest bytes.
    3. start_base_backup(incremental=True) and verify we get tar data.
    """
    # --- Step 1: Full backup to obtain a manifest ---
    manifest_data: list[bytes] = []
    async with aphysical_conn.cursor() as cur:
        await cur.start_base_backup(
            label="psycopg_test_full",
            checkpoint=CheckpointMode.FAST,
            manifest=ManifestOption.YES,
        )
        in_manifest = False
        while (msg := await cur.read_backup_message()) is not None:
            if isinstance(msg, BackupManifestStart):
                in_manifest = True
            elif isinstance(msg, BackupData) and in_manifest:
                manifest_data.append(bytes(msg.data))

    assert len(manifest_data) > 0, "Full backup produced no manifest data"

    # --- Step 2: Upload manifest and take incremental backup ---
    async with aphysical_conn.cursor() as cur:
        await cur.upload_manifest(manifest_data)
        try:
            await cur.start_base_backup(
                label="psycopg_test_incremental",
                checkpoint=CheckpointMode.FAST,
                incremental=True,
            )
            archives = await collect_backup(cur)
        except e.ObjectNotInPrerequisiteState as err:
            pytest.skip(str(err))

    assert len(archives) > 0, "Incremental backup produced no archives"
    for archive_name, data in archives.items():
        if archive_name == "__manifest__":
            continue
        assert_valid_tar(data, archive_name)
