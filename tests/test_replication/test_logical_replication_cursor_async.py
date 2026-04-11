from __future__ import annotations

import pytest

from psycopg import errors as e
from psycopg import sql
from psycopg.replication.replication_options import SnapshotOption

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


async def check_logical_stream_health(aconn, started_cur, table, timeout=0.5):
    await started_cur.send_feedback(request_reply=True)

    await started_cur.read_message(timeout=timeout)

    await aconn.execute(
        "SELECT pg_logical_emit_message(false, 'test_psycopg', 'SUBSCRIBED')"
    )

    await started_cur.read_message(return_keepalive_messages=False, timeout=timeout)

    while True:
        try:
            await started_cur.read_message(return_keepalive_messages=False, timeout=0.1)
        except e.ReadMessageTimeout:
            break

    await aconn.execute(f"INSERT INTO {table} (data) VALUES ('healthy?')")

    return await started_cur.read_message(
        return_keepalive_messages=False, timeout=timeout
    )


async def test_start_replication(
    aconn, alogical_conn, slot_name, test_table, publication
):
    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name)
        await cur.start_replication(
            slot_name,
            publication_names=publication,
            decoder=None,
            messages=True,
        )
        await check_logical_stream_health(aconn, cur, test_table)


async def test_start_replication_multiple_publications(
    aconn,
    alogical_conn,
    slot_name,
    test_table,
    publication,
    empty_publication,
    monkeypatch,
):
    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name)
        orig_execute = cur.execute
        statement: str

        async def mock_execute(*args, **kwargs):
            nonlocal statement
            statement = sql.as_string(args[0])
            return await orig_execute(*args, **kwargs)

        with monkeypatch.context() as m:
            m.setattr(cur, "execute", mock_execute)
            await cur.start_replication(
                slot_name,
                publication_names=[empty_publication, publication],
                decoder=None,
                messages=True,
            )
        assert (
            f'"publication_names" \'"{empty_publication}", "{publication}"\''
            in statement
        )
        await check_logical_stream_health(aconn, cur, test_table)


async def test_start_replication_string_lsn(
    aconn, alogical_conn, slot_name, test_table, publication
):
    """start_replication should accept a string LSN."""
    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name)
        await cur.start_replication(
            slot_name,
            start_lsn="0/0",
            publication_names=publication,
            decoder=None,
            messages=True,
        )
        await check_logical_stream_health(aconn, cur, test_table)


@pytest.mark.parametrize("option_value", [True, False])
@pytest.mark.parametrize(
    "option_name",
    [
        "include_xids",
        "include_timestamp",
        "force_binary",
        "skip_empty_xacts",
        "only_local",
        "include_rewrites",
        "stream_changes",
    ],
)
async def test_start_replication_text_decoder(
    aconn, alogical_conn, slot_name, test_table, publication, option_name, option_value
):
    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name, output_plugin="test_decoding")
        await cur.start_replication(
            slot_name, start_lsn="0/0", **{option_name: option_value}
        )
        begin = await check_logical_stream_health(aconn, cur, test_table)
        insert = await cur.read_message(timeout=1, return_keepalive_messages=False)
        commit = await cur.read_message(timeout=1, return_keepalive_messages=False)

        for msg in (begin, insert, commit):
            assert isinstance(msg.payload, str)

        assert "INSERT" in insert.payload
        assert test_table in insert.payload
        assert "'healthy?'" in insert.payload

        if option_name == "include_xids" and option_value is False:
            assert "BEGIN" == begin.payload
            assert "COMMIT" == commit.payload
        else:
            assert "BEGIN " in begin.payload
            assert "COMMIT " in commit.payload


@pytest.mark.pg(">=15")
async def test_create_slot_snapshot_use(alogical_conn, slot_name):
    async with alogical_conn.transaction():
        with pytest.raises(e.InternalError_):
            async with alogical_conn.cursor() as cur:
                *_, snapshot_name, _ = await cur.create_replication_slot(
                    slot_name, snapshot=SnapshotOption.USE
                )
    async with alogical_conn.transaction():
        # READ ONLY required on PostgreSQL 16+
        await alogical_conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        )
        async with alogical_conn.cursor() as cur:
            *_, snapshot_name, _ = await cur.create_replication_slot(
                slot_name, snapshot=SnapshotOption.USE
            )
    assert snapshot_name is None


@pytest.mark.pg(">=15")
async def test_create_slot_snapshot_export(aconn, alogical_conn, slot_name):
    async with alogical_conn.cursor() as cur:
        *_, snapshot_name, _ = await cur.create_replication_slot(
            slot_name, snapshot=SnapshotOption.EXPORT
        )
    assert snapshot_name is not None
    async with aconn.transaction():
        await aconn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        await aconn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_name}'")


@pytest.mark.pg(">=15")
async def test_create_slot_snapshot_nothing(alogical_conn, slot_name):
    async with alogical_conn.cursor() as cur:
        *_, snapshot_name, _ = await cur.create_replication_slot(
            slot_name, snapshot=SnapshotOption.NOTHING
        )
    assert snapshot_name is None


@pytest.mark.pg(">=17")
@pytest.mark.parametrize(
    "replication_slot_opts",
    [
        pytest.param(("failover",), id="failover", marks=[pytest.mark.pg("==17")]),
        pytest.param(
            ("failover", "two_phase"),
            id="two_phase-failover",
            marks=[pytest.mark.pg(">=18")],
        ),
    ],
)
async def test_alter_replication_slot(alogical_conn, slot_name, replication_slot_opts):
    async def get_options(cur):
        await cur.execute(
            f"SELECT {", ".join(replication_slot_opts)}"
            + " FROM pg_replication_slots WHERE slot_name = %s",
            [slot_name],
        )
        return await cur.fetchone()

    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(
            slot_name, **{opt: False for opt in replication_slot_opts}
        )
        for option in await get_options(cur):
            assert option is False
        await cur.alter_replication_slot(
            slot_name, **{opt: True for opt in replication_slot_opts}
        )
        for option in await get_options(cur):
            assert option is True


@pytest.mark.pg("<17")
async def test_alter_replication_slot_raises(alogical_conn, slot_name):
    async with alogical_conn.cursor() as cur:
        await cur.create_replication_slot(slot_name, two_phase=False)
        with pytest.raises(ValueError, match="alter_replication_slot()"):
            await cur.alter_replication_slot(slot_name, two_phase=True)
