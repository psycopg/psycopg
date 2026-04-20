from __future__ import annotations

from collections import defaultdict

import pytest

import psycopg
from psycopg import pq
from psycopg.rows import scalar_row
from psycopg.types import composite
from psycopg.postgres import types
from psycopg.replication.replication_options import ReplicaIdentity
from psycopg.replication.logical_output_plugins.logical_rows import (
    RowValue,
    args_row,
    class_row,
    dict_row,
    kwargs_row,
    namedtuple_row,
    tuple_row,
)
from psycopg.replication.logical_output_plugins.pgoutput.pgoutput_messages import (
    BeginMessage,
    CommitMessage,
    DeleteMessage,
    EmitMessage,
    InsertMessage,
    OriginMessage,
    RelationMessage,
    StreamAbortMessage,
    StreamCommitMessage,
    StreamStartMessage,
    StreamStopMessage,
    TruncateMessage,
    TypeMessage,
    UpdateMessage,
)

from .params import format_param, oname_param, stream_param
from ..test_adapt import make_bin_loader, make_loader
from .utils_async import (
    collect_xlogdata_messages,
    insert_data,
    replica_identity_default,
    replica_identity_full,
    replica_identity_index,
    start_streaming_insert,
    streaming_insert,
)

if True:  # ASYNC
    pytestmark = [pytest.mark.anyio]


async def test_pgoutput_begin_commit_messages(
    alogical_started_cur,
    aconn,
    test_table,
):
    xid = await insert_data(aconn, test_table, "insert_test", return_xid=True)

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=CommitMessage
    )
    begin, _, _, commit = (msg.payload for msg in xlog_msgs)

    assert isinstance(begin, BeginMessage)
    assert isinstance(commit, CommitMessage)

    assert begin.xid == xid
    assert commit.commit_ts == begin.commit_ts
    assert commit.commit_lsn == begin.final_lsn


async def test_pgoutput_type_message(
    dsn, alogical_started_cur, aconn, test_table, smallpoint_type
):
    await register_composite_types(aconn, alogical_started_cur, smallpoint_type)
    await aconn.execute(
        f"ALTER TABLE {test_table} ADD COLUMN location {smallpoint_type}"
    )
    await insert_data(aconn, test_table, "test_type")

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=RelationMessage
    )

    type_, relation = (msg.payload for msg in xlog_msgs[-2:])

    assert type_.name == smallpoint_type
    assert type_.type_id == relation.columns[-1].type_id
    assert type_.namespace == "public"
    assert type_.xid is None


@pytest.mark.parametrize("streaming", [stream_param("on")])
async def test_pgoutput_streamstart_streamcommit_messages(
    alogical_started_cur,
    aconn,
    test_table,
):
    xid = await streaming_insert(aconn, test_table)

    # start, relation, stop, commit + 320 inserts
    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=StreamCommitMessage
    )
    stream_start, stream_stop, stream_commit = (
        msg.payload for msg in (xlog_msgs[0], xlog_msgs[-2], xlog_msgs[-1])
    )

    assert isinstance(stream_start, StreamStartMessage)
    assert isinstance(stream_commit, StreamCommitMessage)
    assert isinstance(stream_stop, StreamStopMessage)

    assert xid is not None
    assert stream_start.xid == xid
    assert stream_commit.xid == xid
    assert stream_start.first_segment is True


@pytest.mark.parametrize("streaming", [stream_param("on"), stream_param("parallel")])
async def test_pgoutput_streamabort_message(
    dsn,
    alogical_started_cur,
    aconn,
    test_table,
    streaming,
):
    async with start_streaming_insert(aconn, test_table, rollback=True) as xid:
        # start a new transaction to break up the stream segments
        # Otherwise, the Rollback causes the stream transaction to
        # not be delivered on PG 18+ as it knows it's empty.
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True
        ) as aconn2:
            await insert_data(aconn2, test_table, "new_transaction")

        # start, relation, 320 inserts, stop, begin, relation, insert, commit
        await collect_xlogdata_messages(alogical_started_cur, until=CommitMessage)

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=1)
    stream_abort = xlog_msgs[-1].payload

    assert isinstance(stream_abort, StreamAbortMessage)

    assert stream_abort.xid == xid
    assert stream_abort.subxid == xid
    if streaming == "parallel":
        assert stream_abort.abort_ts is not None
        assert stream_abort.abort_lsn is not None
    else:
        assert stream_abort.abort_ts is None
        assert stream_abort.abort_lsn is None


@pytest.mark.parametrize("streaming", [stream_param("on")])
@pytest.mark.parametrize("row_factory", [oname_param(namedtuple_row)])
async def test_pgoutput_streaming_xact_relations_subxact(
    dsn, alogical_started_cur, aconn, test_table
):
    decoder = alogical_started_cur.decode_xlogdata.get_real_decoder()

    async with start_streaming_insert(aconn, test_table) as xid:
        aconn2 = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            # force a flush of the first stream segment
            await insert_data(aconn2, test_table, "force_flush")
        finally:
            await aconn2.close()

        # start, relation, 320 inserts, stop, begin, relation, insert, commit
        xlog_msgs = await collect_xlogdata_messages(
            alogical_started_cur, until=CommitMessage
        )
        relation1, insert1 = (msg.payload for msg in xlog_msgs[1:3])

        assert decoder.relations_by_xid[xid][relation1.relation_id] is relation1

        async with aconn.transaction():  # sub xact
            # lower bound: next id after the unrelated flushing xact above
            lower_subxid = xid + 2
            await aconn.execute(f"ALTER TABLE {test_table} ADD COLUMN extra bool")
            await aconn.execute(
                f"INSERT INTO {test_table} (data, extra) VALUES"
                + " ('xact_relation_test', true)"
            )

    # start, relation, insert
    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=InsertMessage
    )
    relation2, insert2 = (msg.payload for msg in xlog_msgs[-2:])

    assert decoder.relations_by_xid[xid][relation1.relation_id] is relation2
    assert len(decoder.relations) == 1
    assert decoder.relations[relation1.relation_id].xid is None
    assert insert1.xid == xid
    assert not hasattr(insert1, "extra")
    assert insert2.xid >= lower_subxid
    assert insert2.new_tuple.extra is True
    assert insert2.new_tuple.data == "xact_relation_test"

    # stop, commit
    await collect_xlogdata_messages(alogical_started_cur, n=2)
    assert not decoder.relations_by_xid

    # NOTE: Probably don't need to test this, but it's worth seeing that
    # we don't blow up, even if it is PG behaviour.
    await aconn.execute(
        f"INSERT INTO {test_table} (data, extra) VALUES ('post_xact_insert', false)"
    )
    xlog_msgs = (await collect_xlogdata_messages(alogical_started_cur, n=3))[1:3]
    relation, insert = (msg.payload for msg in xlog_msgs)
    assert insert.new_tuple.extra is False
    assert decoder.relations[relation1.relation_id] is relation


@pytest.mark.parametrize("streaming", [stream_param("on")])
@pytest.mark.parametrize("row_factory", [oname_param(namedtuple_row)])
async def test_pgoutput_streaming_xact_relations_subxact_rollback(
    dsn, alogical_started_cur, aconn, test_table
):
    decoder = alogical_started_cur.decode_xlogdata.get_real_decoder()

    async with start_streaming_insert(aconn, test_table) as xid:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True
        ) as aconn2:
            # force a flush of the first stream segment
            await aconn2.execute(
                "SELECT pg_logical_emit_message(true, 'test_psycopg', 'flushing')"
            )

        # start, relation, 320 inserts, stop, begin, msg, commit
        xlog_msgs = await collect_xlogdata_messages(
            alogical_started_cur, until=CommitMessage
        )
        relation1, insert1 = (msg.payload for msg in xlog_msgs[1:3])

        assert decoder.relations_by_xid[xid][relation1.relation_id] is relation1

        # sub xact
        async with start_streaming_insert(
            aconn,
            test_table,
            exec_first=lambda: aconn.execute(
                f"ALTER TABLE {test_table} ADD COLUMN extra bool"
            ),
            extra_data={"extra": "true"},
        ) as xid:
            # lower bound: next id after the unrelated flushing xact above
            lower_subxid = xid + 2
            async with await psycopg.AsyncConnection.connect(
                dsn, autocommit=True
            ) as aconn2:
                # force a flush of the second stream segment
                await aconn2.execute(
                    "SELECT pg_logical_emit_message(true, 'test_psycopg', 'flushing')"
                )
            xlog_msgs = await collect_xlogdata_messages(
                alogical_started_cur, until=CommitMessage
            )
            relation2, insert2 = (msg.payload for msg in xlog_msgs[1:3])

            raise psycopg.Rollback()

        await aconn.execute(
            f"INSERT INTO {test_table} (data) VALUES"
            + " ('xact_relation_post_rollback')"
        )

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=StreamStopMessage
    )
    # ...start ...stop
    abort, _, relation3, insert3 = (msg.payload for msg in xlog_msgs[0:-1])
    for relation in (relation1, relation2, relation3):
        assert isinstance(relation, RelationMessage)

    for insert in (insert1, insert2, insert3):
        assert isinstance(insert, InsertMessage)

    assert isinstance(abort, StreamAbortMessage)
    assert abort.subxid >= lower_subxid

    assert relation2 != relation1
    assert relation1 == relation3
    assert len(relation2.columns) == 3
    assert len(relation3.columns) == 2
    assert decoder.relations_by_xid[xid][insert3.relation_id] == relation1

    commit = (await collect_xlogdata_messages(alogical_started_cur, n=1))[0].payload
    assert isinstance(commit, StreamCommitMessage)
    assert commit.xid == xid
    assert xid not in decoder.relations_by_xid


async def register_composite_types(aconn, context, *types):
    for type_ in types:
        composite.register_composite(
            await composite.CompositeInfo.fetch(aconn, type_),
            context,
        )


@pytest.mark.parametrize("streaming", [stream_param("on")])
@pytest.mark.parametrize("row_factory", [oname_param(namedtuple_row)])
async def test_pgoutput_streaming_xact_types_subxact_rollback(
    dsn, alogical_started_cur, aconn, test_table, smallpoint_type, intpoint_type
):
    await register_composite_types(
        aconn, alogical_started_cur, smallpoint_type, intpoint_type
    )
    decoder = alogical_started_cur.decode_xlogdata.get_real_decoder()
    await aconn.execute(
        f"ALTER TABLE {test_table} ADD COLUMN location {smallpoint_type}"
    )

    async with start_streaming_insert(
        aconn,
        test_table,
        extra_data={"location": "(i, i+1)::smallpoint"},
    ) as xid:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True
        ) as aconn2:
            # force a flush of the first stream segment
            await aconn2.execute(
                "SELECT pg_logical_emit_message(true, 'test_psycopg', 'flushing')"
            )
        # NOTE: PostgreSQL < 15 generates empty begin/commit pairs for DDL statements
        # this deals with those.
        await collect_xlogdata_messages(alogical_started_cur, until=StreamStartMessage)

        # type, relation, 320 inserts, stop, begin, msg, commit
        xlog_msgs = await collect_xlogdata_messages(
            alogical_started_cur, until=CommitMessage
        )
        type1, relation1 = (msg.payload for msg in xlog_msgs[0:2])

        assert isinstance(type1, TypeMessage)
        assert isinstance(relation1, RelationMessage)
        assert decoder.types_by_xid[xid][relation1.columns[2].type_id] is type1
        assert type1.name == smallpoint_type

        # sub xact
        async with start_streaming_insert(
            aconn,
            test_table,
            exec_first=lambda: aconn.execute(
                f"ALTER TABLE {test_table} ALTER COLUMN location TYPE"
                + f" {intpoint_type} USING"
                + f" ((location).x::int, (location).y::int)::{intpoint_type}"
            ),
            extra_data={"location": f"(-i, -i-1)::{intpoint_type}"},
        ) as xid:
            # lower bound: next id after the unrelated flushing xact above
            lower_subxid = xid + 2
            async with await psycopg.AsyncConnection.connect(
                dsn, autocommit=True
            ) as aconn2:
                # force a flush of the second stream segment
                await aconn2.execute(
                    "SELECT pg_logical_emit_message(true, 'test_psycopg', 'flushing')"
                )
            # the remaining commits that weren't flushed from the outer xact
            xlog_msgs = await collect_xlogdata_messages(
                alogical_started_cur, until=StreamStopMessage
            )

            xlog_msgs = await collect_xlogdata_messages(
                alogical_started_cur, until=CommitMessage
            )

            # start...inserts, stop, start, msg, stop, commit
            type2, relation2 = (msg.payload for msg in xlog_msgs[1:3])

            assert isinstance(type2, TypeMessage)
            assert isinstance(relation2, RelationMessage)
            assert relation1 != relation2
            assert decoder.types_by_xid[xid][relation2.columns[2].type_id] is type2
            assert type2.name == intpoint_type

            raise psycopg.Rollback()

        await aconn.execute(
            f"INSERT INTO {test_table} (data, location) VALUES"
            + " ('xact_type_post_rollback', (123, 123))"
        )

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=StreamStopMessage
    )
    # ...start...insert, stop
    abort, _, type3, relation3 = (msg.payload for msg in xlog_msgs[0:-2])
    assert isinstance(abort, StreamAbortMessage)
    assert isinstance(type3, TypeMessage)
    assert isinstance(relation3, RelationMessage)

    assert abort.subxid >= lower_subxid
    assert type3 != type2
    assert type3 == type1
    assert type3.name == "smallpoint"
    assert relation1 == relation3
    assert relation2 != relation3

    assert decoder.types_by_xid[xid][type2.type_id] is type2

    commit = (await collect_xlogdata_messages(alogical_started_cur, n=1))[0].payload
    assert isinstance(commit, StreamCommitMessage)
    assert commit.xid == xid
    assert xid not in decoder.types_by_xid


@pytest.mark.parametrize("streaming", [stream_param("on")])
@pytest.mark.parametrize("row_factory", [oname_param(namedtuple_row)])
# Set binary to ensure type issues raise an error
@pytest.mark.parametrize("format", [format_param(pq.Format.BINARY)])
async def test_pgoutput_streaming_xact_types_subxact(
    dsn, alogical_started_cur, aconn, test_table, smallpoint_type, intpoint_type
):
    await register_composite_types(
        aconn, alogical_started_cur, smallpoint_type, intpoint_type
    )
    await aconn.execute(
        f"ALTER TABLE {test_table} ADD COLUMN location {smallpoint_type}"
    )
    decoder = alogical_started_cur.decode_xlogdata.get_real_decoder()

    async with start_streaming_insert(
        aconn, test_table, extra_data={"location": "(i, i+1)::smallpoint"}
    ) as xid:
        aconn2 = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            # force a flush of the first stream segment
            await aconn2.execute(
                f"INSERT INTO {test_table} (data, location)"
                + " VALUES ('force_flush', (0,0))"
            )
        finally:
            await aconn2.close()

        # NOTE: PostgreSQL < 15 generates empty begin/commit pairs for DDL statements
        # this deals with those.
        xlog_msgs = await collect_xlogdata_messages(
            alogical_started_cur, until=StreamStartMessage
        )
        start1 = xlog_msgs[-1].payload

        # type, relation, 320 - N inserts, stop, begin, type, relation,
        # insert, commit, start, type, relation, N inserts, stop
        xlog_msgs = await collect_xlogdata_messages(
            alogical_started_cur, until=StreamStopMessage
        )
        type1, relation1, insert1 = (msg.payload for msg in xlog_msgs[0:3])

        assert isinstance(start1, StreamStartMessage)
        assert isinstance(type1, TypeMessage)
        assert isinstance(relation1, RelationMessage)
        assert isinstance(insert1, InsertMessage)

        # begin, type, relation, insert, commit
        await collect_xlogdata_messages(alogical_started_cur, n=5)

        async with aconn.transaction():  # sub xact
            # lower bound: next id after the unrelated flushing xact above
            lower_subxid = xid + 2
            await aconn.execute(
                f"ALTER TABLE {test_table} ALTER COLUMN location TYPE"
                + f" {intpoint_type} USING"
                + f" ((location).x::int, (location).y::int)::{intpoint_type}"
            )

            await aconn.execute(
                f"INSERT INTO {test_table} (data, location) VALUES"
                + " ('xact_type_test', (1, 1))"
            )

    # the remaining commits that weren't flushed from the outer xact
    await collect_xlogdata_messages(alogical_started_cur, until=StreamStopMessage)

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=StreamStopMessage
    )
    start2, type2, relation2, insert2 = (msg.payload for msg in xlog_msgs[:4])

    assert isinstance(start2, StreamStartMessage)
    assert isinstance(type2, TypeMessage)
    assert isinstance(relation2, RelationMessage)
    assert isinstance(insert2, InsertMessage)

    assert type1.name == "smallpoint"
    assert type2.name == "intpoint"
    assert all(start.xid == xid for start in (start1, start2))

    assert decoder.types_by_xid[xid][relation1.columns[2].type_id] is type1
    assert len(decoder.types) == 1
    assert decoder.types[relation1.columns[2].type_id].xid is None
    assert decoder.types_by_xid[xid][relation2.columns[2].type_id] is type2
    assert insert1.xid == xid
    assert isinstance(insert2.xid, int)
    assert insert2.xid >= lower_subxid
    assert insert2.new_tuple.location == (1, 1)
    assert type(insert2.new_tuple.location).__name__ == "intpoint"
    assert insert2.new_tuple.data == "xact_type_test"

    # commit
    await collect_xlogdata_messages(alogical_started_cur, n=1)
    assert not decoder.types_by_xid


@pytest.mark.parametrize(
    "transactional",
    [True, False],
    ids=(lambda param: "transactional" if param else "nontransactional"),
)
@pytest.mark.parametrize("streaming", [stream_param("off"), stream_param("on")])
async def test_pgoutput_emit_message(
    alogical_started_cur, aconn, test_table, transactional, streaming
):
    """
    Call pg_logical_emit_message and ensure correct decoding.

    NOTE: server_encoding on the db should be set to something like LATIN1
    for this test to detect transcoding issues:
    `createdb -T template0 -E LATIN1 -l en_US.ISO8859-1 psycopg_test_latin1`
    `export PSYCOPG_TEST_DSN="host=localhost dbname=psycopg_test_latin1"
    """

    async with aconn.cursor() as cur:
        async with aconn.transaction():
            await cur.execute(
                "SELECT pg_logical_emit_message("
                + f"{str(transactional).lower()}, 'préfix', 'contènt'"
                + ") - '0/0';"
            )
            (lsn,) = await cur.fetchone()
            if streaming != "off":
                xid = await streaming_insert(aconn, test_table)

    # from psycopg.replication.replication_utils import string_to_lsn
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, until=EmitMessage)
    msg = xlog_msgs[-1].payload

    assert isinstance(msg, EmitMessage)
    assert msg.transactional is transactional
    assert msg.lsn == lsn
    assert msg.prefix == "préfix"
    assert msg.content == "contènt"

    if transactional and streaming != "off":
        assert xid is not None
        assert msg.xid == xid


@pytest.mark.parametrize(
    "streaming,execute_insert",
    [
        stream_param("off", lambda c, t: insert_data(c, t, "insert_test")),
        stream_param("on", streaming_insert),
    ],
)
@pytest.mark.parametrize(
    # TODO: maybe we don't need to test all these
    # DISCUSS maybe is_key should be renamed as well? It could be a source
    # of confusion, which is why I added the tests, but it's really just testing
    # postgreSQL, not our decoding.
    "set_replica_identity,replica_identity",
    [
        oname_param(replica_identity_default, ReplicaIdentity.DEFAULT),
        oname_param(replica_identity_full, ReplicaIdentity.FULL),
        oname_param(replica_identity_index, ReplicaIdentity.INDEX),
    ],
)
async def test_pgoutput_relation_message(
    alogical_started_cur,
    aconn,
    test_table,
    streaming,
    execute_insert,
    set_replica_identity,
    replica_identity,
):
    await set_replica_identity(aconn, test_table, logical_cur=alogical_started_cur)

    xid = await execute_insert(aconn, test_table)

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=InsertMessage
    )
    relation = xlog_msgs[-2].payload

    assert isinstance(relation, RelationMessage)
    assert relation.relation_name == test_table
    assert relation.replica_identity == replica_identity
    assert relation.namespace == "public"

    id_, data = relation.columns
    assert id_.name == "id"
    assert id_.type_id == types["int4"].oid
    assert id_.type_modifier == -1
    if replica_identity in (ReplicaIdentity.DEFAULT, ReplicaIdentity.FULL):
        assert id_.is_key is True
    else:
        assert id_.is_key is False
    assert data.name == "data"
    assert data.type_id == types["text"].oid
    assert data.type_modifier == -1
    if replica_identity in (ReplicaIdentity.INDEX, ReplicaIdentity.FULL):
        assert data.is_key is True
    else:
        assert data.is_key is False

    if streaming != "off":
        assert xid is not None
        assert relation.xid == xid


@pytest.mark.parametrize("streaming", [stream_param("off"), stream_param("on")])
async def test_pgoutput_truncate_message(
    alogical_started_cur,
    aconn,
    publication,
    test_table,
    streaming,
):
    async with aconn.transaction():
        await aconn.execute(
            "CREATE TABLE IF NOT EXISTS truncate_test_table"
            + " (id SERIAL PRIMARY KEY, data text)"
        )
        await aconn.execute(
            f"ALTER PUBLICATION {publication} ADD TABLE truncate_test_table"
        )
        await insert_data(aconn, "truncate_test_table", "data")
        await insert_data(aconn, test_table, "data")

    await collect_xlogdata_messages(alogical_started_cur, until=CommitMessage)

    async def exec_truncate():
        await aconn.execute(
            f"TRUNCATE {test_table}, truncate_test_table RESTART IDENTITY CASCADE"
        )

    if streaming != "off":
        xid = await streaming_insert(aconn, test_table, exec_first=exec_truncate)
    else:
        await exec_truncate()

    await aconn.execute("DROP TABLE truncate_test_table")

    # begin, relation, relation, truncate (truncate sends the relations again)
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=4)
    truncate = xlog_msgs[-1].payload

    assert isinstance(truncate, TruncateMessage)
    if streaming != "off":
        assert truncate.xid == xid
    assert truncate.cascade is True
    assert truncate.restart_identity is True
    assert truncate._options == 3
    assert len(truncate.relation_ids) == 2


@pytest.mark.parametrize(
    "streaming, execute_insert, expected_value",
    [
        stream_param(
            "off", lambda c, t: insert_data(c, t, "insert_test"), "insert_test"
        ),
        stream_param(
            "on",
            streaming_insert,
            "1: this is a pretty long string for testing streaming transactions :1",
        ),
        stream_param(
            "parallel",
            streaming_insert,
            "1: this is a pretty long string for testing streaming transactions :1",
        ),
    ],
)
async def test_pgoutput_insert_message(
    alogical_started_cur,
    aconn,
    test_table,
    streaming,
    execute_insert,
    expected_value,
):
    xid = await execute_insert(aconn, test_table)

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=4)
    _, _, insert, _ = (msg.payload for msg in xlog_msgs)

    assert isinstance(insert, InsertMessage)

    assert insert.new_tuple[1] == expected_value
    if streaming != "off":
        assert xid is not None
        assert insert.xid == xid


@pytest.mark.pg(">=15")
async def test_pgoutput_origin_message(
    alogical_started_cur,
    aconn,
    test_table,
    origin,
    aset_origin,
):
    origin_name = origin
    decoder = alogical_started_cur.decode_xlogdata.get_real_decoder()

    await insert_data(aconn, test_table, "origin_test")
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=2)
    _, origin = (msg.payload for msg in xlog_msgs)

    assert isinstance(origin, OriginMessage)
    assert decoder.origin is origin
    assert origin.name == origin_name
    assert origin.commit_lsn == 0  # We've not set it

    # relation, insert, commit
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=3)
    # commit still associated with origin
    assert decoder.origin.name == origin_name

    await aconn.execute("SELECT pg_replication_origin_session_reset()")
    await insert_data(aconn, test_table, "no_origin_test")

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=1)
    assert decoder.origin is None


@pytest.mark.pg(">=15")
@pytest.mark.parametrize("streaming", [stream_param("on")])
async def test_pgoutput_origin_streaming(
    dsn,
    alogical_started_cur,
    aconn,
    test_table,
    origin,
    aset_origin,
):
    decoder = alogical_started_cur.decode_xlogdata.get_real_decoder()
    origin_name = origin

    async with start_streaming_insert(aconn, test_table) as xid:
        aconn2 = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        try:
            # force a flush of the first stream segment
            await insert_data(aconn2, test_table, "force_flush")
        finally:
            await aconn2.close()

        # start, origin, relation, 320 inserts, stop
        xlog_msgs = await collect_xlogdata_messages(
            alogical_started_cur, until=StreamStopMessage
        )
        origin1 = xlog_msgs[1].payload
        assert origin1.name == origin_name
        assert decoder.origin is origin1
        # begin, relation, insert, commit
        await collect_xlogdata_messages(alogical_started_cur, n=4)
        assert decoder.origin is None

        await insert_data(aconn, test_table, "stream_segment_2")

    # start
    await collect_xlogdata_messages(alogical_started_cur, n=1)
    assert decoder.origin is origin1
    assert decoder.origin_by_xid[xid] is origin1

    # insert, stop
    await collect_xlogdata_messages(alogical_started_cur, n=2)
    assert decoder.origin is origin1
    assert decoder.origin_by_xid[xid] is origin1

    # commit
    await collect_xlogdata_messages(alogical_started_cur, n=1)
    assert decoder.origin is origin1
    assert xid not in decoder.origin_by_xid

    await insert_data(aconn, test_table, "regular_xact")

    # begin
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=1)
    assert decoder.origin is None

    # origin
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=1)
    assert decoder.origin == xlog_msgs[0].payload


@pytest.mark.parametrize("streaming", [stream_param("off"), stream_param("on")])
@pytest.mark.parametrize(
    "alter_table, expected, null_tuple,",
    [
        oname_param(replica_identity_default, "key_tuple", "old_tuple"),
        oname_param(replica_identity_full, "old_tuple", "key_tuple"),
        oname_param(replica_identity_index, ("key_tuple", (None, "data")), "old_tuple"),
    ],
)
async def test_pgoutput_delete_message(
    alogical_started_cur,
    aconn,
    test_table,
    alter_table,
    expected,
    null_tuple,
    streaming,
):
    await alter_table(aconn, test_table, logical_cur=alogical_started_cur)

    id_ = await insert_data(aconn, test_table, "data", returning="id")
    await collect_xlogdata_messages(alogical_started_cur, n=4)

    async def exec_delete():
        await aconn.execute(f"DELETE from {test_table}")

    if streaming != "off":
        xid = await streaming_insert(aconn, test_table, exec_first=exec_delete)
    else:
        await exec_delete()

    n_msgs = 2 if streaming == "off" else 3  # extra RelationMessage when streaming

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=n_msgs)
    delete = xlog_msgs[-1].payload

    if streaming != "off":
        assert xid is not None
        assert delete.xid == xid

    assert isinstance(delete, DeleteMessage)
    assert getattr(delete, null_tuple) is None
    if isinstance(expected, tuple):
        expected_tuple_attr, expected_value = expected
    else:
        expected_tuple_attr = expected
        if expected_tuple_attr == "key_tuple":
            # Key Tuples are sent with non-REPLICA-IDENTITY columns set to NULL
            expected_value = (id_, None)
        else:
            expected_value = (id_, "data")
    actual_tuple = getattr(delete, expected_tuple_attr)
    assert len(actual_tuple) == 2
    assert actual_tuple == expected_value


@pytest.mark.parametrize("streaming", [stream_param("off"), stream_param("on")])
@pytest.mark.parametrize(
    "alter_table, expected, null_tuples",
    [
        oname_param(replica_identity_default, None, ["old_tuple", "key_tuple"]),
        oname_param(replica_identity_full, "old_tuple", ["key_tuple"]),
        oname_param(
            replica_identity_index, ("key_tuple", (None, "data")), ["old_tuple"]
        ),
    ],
)
async def test_pgoutput_update_message(
    alogical_started_cur,
    aconn,
    test_table,
    alter_table,
    expected,
    null_tuples,
    streaming,
):
    """
    Verify that a transaction produces Begin and Commit pgoutput messages.
    """
    await alter_table(aconn, test_table, logical_cur=alogical_started_cur)

    id_ = await insert_data(aconn, test_table, "data", returning="id")
    await collect_xlogdata_messages(alogical_started_cur, n=4)

    async def exec_update():
        await aconn.execute(
            f"UPDATE {test_table} SET data='updated_data' WHERE data='data'"
        )

    if streaming != "off":
        xid = await streaming_insert(aconn, test_table, exec_first=exec_update)
        # start, relation, update
    else:
        await exec_update()
        # begin, update

    xlog_msgs = await collect_xlogdata_messages(
        alogical_started_cur, until=UpdateMessage
    )
    update = xlog_msgs[-1].payload

    assert isinstance(update, UpdateMessage)
    for null_tuple in null_tuples:
        assert getattr(update, null_tuple) is None

    if expected is not None:
        if isinstance(expected, tuple):
            expected_tuple_attr, expected_value = expected
        else:
            expected_tuple_attr = expected
            if expected_tuple_attr == "key_tuple":
                # Key Tuples are sent with non-REPLICA-IDENTITY columns set to NULL
                expected_value = (id_, None)
            else:
                expected_value = (id_, "data")
        actual_tuple = getattr(update, expected_tuple_attr)
        assert len(actual_tuple) == 2
        assert actual_tuple == expected_value
    assert update.new_tuple == (id_, "updated_data")

    if streaming != "off":
        assert xid is not None
        assert update.xid == xid


def _sequence_get_values(msg):
    return [msg[1]]


def _namedtuple_row_get_values(msg):
    return [msg[1], msg.data]


def _dict_get_values(msg):
    return [msg["data"]]


class MyClass:
    def __init__(self, *, id, data):
        self.other_name = data

    @staticmethod
    def _get_values(msg):
        return [msg.other_name, msg.__dict__["other_name"]]


def _(*vals):
    return pytest.param(*vals, id=vals[0].__name__.strip("_"))


@pytest.mark.parametrize(
    "row_factory, get_values, type_",
    [
        _(tuple_row, _sequence_get_values, tuple),
        _(dict_row, _dict_get_values, dict),
        _(namedtuple_row, _namedtuple_row_get_values, tuple),
        _(class_row(MyClass), MyClass._get_values, MyClass),
        _(args_row(lambda *args: list(args)), _sequence_get_values, list),
        _(kwargs_row(defaultdict), _dict_get_values, defaultdict),
    ],
)
async def test_pgoutput_factories(
    alogical_started_cur,
    aconn,
    test_table,
    get_values,
    type_,
):
    await insert_data(aconn, test_table, "factory_test")

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=4)
    _, _, insert_msg, _ = (msg.payload for msg in xlog_msgs)

    assert isinstance(insert_msg, InsertMessage)
    assert isinstance(insert_msg.new_tuple, type_)
    for value in get_values(insert_msg.new_tuple):
        assert value == "factory_test"


async def test_pgoutput_change_factory_midstream(
    alogical_started_cur, aconn, test_table
):
    await insert_data(aconn, test_table, "factory_test_tuple")

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=4)
    _, _, insert_msg, _ = (msg.payload for msg in xlog_msgs)

    assert isinstance(insert_msg, InsertMessage)
    assert isinstance(insert_msg.new_tuple, tuple)
    assert insert_msg.new_tuple[1] == "factory_test_tuple"

    await insert_data(aconn, test_table, "factory_test_dict")

    alogical_started_cur.decode_xlogdata.row_factory = dict_row

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=3)
    _, insert_msg, _ = (msg.payload for msg in xlog_msgs)

    assert isinstance(insert_msg, InsertMessage)
    assert isinstance(insert_msg.new_tuple, dict)
    assert insert_msg.new_tuple["data"] == "factory_test_dict"


@pytest.mark.parametrize(
    "format,make_loader",
    [
        format_param(pq.Format.TEXT, make_loader),
        format_param(pq.Format.BINARY, make_bin_loader),
    ],
)
async def test_pgoutput_adaption(
    aconn, alogical_started_cur, test_table, format, make_loader
):
    t1_loader = make_loader("t1")
    alogical_started_cur.adapters.register_loader("text", t1_loader)

    await insert_data(aconn, test_table, "t1adapt_test")

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=4)
    _, _, insert_msg, _ = (msg.payload for msg in xlog_msgs)

    assert insert_msg.new_tuple[1] == "t1adapt_testt1"

    await insert_data(aconn, test_table, "t2adapt_test")

    t2_loader = make_loader("t2")
    alogical_started_cur.adapters.register_loader("text", t2_loader)

    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=3)
    _, insert_msg, _ = (msg.payload for msg in xlog_msgs)

    assert insert_msg.new_tuple[1] == "t2adapt_testt2"


@pytest.mark.parametrize(
    "format",
    [format_param(pq.Format.TEXT), format_param(pq.Format.BINARY)],
)
async def test_pgoutput_unchanged_toast_adaption(
    aconn, alogical_started_cur, test_table
):
    async with aconn.transaction():
        await aconn.execute(
            f"ALTER TABLE {test_table} ADD COLUMN toasted text, ADD column extra bool"
        )
        async with aconn.cursor(row_factory=scalar_row) as cur:
            await cur.execute("SHOW block_size")
            toast_tuple_threshhold = int(await cur.fetchone()) // 4
        await aconn.execute(
            f"ALTER TABLE {test_table} ALTER COLUMN toasted SET STORAGE EXTERNAL"
        )
        await aconn.execute(
            f"INSERT INTO {test_table} (data, toasted, extra) VALUES (%s, %s, %s)",
            [
                "data",
                (_val := "test_toast") * (toast_tuple_threshhold // len(_val)),
                True,
            ],
        )

    # begin, relation, insert, commit
    await collect_xlogdata_messages(alogical_started_cur, n=4)

    await aconn.execute(
        f"UPDATE {test_table} SET data='updated_data' WHERE data='data'"
    )

    # begin, relation, insert
    xlog_msgs = await collect_xlogdata_messages(alogical_started_cur, n=3)
    update = xlog_msgs[-1].payload

    assert isinstance(update, UpdateMessage)
    assert len(update.new_tuple) == 4
    assert update.new_tuple[-2] == RowValue.UNCHANGED
    assert update.new_tuple[-2] == RowValue.UNCHANGED
    assert update.new_tuple[-2] == RowValue.UNCHANGED
