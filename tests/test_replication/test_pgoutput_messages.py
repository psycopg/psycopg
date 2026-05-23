"""
Unit tests for pgoutput message decoding.

These tests do not require a database connection – they test the
binary decoders for each pgoutput message type.
"""

from __future__ import annotations

import json
import base64
import struct
from abc import ABC
from typing import Any, Generic, TypeVar, cast, get_args, get_origin
from pathlib import Path
from datetime import datetime

import pytest

from psycopg import errors as e
from psycopg.replication.replication_options import ReplicaIdentity
from psycopg.replication.logical_output_plugins.pgoutput.pgoutput_decoder import (
    PgOutputDecoder,
)
from psycopg.replication.logical_output_plugins.pgoutput.pgoutput_messages import (
    BeginMessage,
    BeginPrepareMessage,
    ColumnDefinition,
    CommitMessage,
    CommitPreparedMessage,
    DeleteMessage,
    EmitMessage,
    InsertMessage,
    MessageType,
    OriginMessage,
    PgOutputMessage,
    PrepareMessage,
    RelationMessage,
    RollbackPreparedMessage,
    StreamAbortMessage,
    StreamCommitMessage,
    StreamPrepareMessage,
    StreamStartMessage,
    StreamStopMessage,
    TruncateMessage,
    TypeMessage,
    UpdateMessage,
    _type_to_decode_map,
)


@pytest.fixture(scope="module")
def data():
    script_dir = Path(__file__).parent

    # From PostgreSQL v18
    with open(script_dir / "pgoutput_message_examples.json", "r") as f:
        test_data = json.load(f)
    return test_data


def null_str(s: str) -> bytes:
    """Encode a string as a null-terminated UTF-8 byte string."""
    return s.encode("utf-8") + b"\x00"


def build_tuple_data(values: list[str | None]) -> bytes:
    """
    Build pgoutput TupleData bytes for a list of values (by position).

    TupleData format:
      Int16: number of columns
      For each column:
        Byte1: 'n' (null), 't' (text)
        If 't': Int32 (length) + bytes (text value)
    """
    nfields = len(values)
    out = struct.pack(">H", nfields)
    for value in values:
        if value is None:
            out += b"n"
        else:
            encoded = value.encode("utf-8")
            out += b"t" + struct.pack(">I", len(encoded)) + encoded
    return out


def parametrize_streaming(*vals):
    for val in vals:
        assert val in {"off", "on", "parallel"}

    def stream_param(val):
        return pytest.param(val, id=f"streaming_{val}")

    @pytest.fixture(params=[stream_param(val) for val in vals])
    def streaming(self, request):
        return request.param

    def _set_streaming(cls):
        setattr(cls, "streaming", streaming)
        return cls

    return _set_streaming


MsgCls = TypeVar("MsgCls", bound=PgOutputMessage[Any])


@parametrize_streaming("off", "on")
class MessageTestBase(ABC, Generic[MsgCls]):
    msg_cls: type[MsgCls]
    _decoder = None

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.msg_cls = cls.get_msg_cls()

    @classmethod
    def get_msg_cls(cls) -> type[MsgCls]:
        for base in cls.__orig_bases__:  # type: ignore[attr-defined]
            origin = get_origin(base)
            if origin is None or not issubclass(origin, MessageTestBase):
                continue
            return cast(type[MsgCls], get_args(base)[0])
        raise AttributeError(f"{cls.__name__} is generic; type argument unspecified")

    @pytest.fixture(autouse=True)
    def setup_test_data(self, data, streaming):
        self.streaming_opt = streaming
        self.xid: int | None = 1
        key = "example_msgs"
        if streaming == "on":
            key += "_stream"
        elif streaming == "parallel":
            key += "_stream_parallel"
        else:
            self.xid = None

        data = data[key]
        self.example_msgs = data["msgs"]
        self.relations = data["relations"]
        if "relations_by_xid" in data:
            self.relations_by_xid = data["relations_by_xid"]
        self.decoder.streaming = data["streaming_xid"].get(self.msg_cls.msg_type)

    @pytest.fixture
    def no_loaders(self, monkeypatch):
        @property  # type: ignore[misc]
        def tx_none(self):
            return None

        with monkeypatch.context() as m:
            m.setattr(self.decoder, "get_row_maker", lambda relation: tuple)
            m.setattr(type(self.decoder), "tx", tx_none)
            m.setattr(
                self.decoder,
                "get_relation",
                lambda relation_id: RelationMessage(
                    relation_id=relation_id,
                    relation_name="fakerelation",
                    namespace="public",
                    columns=(),
                    replica_identity=ReplicaIdentity.DEFAULT,
                    xid=self.xid,
                ),
            )

            yield

    @staticmethod
    def relation_from_dict(rel_dict):
        return RelationMessage(
            columns=tuple(
                ColumnDefinition(**def_dict) for def_dict in rel_dict["columns"]
            ),
            **{key: value for key, value in rel_dict.items() if key != "columns"},
        )

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, relations):
        self._relations = relations
        for rel_id, rel_dict in relations.items():
            rel = self.relation_from_dict(rel_dict)
            self.decoder.relations[int(rel_id)] = rel

    @property
    def relations_by_xid(self):
        return self._relations_by_xid

    @relations_by_xid.setter
    def relations_by_xid(self, relations_by_xid):
        self._relations_by_xid = relations_by_xid
        for xid, relations in relations_by_xid.items():
            xid = int(xid)
            for rel_id, rel_dict in relations.items():
                rel_id = int(rel_id)
                rel = self.relation_from_dict(rel_dict)
                self.decoder.relations_by_xid[xid][rel_id] = rel

    @property
    def decoder(self):
        if self._decoder is not None:
            return self._decoder
        self._decoder = (decoder := PgOutputDecoder())
        decoder.server_encoding = "latin1"
        decoder.streaming = None
        decoder.plugin_options["streaming"] = self.streaming_opt

        return decoder

    def get_real_msg(self):
        payload = base64.b64decode(self.example_msgs[self.msg_cls.msg_type])
        return payload[1:]

    def _build_IUDR_header(self, relation_id=1):
        if self.xid is None:
            data = struct.pack(">I", relation_id)
        else:
            data = struct.pack(">II", self.xid, relation_id)
        return data

    def decode(self, payload: bytes) -> MsgCls:
        return self.msg_cls.decode(self.decoder, payload)

    def test_decode_basic(self):
        payload = self.get_real_msg()
        msg = self.decode(payload)
        assert isinstance(msg, self.msg_cls)

    def test_slots(self):
        payload = self.get_real_msg()
        msg = self.decode(payload)
        with pytest.raises(
            AttributeError, match="no __dict__ for setting new attributes"
        ):
            msg.xxx_does_not_exist = 9  # type: ignore[attr-defined]

    def test_msg_type(self):
        assert self.msg_cls.msg_type == getattr(MessageType, self.msg_cls.msg_type_name)

    def test_decode_map_registered(self):
        assert self.msg_cls.msg_type in _type_to_decode_map


class TestBeginMessage(MessageTestBase[BeginMessage]):
    @classmethod
    def _build(cls, final_lsn=0x12345678, timestamp_micro=0, xid=42):
        return struct.pack(">QQI", final_lsn, timestamp_micro, xid)

    def test_commit_ts_is_datetime(self):
        payload = self.get_real_msg()
        msg = self.decode(payload)
        assert isinstance(msg.commit_ts, datetime)


class TestCommitMessage(MessageTestBase[CommitMessage]):
    @classmethod
    def _build(cls, flags=0, commit_lsn=0x200, end_lsn=0x300, timestamp_micro=500):
        return struct.pack(">BQQQ", flags, commit_lsn, end_lsn, timestamp_micro)

    def test_commit_ts_is_datetime(self):
        payload = self.get_real_msg()
        msg = self.decode(payload)
        assert isinstance(msg.commit_ts, datetime)


class TestOriginMessage(MessageTestBase[OriginMessage]):
    def _build(self, commit_lsn=0x500, name="myorigin"):
        return struct.pack(">Q", commit_lsn) + null_str(name)


class TestRelationMessage(MessageTestBase[RelationMessage]):
    def _build(
        self,
        relation_id=1,
        namespace="public",
        name="mytable",
        replica_identity=ReplicaIdentity.DEFAULT,
        columns=None,
    ):
        data = self._build_IUDR_header(relation_id)
        data += null_str(namespace)
        data += null_str(name)
        # replica identity (1 byte) + ncolumns (2 bytes, big-endian)
        cols = columns or []
        data += struct.pack(">BH", ord(replica_identity), len(cols))
        for flags, col_name, type_id, type_mod in cols:
            data += struct.pack(">B", flags)
            data += null_str(col_name)
            data += struct.pack(">Ii", type_id, type_mod)
        return data

    def test_slots(self):
        payload = self.get_real_msg()
        msg = self.decode(payload)
        # See https://github.com/python/cpython/issues/90055
        with pytest.raises(TypeError):
            msg.xxx_does_not_exist = 9  # type: ignore[attr-defined]

    def test_decode_no_columns(self):
        payload = self._build()
        msg = self.decode(payload)
        assert isinstance(msg, RelationMessage)
        assert msg.relation_name == "mytable"
        assert msg.namespace == "public"
        assert msg.columns == ()

    def test_decode_with_columns(self):
        cols = [
            (0, "id", 23, -1),  # int4, no modifier
            (0, "name", 25, -1),  # text, no modifier
        ]
        payload = self._build(
            relation_id=42, namespace="public", name="users", columns=cols
        )
        msg = self.decode(payload)
        assert msg.relation_id == 42
        assert msg.relation_name == "users"
        assert len(msg.columns) == 2
        assert msg.columns[0].name == "id"
        assert msg.columns[0].type_id == 23
        assert msg.columns[1].name == "name"

    def test_column_is_key_flag(self):
        # flags=0x01 means it is part of the primary key
        cols = [(0x01, "id", 23, -1)]
        payload = self._build(columns=cols)
        msg = self.decode(payload)
        assert msg.columns[0].is_key is True

    def test_column_not_key_flag(self):
        cols = [(0x00, "data", 25, -1)]
        payload = self._build(columns=cols)
        msg = self.decode(payload)
        assert msg.columns[0].is_key is False


class TestTypeMessage(MessageTestBase[TypeMessage]):
    @classmethod
    def _build(cls, type_id=100, namespace="pg_catalog", name="int4"):
        return struct.pack(">I", type_id) + null_str(namespace) + null_str(name)

    # See https://github.com/python/cpython/issues/90055
    test_slots = TestRelationMessage.test_slots


class TestInsertMessage(MessageTestBase[InsertMessage]):
    def _build(self, relation_id=1, values=None, tuple_indicator=b"N"):
        """
        values is a list of str | None representing positional column values.
        """
        if values is None:
            values = ["1", "hello"]
        if self.xid is None:
            data = struct.pack(">I", relation_id)
        else:
            data = struct.pack(">II", self.xid, relation_id)
        data += tuple_indicator  # new tuple marker
        data += build_tuple_data(values)
        return data

    def test_decode_null_field(self, no_loaders):
        payload = self._build(values=["1", None])
        msg = self.decode(payload)
        assert msg.new_tuple[1] is None

    def test_two_columns(self, no_loaders):
        payload = self._build(values=["42", "world"])
        msg = self.decode(payload)
        assert len(msg.new_tuple) == 2

    def test_unexpected_tuple_marker_raises(self, no_loaders):
        bad_payload = self._build(values=["42", "world"], tuple_indicator=b"Z")
        with pytest.raises(e.DataError, match="Expected 'N'"):
            self.decode(bad_payload)


class TestUpdateMessage(MessageTestBase[UpdateMessage]):
    def _build(self, relation_id=1, values=None):
        if values is None:
            values = ["1", "updated"]
        data = self._build_IUDR_header(relation_id)
        data += b"N"  # new tuple only (no old)
        data += build_tuple_data(values)
        return data

    def _build_with_old(self, relation_id=1, old_values=None, new_values=None):
        if old_values is None:
            old_values = ["1", "old"]
        if new_values is None:
            new_values = ["1", "new"]
        data = self._build_IUDR_header(relation_id)
        data += b"O"  # old tuple present
        data += build_tuple_data(old_values)
        data += b"N"  # new tuple follows
        data += build_tuple_data(new_values)
        return data

    def test_decode_new_only(self, no_loaders):
        payload = self._build(values=["1", "new_val"])
        msg = self.decode(payload)
        assert isinstance(msg, UpdateMessage)
        assert msg.old_tuple is None
        assert msg.new_tuple[0] == b"1"
        assert msg.new_tuple[1] == b"new_val"

    def test_decode_with_old_tuple(self, no_loaders):
        payload = self._build_with_old(
            old_values=["1", "before"],
            new_values=["1", "after"],
        )
        msg = self.decode(payload)
        assert msg.old_tuple is not None
        assert msg.old_tuple[1] == b"before"
        assert msg.new_tuple[1] == b"after"


class TestDeleteMessage(MessageTestBase[DeleteMessage]):
    def _build(self, relation_id=1, values=None, tuple_indicator=b"K"):
        if values is None:
            values = [1]
        data = self._build_IUDR_header(relation_id)
        data += tuple_indicator
        data += build_tuple_data(values)
        return data

    def test_decode_key_tuple(self, no_loaders):
        payload = self._build(values=["42"])
        msg = self.decode(payload)
        assert isinstance(msg, DeleteMessage)
        assert msg.key_tuple is not None
        assert msg.key_tuple[0] == b"42"
        assert msg.old_tuple is None

    def test_decode_old_tuple(self, no_loaders):
        payload = self._build(values=["5", "removed"], tuple_indicator=b"O")
        msg = self.decode(payload)
        assert msg.old_tuple is not None
        assert msg.old_tuple[0] == b"5"
        assert msg.key_tuple is None

    def test_unexpected_marker_raises(self, no_loaders):
        bad = self._build(values=["5", "removed"], tuple_indicator=b"X")
        with pytest.raises(e.DataError, match="Expected 'K' or 'O'"):
            self.decode(bad)


class TestTruncateMessage(MessageTestBase[TruncateMessage]):
    def _build(self, relation_ids, options=0):
        if self.xid is None:
            data = struct.pack(">IB", len(relation_ids), options)
        else:
            data = struct.pack(">IIB", self.xid, len(relation_ids), options)
        for rid in relation_ids:
            data += struct.pack(">I", rid)
        return data

    def test_decode_single_relation(self, no_loaders):
        payload = self._build([10])
        msg = self.decode(payload)
        assert isinstance(msg, TruncateMessage)
        assert msg.relation_ids == [10]
        assert msg._options == 0
        assert msg.cascade is False
        assert msg.restart_identity is False

    def test_decode_multiple_relations(self, no_loaders):
        payload = self._build([1, 2, 3], options=1)
        msg = self.decode(payload)
        assert msg.relation_ids == [1, 2, 3]
        assert msg._options == 1
        assert msg.restart_identity is False
        assert msg.cascade is True

    def test_decode_empty(self, no_loaders):
        payload = self._build([])
        msg = self.decode(payload)
        assert msg.relation_ids == []


class TestEmitMessage(MessageTestBase[EmitMessage]):
    @classmethod
    def _build(cls, flags=0, lsn=0x100, prefix="test", content=b"hello"):
        # Format: flags (1B) + lsn (8B) = 9 bytes header
        data = struct.pack(">BQ", flags, lsn)
        data += null_str(prefix)
        data += struct.pack(">I", len(content)) + content
        return data

    def test_transactional_true(self):
        payload = self._build(flags=1)
        msg = self.decode(payload)
        assert msg.transactional is True

    def test_transactional_false(self):
        payload = self._build(flags=0)
        msg = self.decode(payload)
        assert msg.transactional is False

    def test_lsn(self):
        payload = self._build(lsn=0xDEADBEEF)
        msg = self.decode(payload)
        assert msg.lsn == 0xDEADBEEF


@parametrize_streaming("on", "parallel")
class TestStreamStartMessage(MessageTestBase[StreamStartMessage]):

    @classmethod
    def _build(cls, xid=1, first_segment=1):
        return struct.pack(">IB", xid, first_segment)

    def test_decode_first_segment(self):
        payload = self._build(xid=99, first_segment=1)
        msg = self.decode(payload)
        assert isinstance(msg, StreamStartMessage)
        assert msg.xid == 99
        assert msg.first_segment is True

    def test_decode_not_first_segment(self):
        payload = self._build(xid=99, first_segment=0)
        msg = self.decode(payload)
        assert msg.first_segment is False


@parametrize_streaming("on", "parallel")
class TestStreamStopMessage(MessageTestBase[StreamStopMessage]):
    pass


@parametrize_streaming("on")
class TestStreamCommitMessage(MessageTestBase[StreamCommitMessage]):
    @classmethod
    def _build(cls, xid=1, flags=0, commit_lsn_low=0x100):
        return struct.pack(">IBI", xid, flags, commit_lsn_low)


@parametrize_streaming("on", "parallel")
class TestStreamAbortMessage(MessageTestBase[StreamAbortMessage]):
    pass


@parametrize_streaming("off")
class TestBeginPrepareMessage(MessageTestBase[BeginPrepareMessage]):
    pass


@parametrize_streaming("off")
class TestPrepareMessage(MessageTestBase[PrepareMessage]):
    pass


@parametrize_streaming("on")
class TestStreamPrepareMessage(MessageTestBase[StreamPrepareMessage]):
    pass


class TestRollbackPreparedMessage(MessageTestBase[RollbackPreparedMessage]):
    pass


class TestCommitPreparedMessage(MessageTestBase[CommitPreparedMessage]):
    pass


def _generate_tests(msg_type_map):
    for msg_type, msg_cls in msg_type_map.items():

        def test_dispatches_NAME(self):
            assert _type_to_decode_map[msg_type] is msg_cls

        test_dispatches_NAME.__name__ = "test_dispatches_%s" % msg_cls.__name__.lower()
        yield test_dispatches_NAME


class TestPgOutputDecoder:
    msg_type_map = {
        "B": BeginMessage,
        "C": CommitMessage,
        "c": StreamCommitMessage,
        "D": DeleteMessage,
        "U": UpdateMessage,
        "M": EmitMessage,
        "O": OriginMessage,
        "R": RelationMessage,
        "Y": TypeMessage,
        "I": InsertMessage,
        "T": TruncateMessage,
        "S": StreamStartMessage,
        "E": StreamStopMessage,
        "A": StreamAbortMessage,
        "b": BeginPrepareMessage,
        "P": PrepareMessage,
        "K": CommitPreparedMessage,
        "r": RollbackPreparedMessage,
        "p": StreamPrepareMessage,
    }

    for _ in _generate_tests(msg_type_map):
        locals()[_.__name__] = _
    del _

    def test_decoder_unknown_type_raises(self):
        decoder = PgOutputDecoder()
        with pytest.raises(
            e.DataError,
            match="Unexpected message type for pgoutput plugin: 'Z'",
        ):
            decoder(b"Z")

    def test_all_msg_types_mapped(self):
        assert self.msg_type_map.keys() == _type_to_decode_map.keys()
