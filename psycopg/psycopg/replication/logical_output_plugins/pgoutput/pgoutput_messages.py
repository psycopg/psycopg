from __future__ import annotations

import inspect
import functools
from abc import abstractmethod
from sys import intern
from enum import StrEnum
from struct import Struct
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Literal,
    Protocol,
    TypeVar,
    cast,
    dataclass_transform,
)
from weakref import WeakValueDictionary
from datetime import datetime
from dataclasses import dataclass, field

from .... import errors as e
from .... import pq
from ....abc import Buffer, Transformer
from ...._compat import Self
from ...._cmodule import _psycopg
from ..logical_rows import LogicalRow, RowValue
from ...replication_utils import pg_epoch_to_datetime
from ...replication_options import ReplicaIdentity

if TYPE_CHECKING:
    from .pgoutput_decoder import PgOutputDecoder


class MessageType(StrEnum):
    BEGIN = intern("B")
    MESSAGE = intern("M")
    COMMIT = intern("C")
    ORIGIN = intern("O")
    RELATION = intern("R")
    TYPE = intern("Y")
    INSERT = intern("I")
    UPDATE = intern("U")
    DELETE = intern("D")
    TRUNCATE = intern("T")
    STREAM_START = intern("S")
    STREAM_STOP = intern("E")
    STREAM_COMMIT = intern("c")
    STREAM_ABORT = intern("A")
    BEGIN_PREPARE = intern("b")
    PREPARE = intern("P")
    COMMIT_PREPARED = intern("K")
    ROLLBACK_PREPARED = intern("r")
    STREAM_PREPARE = intern("p")


_type_to_decode_map: dict[str, type[PgOutputMessage[Any]]] = {}

# Struct definitions for unpacking binary data
_unpack_int32 = cast(Callable[[Buffer], tuple[int]], Struct(">I").unpack)
_unpack_int32_int8 = cast(Callable[[Buffer], tuple[int, int]], Struct(">IB").unpack)
_unpack_2int32 = cast(Callable[[Buffer], tuple[int, int]], Struct(">II").unpack)
_unpack_int8_3int64_int32 = cast(
    Callable[[Buffer], tuple[int, int, int, int, int]], Struct(">BQQQI").unpack
)

_begin_struct = Struct(">QQI")
_unpack_begin = cast(Callable[[Buffer], tuple[int, int, int]], _begin_struct.unpack)

_commit_struct = Struct(">BQQQ")
_unpack_commit = cast(
    Callable[[Buffer], tuple[int, int, int, int]], _commit_struct.unpack
)

unpack_relation_id = _unpack_int32
unpack_xid_and_relation_id = _unpack_2int32

_relation_replica_identity_struct = Struct(">BH")
unpack_replica_identity_and_ncolumns = cast(
    Callable[[Buffer], tuple[int, int]], _relation_replica_identity_struct.unpack
)

_column_flags_struct = Struct(">B")
unpack_column_flags = cast(Callable[[Buffer], tuple[int]], _column_flags_struct.unpack)

_column_type_struct = Struct(">Ii")
unpack_column_type = cast(
    Callable[[Buffer], tuple[int, int]], _column_type_struct.unpack
)

unpack_truncate_header = _unpack_int32_int8
_unpack_streaming_truncate_header_struct = Struct(">IIB")
unpack_streaming_truncate_header = cast(
    Callable[[Buffer], tuple[int, int, int]],
    _unpack_streaming_truncate_header_struct.unpack,
)

_origin_struct = Struct(">Q")
unpack_origin_lsn = cast(Callable[[Buffer], tuple[int]], _origin_struct.unpack)

unpack_type_id = _unpack_int32
unpack_xid_and_type_id = _unpack_2int32

_message_struct = Struct(">BQ")
unpack_emit_message_header = cast(
    Callable[[Buffer], tuple[int, int]], _message_struct.unpack
)

_streaming_emit_message_struct = Struct(">IBQ")
unpack_streaming_emit_message_header = cast(
    Callable[[Buffer], tuple[int, int, int]], _streaming_emit_message_struct.unpack
)

_unpack_stream_start = _unpack_int32_int8

_stream_commit_struct = Struct(">IBQQQ")
_unpack_stream_commit = cast(
    Callable[[Buffer], tuple[int, int, int, int, int]], _stream_commit_struct.unpack
)

_unpack_stream_abort = _unpack_2int32
_stream_abort_parallel_struct = Struct(">IIQQ")
_unpack_stream_abort_parallel = cast(
    Callable[[Buffer], tuple[int, int, int, int]], _stream_abort_parallel_struct.unpack
)

_begin_prepare_struct = Struct(">QQQI")
_unpack_begin_prepare = cast(
    Callable[[Buffer], tuple[int, int, int, int]], _begin_prepare_struct.unpack
)
_unpack_prepare = _unpack_int8_3int64_int32
_unpack_commit_prepared = _unpack_int8_3int64_int32
_rollback_prepared_struct = Struct(">BQQQQI")
_unpack_rollback_prepared = cast(
    Callable[[Buffer], tuple[int, int, int, int, int, int]],
    _rollback_prepared_struct.unpack,
)
unpack_stream_prepare = _unpack_int8_3int64_int32


def _read_null_terminated_string(
    data: Buffer, offset: int, encoding: str = "utf-8"
) -> tuple[str, int]:
    """Read a null-terminated string from buffer and return (string, new_offset)."""
    end = offset
    while end < len(data) and data[end] != 0:
        end += 1
    string = bytes(data[offset:end]).decode(encoding)
    return string, end + 1  # Skip null terminator


def _parse_tuple_data(
    data: Buffer,
    offset: int,
    tx: Transformer,
    format: pq.Format,
    unchanged_sentinel: Any,
) -> tuple[list[Any] | tuple[Any], int]:
    # Number of columns (2 bytes)
    ncolumns = int.from_bytes(data[offset : offset + 2], byteorder="big", signed=False)
    offset += 2

    tuple_: list[bytes | None] = []

    for i in range(ncolumns):
        # Column data format (1 byte): 'n' = null, 't' = text, 'u' = unchanged TOAST
        col_type = chr(data[offset])
        offset += 1

        if col_type == "n":
            # Null value
            tuple_.append(None)
        elif col_type == "u":
            # Unchanged TOAST value
            tuple_.append(unchanged_sentinel)
        elif col_type in ("t", "b"):
            if format is pq.Format.TEXT:
                if col_type == "b":
                    raise e.DataError("Expected TEXT format but got BINARY format")
            elif col_type == "t":
                raise e.DataError("Expected BINARY format but got TEXT format")

            # text-encoded PostgreSQL value with length prefix
            length = int.from_bytes(
                data[offset : offset + 4], byteorder="big", signed=False
            )
            offset += 4
            value = bytes(data[offset : offset + length])
            offset += length
            tuple_.append(value)
        else:
            raise e.DataError(f"Unknown column data type: {col_type}")

    if tx is not None:
        adapted_tuple: list[Any] | tuple[Any] = tx.load_sequence(
            tuple_, passthrough=unchanged_sentinel
        )
    else:
        adapted_tuple = tuple_

    return adapted_tuple, offset


def _create_row(
    decoder: PgOutputDecoder[LogicalRow],
    relation: RelationMessage,
    data: Buffer,
    offset: int,
) -> tuple[LogicalRow, int]:
    if decoder.tx is not None:
        decoder.tx.set_loader_types(
            [col.type_id for col in relation.columns],
            decoder.format,
        )
    tuple_, offset = parse_tuple_data(
        data,
        offset,
        tx=decoder.tx,
        format=decoder.format,
        unchanged_sentinel=RowValue.UNCHANGED,
    )
    row_maker = decoder.get_row_maker(relation)
    return row_maker(tuple_), offset


class PgOutputMessage(Protocol[LogicalRow]):
    __slots__ = ()
    msg_type: ClassVar[str]
    msg_type_name: ClassVar[str]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        _type_to_decode_map[cls.msg_type] = cls

    @classmethod
    @abstractmethod
    def decode(
        cls,
        decoder: PgOutputDecoder[Any],
        data: Buffer,
    ) -> Self: ...


T = TypeVar("T", bound=Any)


@dataclass_transform()
def msg_dataclass(cls: type[T]) -> type[T]:
    return dataclass(slots=True, weakref_slot=True)(cls)


@dataclass_transform(kw_only_default=True, frozen_default=True)
def cached_msg_dataclass(cls: type[T]) -> type[T]:
    cls.__annotations__["_initialized"] = bool
    cls._initialized = field(
        default=False, init=False, compare=False, hash=False, repr=False
    )
    cls = dataclass(slots=True, weakref_slot=True, frozen=True, kw_only=True)(cls)
    generated_init = cls.__init__
    __instances: WeakValueDictionary[frozenset[tuple[str, Any]], T] = (
        WeakValueDictionary()
    )

    def __new__(cls: type[T], **kwargs: Any) -> T:
        key = frozenset(kwargs.items())
        existing = __instances.get(key)
        if existing is not None:
            return existing
        new = __instances.setdefault(key, object.__new__(cls))
        object.__setattr__(new, "_initialized", False)
        return new

    @functools.wraps(generated_init)
    def __init__(self: T, **kwargs: Any) -> None:
        if not self._initialized:
            generated_init(self, **kwargs)
            object.__setattr__(self, "_initialized", True)

    # Needed for Sphinx docs to get the correct signature
    __new__.__dict__["__signature__"] = inspect.Signature(
        [
            inspect.Parameter(
                "cls", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation="type[T]"
            )
        ]
        + list(inspect.signature(cls.__init__).parameters.values())[1:],
        return_annotation="T",
    )

    setattr(cls, "__init__", __init__)
    setattr(cls, "__new__", __new__)

    return cls


@msg_dataclass
class BeginMessage(PgOutputMessage):
    msg_type = MessageType.BEGIN
    msg_type_name = MessageType(MessageType.BEGIN).name

    final_lsn: int
    commit_ts: datetime
    xid: int

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
        format: pq.Format = pq.Format.TEXT,
    ) -> Self:
        """Decode Begin message.

        Format (from `PostgreSQL docs`__):

        * Int64: Final LSN of the transaction
        * Int64: Commit timestamp (microseconds since PostgreSQL epoch)
        * Int32: Transaction ID (xid)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-BEGIN
        """  # noqa: E501
        final_lsn, commit_ts_micro, xid = unpack_begin(data)

        decoder.origin = None

        return cls(
            final_lsn=final_lsn,
            commit_ts=pg_epoch_to_datetime(commit_ts_micro),
            xid=xid,
        )


def _parse_emit_message(
    data: Buffer, is_streaming: bool, encoding: str
) -> tuple[int | None, bool, int, str, str]:
    # Flags, LSN (1 + 8 = 9 bytes)
    offset = 9

    if not is_streaming:
        xid = None
        transactional, lsn = unpack_emit_message_header(data[0:offset])
    else:
        offset += 4
        xid, transactional, lsn = unpack_streaming_emit_message_header(data[0:offset])

    # Prefix (null-terminated)
    prefix, offset = _read_null_terminated_string(data, offset, encoding=encoding)

    # Content length (4 bytes)
    content_len = int.from_bytes(
        data[offset : offset + 4], byteorder="big", signed=False
    )
    offset += 4

    # Content
    content = str(data[offset : offset + content_len], encoding=encoding)

    return xid, bool(transactional), lsn, prefix, content


@msg_dataclass
class EmitMessage(PgOutputMessage):
    """Logical decoding message - generic message from pg_logical_emit_message."""

    msg_type = MessageType.MESSAGE
    msg_type_name = MessageType(MessageType.MESSAGE).name

    xid: int | None
    transactional: bool
    lsn: int
    prefix: str
    content: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode generic logical decoding message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int8: Flags (0 = non-transactional, 1 = transactional)
        * Int64: LSN of the message
        * String: Prefix (null-terminated)
        * Int32: Length of content
        * Bytes: Content

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-MESSAGE
        """  # noqa: E501
        xid, transactional, lsn, prefix, content = parse_emit_message(
            data, decoder.streaming is not None, decoder.server_encoding
        )
        return cls(
            xid=xid,
            transactional=transactional,
            lsn=lsn,
            prefix=prefix,
            content=content,
        )


@msg_dataclass
class CommitMessage(PgOutputMessage):
    msg_type = MessageType.COMMIT
    msg_type_name = MessageType(MessageType.COMMIT).name

    commit_lsn: int
    end_lsn: int
    commit_ts: datetime

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Commit message.

        Format (from `PostgreSQL docs`__):

        * Int8: Flags (currently unused, always 0)
        * Int64: LSN of the commit
        * Int64: LSN of the end of the transaction
        * Int64: Commit timestamp (microseconds since PostgreSQL epoch)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-COMMIT
        """  # noqa: E501
        _, commit_lsn, end_lsn, commit_ts_micro = unpack_commit(data)

        return cls(
            commit_lsn=commit_lsn,
            end_lsn=end_lsn,
            commit_ts=pg_epoch_to_datetime(commit_ts_micro),
        )


@msg_dataclass
class OriginMessage(PgOutputMessage):
    """Origin message - identifies replication origin."""

    msg_type = MessageType.ORIGIN
    msg_type_name = MessageType(MessageType.ORIGIN).name

    commit_lsn: int
    name: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Origin message.

        Format (from `PostgreSQL docs`__):

        * Int64: LSN of the commit on the origin server
        * String: Name of the origin (null-terminated)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-ORIGIN
        """  # noqa: E501
        # Commit LSN (8 bytes)
        (commit_lsn,) = unpack_origin_lsn(data[0:8])

        # Origin name (null-terminated)
        name = str(data[8:-1], encoding=decoder.server_encoding)

        origin = cls(commit_lsn=commit_lsn, name=name)
        decoder.origin = origin
        if decoder.streaming is not None:
            decoder.origin_by_xid[decoder.streaming] = origin

        return origin


@cached_msg_dataclass
class ColumnDefinition:
    """Column metadata from Relation message."""

    flags: int
    name: str
    type_id: int
    type_modifier: int

    @property
    def is_key(self) -> bool:
        """Check if column is part of replica identity."""
        return bool(self.flags & 0x01)


def _parse_relation(
    data: Buffer, is_streaming: bool, encoding: str, column_cls: type[ColumnDefinition]
) -> tuple[int | None, int, str, str, str, tuple[ColumnDefinition, ...]]:
    # Relation ID (4 bytes)
    offset = 4

    if not is_streaming:
        xid = None
        (relation_id,) = unpack_relation_id(data[0:offset])
    else:
        offset += 4
        xid, relation_id = unpack_xid_and_relation_id(data[0:offset])

    # Namespace name (null-terminated)
    namespace, offset = _read_null_terminated_string(data, offset, encoding=encoding)

    # Relation name (null-terminated)
    relation_name, offset = _read_null_terminated_string(
        data, offset, encoding=encoding
    )

    # Replica identity (1 byte) + number of columns (2 bytes)
    replica_identity, ncolumns = unpack_replica_identity_and_ncolumns(
        data[offset : offset + 3]
    )
    offset += 3

    # Parse columns
    columns = []
    for _ in range(ncolumns):
        # Column flags (1 byte)
        (flags,) = unpack_column_flags(data[offset : offset + 1])
        offset += 1

        # Column name (null-terminated)
        col_name, offset = _read_null_terminated_string(data, offset, encoding=encoding)

        # Type ID (4 bytes) + Type modifier (4 bytes)
        type_id, type_modifier = unpack_column_type(data[offset : offset + 8])
        offset += 8

        columns.append(
            column_cls(
                flags=flags,
                name=col_name,
                type_id=type_id,
                type_modifier=type_modifier,
            )
        )

    return (
        xid,
        relation_id,
        namespace,
        relation_name,
        chr(replica_identity),
        tuple(columns),
    )


@cached_msg_dataclass
class RelationMessage(PgOutputMessage):
    """Relation (table schema) message."""

    msg_type = MessageType.RELATION
    msg_type_name = MessageType(MessageType.RELATION).name

    xid: int | None = field(compare=False)
    relation_id: int
    namespace: str
    relation_name: str
    replica_identity: ReplicaIdentity
    columns: tuple[ColumnDefinition, ...]

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Relation message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int32: Relation ID
        * String: Namespace (null-terminated)
        * String: Relation name (null-terminated)
        * Int8: Replica identity setting
        * Int16: Number of columns
        * Next, for each column:

          * Int8: Flags
          * String: Column name (null-terminated)
          * Int32: Column type OID
          * Int32: Type modifier

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-RELATION
        """  # noqa: E501
        xid, relation_id, namespace, relation_name, replica_identity, columns = (
            parse_relation(
                data,
                decoder.streaming is not None,
                decoder.server_encoding,
                ColumnDefinition,
            )
        )
        relation = cls(
            xid=xid,
            relation_id=relation_id,
            namespace=namespace,
            relation_name=relation_name,
            replica_identity=ReplicaIdentity(replica_identity),
            columns=columns,
        )

        if xid is None:
            decoder.relations[relation_id] = relation
        else:
            assert decoder.streaming  # FIXME: what type of error?
            decoder.relations_by_xid[decoder.streaming][relation_id] = relation

        return relation


def _parse_type(
    data: Buffer, is_streaming: bool, encoding: str
) -> tuple[int | None, int, str, str]:
    # Type ID (4 bytes)
    offset = 4

    if not is_streaming:
        xid = None
        (type_id,) = unpack_type_id(data[0:offset])
    else:
        offset += 4
        xid, type_id = unpack_xid_and_type_id(data[0:offset])

    namespace, offset = _read_null_terminated_string(data, offset, encoding=encoding)
    name, _ = _read_null_terminated_string(data, offset, encoding=encoding)

    return xid, type_id, namespace, name


@cached_msg_dataclass
class TypeMessage(PgOutputMessage):
    """Type message - contains information about a data type."""

    msg_type = MessageType.TYPE
    msg_type_name = MessageType(MessageType.TYPE).name

    xid: int | None
    type_id: int
    namespace: str
    name: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Type message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int32: OID of the data type
        * String: Namespace (null-terminated)
        * String: Name of the data type (null-terminated)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TYPE
        """  # noqa: E501

        xid, type_id, namespace, name = parse_type(
            data, decoder.streaming is not None, decoder.server_encoding
        )
        type_ = cls(xid=xid, type_id=type_id, namespace=namespace, name=name)

        if xid is None:
            decoder.types[type_id] = type_
        else:
            assert decoder.streaming  # FIXME: what type of error?
            decoder.types_by_xid[decoder.streaming][type_id] = type_

        return type_


@msg_dataclass
class InsertMessage(PgOutputMessage[LogicalRow]):
    msg_type = MessageType.INSERT
    msg_type_name = MessageType(MessageType.INSERT).name

    xid: int | None
    relation: RelationMessage
    # See comment in XLogDataMessage for type: ignore
    new_tuple: LogicalRow  # type: ignore[misc]

    @property
    def relation_id(self) -> int:
        return self.relation.relation_id

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder[LogicalRow],
        data: Buffer,
    ) -> Self:
        """Decode Insert message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int32: Relation ID
        * Byte1: 'N' (identifies new tuple follows)
        * TupleData: New tuple data

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-INSERT
        """  # noqa: E501
        # Relation ID (4 bytes)
        offset = 4

        if decoder.streaming is None:
            xid = None
            (relation_id,) = unpack_relation_id(data[0:offset])
        else:
            offset += 4
            xid, relation_id = unpack_xid_and_relation_id(data[0:offset])

        relation = decoder.get_relation(relation_id)

        # Tuple type identifier (1 byte) - should be 'N'
        tuple_type = chr(data[offset])
        offset += 1

        if tuple_type != "N":
            raise e.DataError(f"Expected 'N' for new tuple, got {tuple_type}")

        new_tuple, offset = _create_row(decoder, relation, data, offset)

        return cls(xid=xid, relation=relation, new_tuple=new_tuple)


@msg_dataclass
class UpdateMessage(PgOutputMessage[LogicalRow]):
    msg_type = MessageType.UPDATE
    msg_type_name = MessageType(MessageType.UPDATE).name

    xid: int | None
    relation: RelationMessage
    target_tuple: LogicalRow | None
    target_tuple_type: Literal["K", "O"] | None
    # See comment in XLogDataMessage for type: ignore
    new_tuple: LogicalRow  # type: ignore[misc]

    @property
    def relation_id(self) -> int:
        return self.relation.relation_id

    @property
    def key_tuple(self) -> LogicalRow | None:
        if self.target_tuple_type == "K":
            return self.target_tuple
        return None

    @property
    def old_tuple(self) -> LogicalRow | None:
        if self.target_tuple_type == "O":
            return self.target_tuple
        return None

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder[LogicalRow],
        data: Buffer,
    ) -> Self:
        """Decode Update message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int32: Relation ID
        * Byte1: 'K' (old key tuple) or 'O' (old tuple) or 'N' (new tuple)
        * TupleData: (if 'K' or 'O')
        * Byte1: 'N' (identifies new tuple)
        * TupleData: New tuple data

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE
        """  # noqa: E501
        # Relation ID (4 bytes)
        offset = 4

        if decoder.streaming is None:
            xid = None
            (relation_id,) = unpack_relation_id(data[0:offset])
        else:
            offset += 4
            xid, relation_id = unpack_xid_and_relation_id(data[0:offset])

        relation = decoder.get_relation(relation_id)

        # Check for old or key tuple
        target_tuple = None
        target_tuple_type: Literal["O", "K"] | None = None
        tuple_type = chr(data[offset])
        offset += 1

        if tuple_type in ("K", "O"):
            # Old or Key tuple
            target_tuple, offset = _create_row(
                decoder,
                relation,
                data,
                offset,
            )
            target_tuple_type = cast(Literal["K", "O"], tuple_type)
            tuple_type = chr(data[offset])
            offset += 1
        elif tuple_type != "N":
            raise e.DataError(f"Expected 'O' or 'K' or 'N', got {tuple_type}")

        if tuple_type != "N":
            raise e.DataError(f"Expected 'N' for new tuple, got {tuple_type}")

        new_tuple, offset = _create_row(
            decoder,
            relation,
            data,
            offset,
        )

        return cls(
            xid=xid,
            relation=relation,
            target_tuple=target_tuple,
            target_tuple_type=target_tuple_type,
            new_tuple=new_tuple,
        )


@msg_dataclass
class DeleteMessage(PgOutputMessage[LogicalRow]):
    """Delete operation message."""

    msg_type = MessageType.DELETE
    msg_type_name = MessageType(MessageType.DELETE).name

    xid: int | None
    relation: RelationMessage
    # See comment in XLogDataMessage for type: ignore
    target_tuple: LogicalRow  # type: ignore[misc]
    target_tuple_type: Literal["K", "O"]

    @property
    def relation_id(self) -> int:
        return self.relation.relation_id

    @property
    def key_tuple(self) -> LogicalRow | None:
        if self.target_tuple_type == "K":
            return self.target_tuple
        return None

    @property
    def old_tuple(self) -> LogicalRow | None:
        if self.target_tuple_type == "O":
            return self.target_tuple
        return None

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder[LogicalRow],
        data: Buffer,
    ) -> Self:
        """Decode Delete message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int32: Relation ID
        * Byte1: 'K' (old key tuple) or 'O' (old tuple)
        * TupleData: Old or key tuple data

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-DELETE
        """  # noqa: E501
        # Relation ID (4 bytes)
        offset = 4

        if decoder.streaming is None:
            xid = None
            (relation_id,) = unpack_relation_id(data[0:offset])
        else:
            offset += 4
            xid, relation_id = unpack_xid_and_relation_id(data[0:offset])

        relation = decoder.get_relation(relation_id)

        # Tuple type identifier (1 byte) - 'K' or 'O'
        tuple_type = cast(Literal["O", "K"], chr(data[offset]))
        offset += 1

        if tuple_type in ("K", "O"):
            # Key or Old tuple
            target_tuple, offset = _create_row(
                decoder,
                relation,
                data,
                offset,
            )
        else:
            raise e.DataError(f"Expected 'K' or 'O' for delete tuple, got {tuple_type}")

        return cls(
            xid=xid,
            relation=relation,
            target_tuple=target_tuple,
            target_tuple_type=tuple_type,
        )


def _parse_truncate(
    data: Buffer, is_streaming: bool
) -> tuple[int | None, int, list[int]]:
    # Number of relations (4 bytes) + Options (1 byte)
    offset = 5

    if not is_streaming:
        xid = None
        nrelations, options = unpack_truncate_header(data[0:offset])
    else:
        offset += 4
        xid, nrelations, options = unpack_streaming_truncate_header(data[0:offset])

    # Parse relation IDs
    relation_ids = []
    for _ in range(nrelations):
        (rel_id,) = unpack_relation_id(data[offset : offset + 4])
        offset += 4
        relation_ids.append(rel_id)

    return xid, options, relation_ids


@msg_dataclass
class TruncateMessage(PgOutputMessage):
    msg_type = MessageType.TRUNCATE
    msg_type_name = MessageType(MessageType.TRUNCATE).name

    xid: int | None
    _options: int
    relations: list[RelationMessage]

    @property
    def relation_ids(self) -> list[int]:
        return [relation.relation_id for relation in self.relations]

    @property
    def options(self) -> list[str]:
        options = []
        if self.cascade:
            options.append("CASCADE")
        if self.restart_identity:
            options.append("RESTART_IDENTITY")
        return options

    @property
    def cascade(self) -> bool:
        return bool(self._options & 0x01)

    @property
    def restart_identity(self) -> bool:
        return bool(self._options & 0x02)

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Truncate message.

        Format (from `PostgreSQL docs`__):

        * [Int32: XID when streaming]
        * Int32: Number of relations
        * Int8: Option bits (CASCADE, RESTART IDENTITY)
        * Int32[]: Array of relation IDs

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TRUNCATE
        """  # noqa: E501

        xid, options, relation_ids = parse_truncate(data, decoder.streaming is not None)
        return cls(
            xid=xid,
            _options=options,
            relations=[
                decoder.get_relation(relation_id) for relation_id in relation_ids
            ],
        )


@msg_dataclass
class StreamStartMessage(PgOutputMessage):
    """Stream start message - begins a streaming transaction."""

    msg_type = MessageType.STREAM_START
    msg_type_name = MessageType(MessageType.STREAM_START).name

    xid: int
    first_segment: bool

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Stream Start message.

        Format (from `PostgreSQL docs`__):

        * Int32: Transaction ID (xid)
        * Int8: Flags (1 = first segment of transaction)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-STREAM-START
        """  # noqa: E501
        xid, flags = unpack_stream_start(data)
        first_segment = bool(flags & 1)
        if decoder.streaming is not None:
            raise e.DataError(
                "Shouldn't see a Stream Start Message while already streaming"
            )
        decoder.streaming = xid
        if first_segment:
            decoder.origin = None
        else:
            decoder.origin = decoder.origin_by_xid.get(xid)

        return cls(xid=xid, first_segment=first_segment)


class StreamStopMessage(PgOutputMessage):
    """Stream stop message - ends a streaming transaction."""

    __slots__ = ("__weakref__",)
    msg_type = MessageType.STREAM_STOP
    msg_type_name = MessageType(MessageType.STREAM_STOP).name

    _instance: StreamStopMessage

    def __new__(cls: type[StreamStopMessage]) -> StreamStopMessage:
        return cls._instance

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Stream Stop message.

        Format (from `PostgreSQL docs`__):

        * (no data)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-STREAM-STOP
        """  # noqa: E501
        decoder.streaming = None
        return cls()


StreamStopMessage._instance = object.__new__(StreamStopMessage)


@msg_dataclass
class StreamCommitMessage(PgOutputMessage):
    """Stream commit message - commits a streaming transaction."""

    msg_type = MessageType.STREAM_COMMIT
    msg_type_name = MessageType(MessageType.STREAM_COMMIT).name

    xid: int
    commit_lsn: int
    end_lsn: int
    commit_ts: datetime

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Stream Commit message.

        Format (from `PostgreSQL docs`__):

        * Int32: Transaction ID (xid)
        * Int8: Flags (currently unused, always 0)
        * Int64: LSN of the commit
        * Int64: LSN of the end of the transaction
        * Int64: Commit timestamp (microseconds since PostgreSQL epoch)

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-STREAM-COMMIT
        """  # noqa: E501
        xid, _, commit_lsn, end_lsn, commit_ts_micro = unpack_stream_commit(data)

        decoder.relations_by_xid.pop(xid, None)
        decoder.types_by_xid.pop(xid, None)
        decoder.origin_by_xid.pop(xid, None)

        return cls(
            xid=xid,
            commit_lsn=commit_lsn,
            end_lsn=end_lsn,
            commit_ts=pg_epoch_to_datetime(commit_ts_micro),
        )


@msg_dataclass
class StreamAbortMessage(PgOutputMessage):
    """Stream abort message - aborts a streaming transaction."""

    msg_type = MessageType.STREAM_ABORT
    msg_type_name = MessageType(MessageType.STREAM_ABORT).name

    xid: int
    subxid: int
    abort_lsn: int | None
    abort_ts: datetime | None

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Stream Abort message.

        Format (from `PostgreSQL docs`__):

        * Int32: Xid of the transaction
        * Int32: Xid of subtransaction (same as above for top-level xacts)
        * [Int64: The LSN of the abort operation, present when streaming=parallel]
        * [Int64: Abort timestamp of the transaction, present when streaming=parallel]

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-STREAM-ABORT
        """  # noqa: E501
        if decoder.plugin_options["streaming"] != "parallel":
            xid, subxid = unpack_stream_abort(data)
            abort_lsn = abort_ts = None
        else:
            xid, subxid, abort_lsn, abort_ts_micro = unpack_stream_abort_parallel(data)
            abort_ts = pg_epoch_to_datetime(abort_ts_micro)

        if xid == subxid:
            decoder.relations_by_xid.pop(xid, None)
            decoder.types_by_xid.pop(xid, None)
            decoder.origin_by_xid.pop(xid, None)

        return cls(xid=xid, subxid=subxid, abort_lsn=abort_lsn, abort_ts=abort_ts)


@msg_dataclass
class BeginPrepareMessage(PgOutputMessage):
    msg_type = MessageType.BEGIN_PREPARE
    msg_type_name = MessageType(MessageType.BEGIN_PREPARE).name

    prepare_lsn: int
    end_lsn: int
    prepare_ts: datetime
    xid: int
    transaction_name: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Begin Prepare message

        Format (from `PostgreSQL docs`__):

        * Int64: The LSN of the prepare
        * Int64: The end LSN of the prepared transaction
        * Int64: Prepare timestamp of transaction (microseconds since PostgreSQL epoch)
        * Int32: Xid of the transaction
        * String: The user defined GID of the prepared transaction.

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-BEGIN-PREPARE
        """  # noqa: E501
        offset = 3 * 8 + 4
        prepare_lsn, end_lsn, prepare_ts_micro, xid = unpack_begin_prepare(
            data[0:offset]
        )
        transaction_name = str(
            data[offset:-1], decoder.server_encoding  # Leave off the null char
        )

        decoder.origin = None

        return cls(
            prepare_lsn=prepare_lsn,
            end_lsn=end_lsn,
            prepare_ts=pg_epoch_to_datetime(prepare_ts_micro),
            xid=xid,
            transaction_name=transaction_name,
        )


@msg_dataclass
class PrepareMessage(PgOutputMessage):
    msg_type = MessageType.PREPARE
    msg_type_name = MessageType(MessageType.PREPARE).name

    prepare_lsn: int
    end_lsn: int
    prepare_ts: datetime
    xid: int
    transaction_name: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Prepare message

        Format (from `PostgreSQL docs`__):

        * Int8(0): Flags; currently unused.
        * Int64: The LSN of the prepare.
        * Int64: The end LSN of the prepared transaction.
        * Int64: Prepare timestamp of transaction (microseconds since PostgreSQL epoch)
        * Int32: Xid of the transaction.
        * String: The user defined GID of the prepared transaction.

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-PREPARE
        """  # noqa: E501
        offset = 1 + 3 * 8 + 4
        _, prepare_lsn, end_lsn, prepare_ts_micro, xid = unpack_prepare(data[0:offset])
        transaction_name = str(
            data[offset:-1], decoder.server_encoding  # Leave off the null char
        )

        return cls(
            prepare_lsn=prepare_lsn,
            end_lsn=end_lsn,
            prepare_ts=pg_epoch_to_datetime(prepare_ts_micro),
            xid=xid,
            transaction_name=transaction_name,
        )


@msg_dataclass
class CommitPreparedMessage(PgOutputMessage):
    msg_type = MessageType.COMMIT_PREPARED
    msg_type_name = MessageType(MessageType.COMMIT_PREPARED).name

    end_lsn: int
    commit_lsn: int
    commit_ts: datetime
    xid: int
    transaction_name: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Commit Prepared message

        Format (from `PostgreSQL docs`__):

        * Int8(0): Flags; currently unused.
        * Int64: The LSN of the commit of the prepared transaction.
        * Int64: The end LSN of the commit of the prepared transaction.
        * Int64: Commit timestamp of transaction (microseconds since PostgreSQL epoch)
        * Int32: Xid of the transaction.
        * String: The user defined GID of the prepared transaction.

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-COMMIT-PREPARED
        """  # noqa: E501
        offset = 1 + 3 * 8 + 4
        _, end_lsn, commit_lsn, commit_ts_micro, xid = unpack_commit_prepared(
            data[0:offset]
        )
        transaction_name = str(
            data[offset:-1], decoder.server_encoding  # Leave off the null char
        )

        msg = cls(
            end_lsn=end_lsn,
            commit_lsn=commit_lsn,
            commit_ts=pg_epoch_to_datetime(commit_ts_micro),
            xid=xid,
            transaction_name=transaction_name,
        )

        decoder.origin_by_xid.pop(msg.xid, None)

        return msg


@msg_dataclass
class RollbackPreparedMessage(PgOutputMessage):
    msg_type = MessageType.ROLLBACK_PREPARED
    msg_type_name = MessageType(MessageType.ROLLBACK_PREPARED).name

    end_lsn: int
    rollback_lsn: int
    prepare_ts: datetime
    rollback_ts: datetime
    xid: int
    transaction_name: str

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Rollback Prepared message

        Format (from `PostgreSQL docs`__):

        * Int8(0): Flags; currently unused.
        * Int64: The end LSN of the prepared transaction.
        * Int64: The end LSN of the rollback of the prepared transaction.
        * Int64: Prepare timestamp of transaction (microseconds since PostgreSQL epoch)
        * Int64: Rollback timestamp of transaction (microseconds since PostgreSQL epoch)
        * Int32: Xid of the transaction.
        * String: The user defined GID of the prepared transaction.

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-ROLLBACK-PREPARED
        """  # noqa: E501
        offset = 1 + 4 * 8 + 4
        (
            _,
            end_lsn,
            rollback_lsn,
            prepare_ts_micro,
            rollback_ts_micro,
            xid,
        ) = unpack_rollback_prepared(data[0:offset])
        transaction_name = str(
            data[offset:-1], decoder.server_encoding  # Leave off the null char
        )

        msg = cls(
            end_lsn=end_lsn,
            rollback_lsn=rollback_lsn,
            prepare_ts=pg_epoch_to_datetime(prepare_ts_micro),
            rollback_ts=pg_epoch_to_datetime(rollback_ts_micro),
            xid=xid,
            transaction_name=transaction_name,
        )

        decoder.origin_by_xid.pop(msg.xid, None)

        return msg


@msg_dataclass
class StreamPrepareMessage(PrepareMessage):
    msg_type = MessageType.STREAM_PREPARE
    msg_type_name = MessageType(MessageType.STREAM_PREPARE).name

    @classmethod
    def decode(
        cls,
        decoder: PgOutputDecoder,
        data: Buffer,
    ) -> Self:
        """Decode Stream Prepare message

        Format (from `PostgreSQL docs`__):

        * Int8(0): Flags; currently unused.
        * Int64: The LSN of the prepare.
        * Int64: The end LSN of the prepared transaction.
        * Int64: Prepare timestamp of transaction (microseconds since PostgreSQL epoch)
        * Int32: Xid of the transaction.
        * String: The user defined GID of the prepared transaction.

        .. __: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-STREAM-PREPARE
        """  # noqa: E501
        msg = super().decode(decoder, data)

        decoder.relations_by_xid.pop(msg.xid, None)
        decoder.types_by_xid.pop(msg.xid, None)

        return msg


# Override functions with fast versions if available
if _psycopg:
    parse_tuple_data = _psycopg.parse_pgoutput_row
    parse_emit_message = _psycopg.parse_emit_message
    unpack_begin = _psycopg.unpack_begin
    unpack_commit = _psycopg.unpack_commit
    parse_relation = _psycopg.parse_relation
    parse_type = _psycopg.parse_type
    parse_truncate = _psycopg.parse_truncate
    unpack_stream_start = _psycopg.unpack_stream_start
    unpack_stream_commit = _psycopg.unpack_stream_commit
    unpack_stream_abort = _psycopg.unpack_stream_abort
    unpack_stream_abort_parallel = _psycopg.unpack_stream_abort_parallel
    unpack_begin_prepare = _psycopg.unpack_begin_prepare
    unpack_prepare = _psycopg.unpack_prepare
    unpack_commit_prepared = _psycopg.unpack_commit_prepared
    unpack_rollback_prepared = _psycopg.unpack_rollback_prepared
else:
    parse_tuple_data = _parse_tuple_data
    parse_emit_message = _parse_emit_message
    unpack_begin = _unpack_begin
    unpack_commit = _unpack_commit
    parse_relation = _parse_relation
    parse_type = _parse_type
    parse_truncate = _parse_truncate
    unpack_stream_start = _unpack_stream_start
    unpack_stream_commit = _unpack_stream_commit
    unpack_stream_abort = _unpack_stream_abort
    unpack_stream_abort_parallel = _unpack_stream_abort_parallel
    unpack_begin_prepare = _unpack_begin_prepare
    unpack_prepare = _unpack_prepare
    unpack_commit_prepared = _unpack_commit_prepared
    unpack_rollback_prepared = _unpack_rollback_prepared
