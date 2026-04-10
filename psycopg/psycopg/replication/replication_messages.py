from __future__ import annotations

from struct import Struct
from typing import Callable, ClassVar, Generic, Literal, TypeVar, cast
from datetime import datetime
from dataclasses import dataclass

from ..abc import Buffer
from .replication_utils import lsn_to_string, pg_epoch_to_datetime

DecodedPayload = TypeVar("DecodedPayload", covariant=True)


class ReplicationMessage:
    """
    Base class for all specific top-level replication messages.
    """

    __module__ = "psycopg.replication"
    __slots__ = ()

    message_type: ClassVar[Literal["w", "k", "r"]]


@dataclass(slots=True, weakref_slot=True)
class XLogDataMessage(ReplicationMessage, Generic[DecodedPayload]):
    """
    Class representing WAL data delivered by a replication stream.
    `!XLogDataMessage.payload` is the actual data and varies
    based on whether this is a message from physical or logical decoding and which
    logical output plugin (and corresponding decoder) is in use.
    """

    __module__ = "psycopg.replication"

    message_type = "w"

    # NOTE: mypy doesn't play nice with generics in dataclasses, we get:
    # `error: Cannot use a covariant type variable as a parameter  [misc]`.
    # But, payload is only set on creation, so this should be fine.
    # See https://discuss.python.org/t/make-replace-stop-interfering-with-variance-inference/96092/40  # noqa: E501
    payload: DecodedPayload  # type: ignore[misc]
    data_start: int  # LSN where this data starts
    wal_end: int  # Last LSN on the server
    send_time_microseconds_since_2000: int  # Server send timestamp

    @property
    def send_time(self) -> datetime:
        return pg_epoch_to_datetime(microseconds=self.send_time_microseconds_since_2000)


@dataclass(slots=True, weakref_slot=True)
class PrimaryKeepaliveMessage(ReplicationMessage):
    """
    Class representing the keepalive message delivered by a replication stream.
    """

    __module__ = "psycopg.replication"

    message_type = "k"

    wal_end: int  # Last LSN on the server
    reply_asap: bool  # Server requests a reply as soon as possible
    send_time_microseconds_since_2000: int  # Server send timestamp

    @property
    def send_time(self) -> datetime:
        return pg_epoch_to_datetime(microseconds=self.send_time_microseconds_since_2000)


@dataclass(slots=True, weakref_slot=True)
class StandbyStatusUpdate(ReplicationMessage):
    """
    Class representing status updates sent to the server during replication,
    returned by `~psycopg.replication.BaseReplicationCursor.send_feedback()`
    (and its async variant).
    """

    # This one is only sent, so strictly unnecessary, but we return it from
    # `BaseReplicationCursor.send_feedback()` for convenience
    __module__ = "psycopg.replication"

    message_type = "r"

    written_lsn: int
    flushed_lsn: int
    applied_lsn: int
    send_time_microseconds_since_2000: int
    reply_asap: bool

    @property
    def written_lsn_str(self) -> str:
        return lsn_to_string(self.written_lsn)

    @property
    def flushed_lsn_str(self) -> str:
        return lsn_to_string(self.flushed_lsn)

    @property
    def applied_lsn_str(self) -> str:
        return lsn_to_string(self.applied_lsn)

    @property
    def send_time(self) -> datetime:
        return pg_epoch_to_datetime(microseconds=self.send_time_microseconds_since_2000)


def parse_xlogdata(data: Buffer) -> tuple[int, int, int]:
    # data_start, wal_end, microseconds
    return unpack_xlogdata(data)


def parse_primarykeepalive(data: Buffer) -> tuple[int, int, bool]:
    wal_end, microseconds_since_2000, reply_asap = unpack_primary_keepalive_message(
        data
    )
    reply_asap = bool(reply_asap)

    return wal_end, microseconds_since_2000, reply_asap


_standby_status_update_struct = Struct(">cQQQQB")
pack_standby_status_update = cast(
    Callable[[bytes, int, int, int, int, bool], bytes],
    _standby_status_update_struct.pack,
)

_xlogdata_struct = Struct(">QQQ")
unpack_xlogdata = cast(
    Callable[[Buffer], tuple[int, int, int]], _xlogdata_struct.unpack
)

_primary_keepalive_message_struct = Struct(">QQB")
unpack_primary_keepalive_message = cast(
    Callable[[Buffer], tuple[int, int, int]], _primary_keepalive_message_struct.unpack
)
