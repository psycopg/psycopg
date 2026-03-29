from __future__ import annotations

import time
import logging
import datetime
from typing import TYPE_CHECKING, Any, Callable, LiteralString, NoReturn, cast

from .. import errors as e
from .. import generators, sql
from ..rows import Row
from .._compat import Self
from ..client_cursor import AsyncClientCursor
from .replication_utils import PG_EPOCH
from .replication_options import ReplicationType, SnapshotOption
from .replication_messages import (
    PrimaryKeepaliveMessage,
    ReplicationMessage,
    StandbyStatusUpdate,
    XLogDataMessage,
    pack_standby_status_update,
    parse_primarykeepalive,
    parse_xlogdata,
)

if TYPE_CHECKING:
    from ..abc import Buffer, PQGen


logger = logging.getLogger("psycopg")


MIDNIGHT_JAN_2020_UNIX_EPOCH_MICROSECONDS = (
    int(
        (
            PG_EPOCH
            - datetime.datetime(day=1, month=1, year=1970, tzinfo=datetime.timezone.utc)
        ).total_seconds()
    )
    * 1_000_000
)


class AsyncBaseReplicationCursor(AsyncClientCursor[Row]):
    """
    Base class implementing the replication commands shared between logical
    and physical replication connections.

    See `the PostgreSQL replication protocol documentation
    <https://www.postgresql.org/docs/current/protocol-replication.html>`_ for
    details.
    """

    __module__ = "psycopg.replication"

    replication_type: ReplicationType | None = None

    last_flushed_lsn: int = 0
    last_applied_lsn: int = 0
    _last_received_lsn: int = 0
    _last_feedback_time: float = 0.0

    async def identify_system(self) -> Row:
        await self.execute("IDENTIFY_SYSTEM")
        return cast(Row, await self.fetchone())

    async def show(self, name: str) -> Row | None:
        await self.execute(sql.SQL("SHOW {name}").format(name=sql.BareIdentifier(name)))
        return await self.fetchone()

    async def timeline_history(self, timeline: int) -> Row:
        """
        Returns a single row containing (filename, history)
        Raises psycopg.errors.UndefinedFile if timeline history doesn't exist.
        Cannot run in a transaction
        """
        await self.execute("TIMELINE_HISTORY %s", [timeline])
        return cast(Row, await self.fetchone())

    async def create_replication_slot(
        self,
        slot_name: str,
        replication_type: ReplicationType | None = None,
        *,
        temporary: bool = False,
        reserve_wal: bool | None = None,
        output_plugin: str = "pgoutput",
        two_phase: bool | None = None,
        snapshot: SnapshotOption | None = None,
        failover: bool | None = None,
    ) -> Row:
        """
        Create a replication slot of type `replication_type`.

        For `"physical"` slots, the `output_plugin`, `two_phase`, `snapshot`, and
        `failover` parameters are ignored.

        For `"logical"` slots, the `reserve_wal` parameter is ignored.

        `replication_type` defaults to self.replication_type:

        * `"physical"` for physical replication cursors.

        * `"logical"` for logical replication cursors.
        """
        if replication_type is None:
            replication_type = self.replication_type
        if replication_type is None:
            raise TypeError(
                f"{type(self).__name__}.create_replication_slot() missing 1 required"
                + " argument: 'replication_type'"
            )

        snips: list[sql.Composable] = [
            sql.SQL(
                "CREATE_REPLICATION_SLOT {slot_name}"
                + (" TEMPORARY" if temporary else "")
                + (
                    " PHYSICAL"
                    if replication_type == ReplicationType.PHYSICAL
                    else " LOGICAL"
                )
            ).format(slot_name=sql.BareIdentifier(slot_name))
        ]
        server_version = self._conn.info.server_version
        # TASK: when PostgreSQL 14 is out of support, rip out old calling style
        # See https://www.postgresql.org/docs/18/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-REPLICATION-SLOT  # noqa: E501
        if replication_type == ReplicationType.PHYSICAL:
            # 15 added support for new calling style with options
            if server_version >= 150000:
                options: dict[LiteralString, str] = {}
                if reserve_wal is not None:
                    options["RESERVE_WAL"] = str(bool(reserve_wal)).lower()
                if options:
                    snips.append(
                        sql.SQL("({options})").format(
                            options=self._format_sql_options(options)
                        )
                    )
            else:
                if reserve_wal is not None and reserve_wal:
                    snips.append(sql.SQL("RESERVE_WAL"))
        if replication_type == ReplicationType.LOGICAL:
            snips.append(sql.BareIdentifier(output_plugin))
            # 15 added support for new calling style with options
            if server_version >= 150000:
                options = {}
                if snapshot is not None:
                    if snapshot not in SnapshotOption.all_values:
                        # DISCUSS: let PostgreSQL handle this case?
                        raise ValueError(
                            "Unsupported 'snapshot' argument to "
                            + "'create_replication_slot()'"
                        )
                    options["SNAPSHOT"] = snapshot
                if two_phase is not None:
                    options["TWO_PHASE"] = str(bool(two_phase)).lower()
                if failover is not None:
                    # NOTE: only supported for PostgreSQL >= 17, but we assume the
                    # server will give us an appropriate error.
                    options["FAILOVER"] = str(bool(failover)).lower()
                if options:
                    snips.append(
                        sql.SQL("({options})").format(
                            options=self._format_sql_options(options)
                        )
                    )
            else:
                if snapshot is not None:
                    if snapshot == "export":
                        snips.append(sql.SQL("EXPORT_SNAPSHOT"))
                    elif snapshot == "use":
                        snips.append(sql.SQL("USE_SNAPSHOT"))
                    elif snapshot == "nothing":
                        snips.append(sql.SQL("NOEXPORT_SNAPSHOT"))
                    else:
                        raise ValueError(
                            "Unsupported 'snapshot' argument to "
                            + "'create_replication_slot()'"
                        )
                if two_phase is not None and two_phase:
                    snips.append(sql.SQL("TWO_PHASE"))
                if failover:
                    # NOTE: Server can't give us an appropriate error in this case,
                    # as it's a syntax error in the old calling form.
                    self._raise_unsupported_error(
                        "<15", "FAILOVER", "CREATE_REPLICATION_SLOT"
                    )

        statement = sql.SQL(" ").join(snips)
        await self.execute(statement)
        return cast(Row, await self.fetchone())

    async def drop_replication_slot(
        self, slot_name: str, wait: bool | None = None
    ) -> Self:
        return await self.execute(
            sql.SQL(
                "DROP_REPLICATION_SLOT {slot_name}" + (" WAIT" if wait else "")
            ).format(slot_name=sql.BareIdentifier(slot_name))
        )

    def _format_sql_options(self, options: dict[LiteralString, str]) -> sql.Composed:
        return sql.SQL(", ").join(
            sql.SQL("{opt_name} {opt}").format(opt_name=sql.SQL(opt_name), opt=str(opt))
            for opt_name, opt in options.items()
        )

    def _read_gen(self, timeout: float = 0.0) -> PQGen[memoryview]:
        res = yield from generators.fetch_replication_messages(
            self._pgconn, timeout=timeout
        )

        if isinstance(res, memoryview):
            return res

        return memoryview(b"")

    async def read_message(
        self,
        auto_reply: bool = True,
        auto_flushed: bool = False,
        return_keepalive_messages: bool = True,
        timeout: float = 0.0,
    ) -> ReplicationMessage:
        while True:
            data: Buffer = await self._conn.wait_no_cancel(
                self._read_gen(timeout=timeout)
            )

            # FIXME: handle the case of alternative timelines
            # where the stream is not infinite.
            msg_type = chr(data[0])

            if msg_type == "w":  # XLogData
                # See https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA  # noqa: E501
                wal_data = data[25:]
                data_start, wal_end, microseconds_since_2000 = parse_xlogdata(
                    data[1:25]
                )
                self._last_received_lsn = data_start
                if auto_flushed:
                    self.last_flushed_lsn = data_start
                return XLogDataMessage(
                    wal_data, data_start, wal_end, microseconds_since_2000
                )
            elif msg_type == "k":  # Primary keepalive message
                # See https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE  # noqa: E501
                wal_end, microseconds_since_2000, reply_asap = parse_primarykeepalive(
                    data[1:]
                )

                if auto_reply and reply_asap:
                    # Send a reply to keep the connection alive
                    await self.send_feedback(
                        flushed_lsn=self.last_flushed_lsn,
                        received_lsn=self._last_received_lsn,
                    )
                if return_keepalive_messages:
                    return PrimaryKeepaliveMessage(
                        wal_end, bool(reply_asap), microseconds_since_2000
                    )
            else:
                raise e.DataError(
                    "Received an unexpected replication message type from the server: "
                    + f"{msg_type}"
                )

    async def send_feedback(
        self,
        flushed_lsn: int | None = None,
        received_lsn: int | None = None,
        applied_lsn: int | None = None,
        request_reply: bool = False,
    ) -> StandbyStatusUpdate:
        # flushed lsn dictates when the wal is no longer reserved for this slot
        # consumer if using a slot.
        if flushed_lsn is None:
            flushed_lsn = self.last_flushed_lsn
        if received_lsn is None:
            received_lsn = self._last_received_lsn
        if applied_lsn is None:
            applied_lsn = self.last_applied_lsn

        time_sent = (time.time_ns() // 1000) - MIDNIGHT_JAN_2020_UNIX_EPOCH_MICROSECONDS
        status_msg = pack_standby_status_update(
            b"r",
            received_lsn,
            flushed_lsn,
            applied_lsn,
            time_sent,
            request_reply,
        )
        await self._conn.wait_no_cancel(generators.copy_to(self._pgconn, status_msg))
        self._last_feedback_time = time.monotonic()
        self.last_flushed_lsn = max(self.last_flushed_lsn, flushed_lsn)
        self.last_applied_lsn = max(self.last_applied_lsn, applied_lsn)

        return StandbyStatusUpdate(
            written_lsn=received_lsn,
            flushed_lsn=flushed_lsn,
            applied_lsn=applied_lsn,
            send_time_microseconds_since_2000=time_sent,
            reply_asap=request_reply,
        )

    async def consume_stream(
        self,
        consume: Callable[
            [
                AsyncBaseReplicationCursor[Row],
                ReplicationMessage,
            ],
            Any,
        ],
        feedback_interval: float | bool = 10,
        auto_reply: bool = True,
        flushes: bool = True,
        applies: bool = True,
        consume_all_messages: bool = False,
    ) -> None:
        while True:
            msg = await self.read_message(
                auto_reply=auto_reply,
                auto_flushed=False,
                return_keepalive_messages=consume_all_messages,
            )
            await consume(self, msg)
            if not consume_all_messages or isinstance(msg, XLogDataMessage):
                msg = cast(XLogDataMessage, msg)
                flush_lsn = None
                apply_lsn = None
                if flushes:
                    flush_lsn = msg.data_start
                if applies:
                    apply_lsn = msg.data_start
                if feedback_interval and (
                    feedback_interval is True
                    or self._last_feedback_time < time.monotonic() - feedback_interval
                ):
                    await self.send_feedback(
                        flushed_lsn=flush_lsn, applied_lsn=apply_lsn
                    )

    def _raise_unsupported_error(
        self, version_str: str, argument: str, cmd: str
    ) -> NoReturn:
        raise ValueError(
            f"PostgreSQL {version_str} does not support argument"
            + f" '{argument}' to '{cmd}'"
        )
