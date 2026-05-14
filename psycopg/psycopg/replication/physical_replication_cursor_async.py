from __future__ import annotations

import time
import logging
from typing import cast

from .. import sql
from .abc import XLogDataDecoder
from ..rows import Row, tuple_row
from .._compat import Self
from .replication_utils import lsn_to_string, string_to_lsn
from .replication_options import ReplicationType
from .replication_messages import DecodedPayload
from .base_replication_cursor_async import AsyncBaseReplicationCursor

logger = logging.getLogger("psycopg")


class AsyncPhysicalReplicationCursor(AsyncBaseReplicationCursor[Row]):
    __module__ = "psycopg.replication"

    replication_type = ReplicationType.PHYSICAL

    @staticmethod
    def _to_str(ascii_text: str | bytes) -> str:
        if isinstance(ascii_text, bytes):
            ascii_text = ascii_text.decode("ascii")
        return ascii_text

    async def start_replication(
        self,
        slot_name: str | None = None,
        start_lsn: int | str | None = None,
        timeline: int | None = None,
        decoder: XLogDataDecoder[DecodedPayload] | None = None,
    ) -> Self:
        """
        Start physical replication.

        Executes the
        `START_REPLICATION [ SLOT slot_name ] [ PHYSICAL ] XXX/XXX [ TIMELINE tli ]`
        command.

        `start_lsn` is required when no `slot_name` is given or on PostgreSQL versions
        less than 15.  Otherwise, it defaults to the `restart_lsn` of the named slot,
        obtained by calling `AsyncPhysicalReplicationCursor.read_replication_slot`

        .. note::
           The slot must be created with `reserve_wal=True` for the `restart_lsn` to
           exist.
        """
        if start_lsn is None:
            restart_lsn = None
            if slot_name is not None and self._conn.info.server_version >= 150000:
                async with AsyncPhysicalReplicationCursor(
                    self.connection, row_factory=tuple_row
                ) as cur:
                    _, restart_lsn, _ = await cur.read_replication_slot(slot_name)

            if restart_lsn is None:
                raise TypeError(
                    f"{type(self).__name__}.start_replication() missing 1 required"
                    + " argument: 'start_lsn'"
                )
            cast(str | bytes, restart_lsn)
            start_lsn = string_to_lsn(self._to_str(restart_lsn))
        elif isinstance(start_lsn, str):
            # NOTE: this sanitizes the input
            start_lsn = string_to_lsn(start_lsn)

        self.decode_xlogdata = decoder
        if decoder is not None:
            decoder.server_encoding = self._encoding

        if slot_name is not None:
            statement = sql.SQL(
                "START_REPLICATION SLOT {slot_name} PHYSICAL {start_lsn}"
            ).format(
                slot_name=sql.BareIdentifier(slot_name),
                start_lsn=sql.SQL(lsn_to_string(start_lsn)),
            )
        else:
            statement = sql.SQL("START_REPLICATION PHYSICAL {start_lsn}").format(
                start_lsn=sql.SQL(lsn_to_string(start_lsn))
            )

        if timeline is not None:
            statement += statement + sql.SQL(" TIMELINE {timeline}").format(
                timeline=sql.BareIdentifier(str(timeline))
            )

        await self.execute(statement)
        self._last_received_lsn = start_lsn
        self.last_flushed_lsn = start_lsn
        self.last_applied_lsn = start_lsn
        self._last_feedback_time = time.monotonic()

        return self

    async def read_replication_slot(self, slot_name: str) -> Row:
        """
        Read information about a replication slot.

        Executes the `READ_REPLICATION_SLOT slot_name` command and returns
        a row containing the slot type, restart lsn, and
        restart timeline. If the slot does not exist, all values will be `None`.

        See https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-READ-REPLICATION-SLOT
        """  # noqa: E501
        if self._conn.info.server_version < 150000:
            raise ValueError(
                "'read_replication_slot()' is not supported on this version of"
                + " PostgreSQL"
            )
        await self.execute(
            sql.SQL("READ_REPLICATION_SLOT {slot_name}").format(
                slot_name=sql.BareIdentifier(slot_name)
            )
        )

        return cast(Row, await self.fetchone())
