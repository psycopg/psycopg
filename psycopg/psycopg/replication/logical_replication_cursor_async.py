from __future__ import annotations

import time
import logging
from typing import Any, LiteralString, cast

from .. import sql
from ..rows import Row, tuple_row
from .._compat import Self
from .replication_utils import lsn_to_string, string_to_lsn
from .replication_options import ReplicationType
from .logical_output_plugins import get_output_plugin_options
from .base_replication_cursor_async import AsyncBaseReplicationCursor

logger = logging.getLogger("psycopg")


class AsyncLogicalReplicationCursor(AsyncBaseReplicationCursor[Row]):
    __module__ = "psycopg.replication"

    replication_type = ReplicationType.LOGICAL

    def _format_output_plugin_options(
        self, output_plugin_options: dict[str, str]
    ) -> sql.Composed:
        return sql.SQL(", ").join(
            sql.SQL("{opt_name} {opt}").format(
                opt_name=sql.Identifier(opt_name), opt=str(opt)
            )
            for opt_name, opt in output_plugin_options.items()
        )

    async def output_plugin_for_slot(self, slot_name: str) -> str:
        async with self._conn.cursor(row_factory=tuple_row) as cursor:
            await cursor.execute(
                "SELECT plugin FROM pg_replication_slots "
                + "WHERE slot_type = 'logical' AND slot_name = %s;",
                [slot_name],
            )
            result = await cursor.fetchall()
        if not result:
            raise ValueError(f"No logical replication slot named {slot_name}")
        output_plugin = cast(str, result[0][0])

        return output_plugin

    async def start_replication(
        self,
        slot_name: str,
        start_lsn: int | str = 0,
        raw_output_plugin_options: dict[str, str] | None = None,
        **output_plugin_options: Any,
    ) -> Self:

        if raw_output_plugin_options and output_plugin_options:
            raise TypeError(
                "Only one of 'raw_output_plugin_options' and 'output_plugin_options'"
                + " can be passed to start_replication()"
            )

        if isinstance(start_lsn, str):
            # NOTE: this validates the string format in a simple manner at the
            # expense of an unnecessary conversion.
            start_lsn = string_to_lsn(start_lsn)

        if output_plugin_options:
            self._output_plugin = await self.output_plugin_for_slot(slot_name)
            plugin_opts = get_output_plugin_options(self._output_plugin)(
                output_plugin_options,
            )
            plugin_opts.validate_opts()
            output_plugin_options = plugin_opts.string_opts
        else:
            output_plugin_options = raw_output_plugin_options or {}

        statement = sql.SQL(
            "START_REPLICATION SLOT {slot_name} LOGICAL {start_lsn}"
        ).format(
            slot_name=sql.BareIdentifier(slot_name),
            start_lsn=sql.SQL(lsn_to_string(start_lsn)),
        )

        if output_plugin_options:
            statement += sql.SQL(" ({options})").format(
                options=self._format_output_plugin_options(output_plugin_options)
            )
        await self.execute(statement)
        self._last_feedback_time = time.monotonic()
        self._last_received_lsn = start_lsn
        self.last_flushed_lsn = start_lsn
        self.last_applied_lsn = start_lsn

        return self

    async def alter_replication_slot(
        self,
        slot_name: str,
        two_phase: bool | None = None,
        failover: bool | None = None,
    ) -> Self:
        if self._conn.info.server_version < 170000:
            raise ValueError(
                "'alter_replication_slot()' is not supported on this version of"
                + " PostgreSQL"
            )
        options: dict[LiteralString, str] = {}
        if two_phase is not None:
            options["TWO_PHASE"] = str(bool(two_phase)).lower()
        if failover is not None:
            options["FAILOVER"] = str(bool(failover)).lower()

        statement = sql.SQL("ALTER_REPLICATION_SLOT {slot_name}").format(
            slot_name=sql.BareIdentifier(slot_name)
        )
        if options:
            statement += sql.SQL(" ({options})").format(
                options=self._format_sql_options(options)
            )

        return await self.execute(statement)
