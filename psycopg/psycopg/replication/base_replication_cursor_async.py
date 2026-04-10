from __future__ import annotations

import time
import logging
import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable, LiteralString, NoReturn, cast

from .. import errors as e
from .. import generators, pq, sql
from .abc import XLogDataDecoder
from ..rows import Row
from .._compat import Self
from ..client_cursor import AsyncClientCursor
from ..connection_async import _INTERRUPTED
from .replication_utils import PG_EPOCH
from .base_backup_options import (
    BackupTarget,
    CheckpointMode,
    CompressionMethod,
    ManifestChecksums,
    ManifestOption,
)
from .replication_options import ReplicationType, SnapshotOption
from .base_backup_messages import (
    BackupData,
    BackupManifestStart,
    BackupMessage,
    BackupNewArchive,
    BackupProgress,
    unpack_backup_progress,
)
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
    from ..abc import Buffer, PQGen, Query


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
    decode_xlogdata: XLogDataDecoder[Any] | None
    _base_backup_started: bool = False

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

                if self.decode_xlogdata is not None:
                    wal_data = self.decode_xlogdata(wal_data)
                    # decoder can filter which messages are delivered
                    if wal_data is None:
                        continue

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
                msg = cast(XLogDataMessage[Any], msg)
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

    async def upload_manifest(self, manifest_content: Iterable[Buffer]) -> Self:
        """
        Upload a backup manifest prior to calling start_base_backup with
        the incremental option.
        """
        async with self.copy("UPLOAD_MANIFEST") as copy:
            for chunk in manifest_content:
                await copy._write(chunk)
        return self

    def _check_base_backup_results(self, results: list[pq.abc.PGresult]) -> None:
        start_position, tablespace_data, copy_out = results
        if (
            start_position.status == pq.ExecStatus.TUPLES_OK
            and tablespace_data.status == pq.ExecStatus.TUPLES_OK
            and copy_out.status == pq.ExecStatus.COPY_OUT
        ):
            return
        else:
            raise e.DataError(
                "start_base_backup() should provide two TUPLES_OK and a COPY_OUT, "
                + f"got {", ".join(pq.ExecStatus(r.status).name for r in results)}"
            )

    # DISCUSS: modified from BaseCursor._start_copy_gen
    # not sure if all this is needed
    def _start_base_backup_gen(self, statement: Query) -> PQGen[None]:
        """Generator implementing sending a command for `Cursor.start_base_backup()."""

        if self._conn._pipeline:
            raise e.NotSupportedError("BASE_BACKUP cannot be used in pipeline mode")

        yield from self._start_query()

        query = self._convert_query(statement)

        self._execute_send(query, binary=False)
        if len(results := (yield from generators.execute(self._pgconn))) != 3:
            if len(results) == 1 and results[0].status == pq.ExecStatus.FATAL_ERROR:
                raise e.error_from_result(results[0], encoding=self._encoding)
            raise e.DataError("BASE_BACKUP should yield three results")

        self._check_base_backup_results(results)
        self._set_results(results)

    async def start_base_backup(
        self,
        label: str | None = None,
        target: BackupTarget | None = None,
        target_detail: str | None = None,
        progress: bool | None = None,
        checkpoint: CheckpointMode | None = None,
        wal: bool | None = None,
        wait: bool | None = None,
        compression: CompressionMethod | None = None,
        compression_detail: int | dict[str, str | int] | None = None,
        max_rate: int | None = None,
        tablespace_map: bool | None = None,
        verify_checksums: bool | None = None,
        manifest: ManifestOption | None = None,
        manifest_checksums: ManifestChecksums | None = None,
        incremental: bool = False,
    ) -> tuple[Row, list[Row]]:
        """
        Start a base backup and prepare to receive the backup data.

        Returns a two-tuple containing a row containing (start LSN, timeline)
        and a list of rows containing tablespace information.

        After calling this method call `read_backup_message()` repeatedly to receive
        the tar archive chunks or call `consume_base_backup()` with an appropriate
        `consume` callable.

        compression_detail can be:

        * An integer for compression level
        * A dict of compression options (e.g. `{"level": 6, "workers": 2}`)

        See https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-BASE-BACKUP
        """  # noqa: E501
        server_version = self._conn.info.server_version
        old_style_options = server_version < 150000
        statement: sql.Composed | sql.SQL = sql.SQL("BASE_BACKUP")

        options_snips: list[sql.Composable] = []

        if label is not None:
            options_snips.append(sql.SQL("LABEL {label}").format(label=label))
        if target is not None:
            if old_style_options:
                raise self._raise_unsupported_error("<15", "TARGET", "BASE_BACKUP")
            options_snips.append(sql.SQL("TARGET {target}").format(target=target))
        if target_detail is not None:
            if old_style_options:
                raise self._raise_unsupported_error(
                    "<15", "TARGET_DETAIL", "BASE_BACKUP"
                )
            if target != BackupTarget.SERVER:
                # DISCUSS: leave this to PostgreSQL to error?
                raise ValueError(
                    "target_detail can only be used when target is 'server'"
                )
            options_snips.append(
                sql.SQL("TARGET_DETAIL {target_detail}").format(
                    target_detail=target_detail
                )
            )
        if progress is not None:
            if old_style_options:
                if progress:
                    options_snips.append(sql.SQL("PROGRESS"))
            else:
                options_snips.append(
                    sql.SQL("PROGRESS {progress}").format(
                        progress=str(bool(progress)).lower()
                    )
                )
        if checkpoint is not None:
            if old_style_options:
                if checkpoint == CheckpointMode.FAST:
                    options_snips.append(sql.SQL("FAST"))
            else:
                options_snips.append(
                    sql.SQL("CHECKPOINT {checkpoint}").format(
                        checkpoint=sql.BareIdentifier(checkpoint)
                    )
                )
        if wal is not None:
            if old_style_options:
                if wal:
                    options_snips.append(sql.SQL("WAL"))
            else:
                options_snips.append(
                    sql.SQL("WAL {wal}").format(wal=str(bool(wal)).lower())
                )
        if wait is not None:
            if old_style_options:
                if not wait:
                    options_snips.append(sql.SQL("NOWAIT"))
            else:
                options_snips.append(
                    sql.SQL("WAIT {wait}").format(wait=str(bool(wait)).lower())
                )
        if compression is not None:
            if old_style_options:
                raise self._raise_unsupported_error("<15", "COMPRESSION", "BASE_BACKUP")
            options_snips.append(
                sql.SQL("COMPRESSION {compression}").format(compression=compression)
            )
        if compression_detail is not None:
            if old_style_options:
                raise self._raise_unsupported_error(
                    "<15", "COMPRESSION_DETAIL", "BASE_BACKUP"
                )
            if compression is None:
                # DISCUSS: leave PostgreSQL to handle this case?
                raise ValueError("compression_detail requires compression to be set")
            # Can be a simple integer (compression level) or dict of options
            if isinstance(compression_detail, int):
                detail: int | str = compression_detail
            else:
                # Format as comma-separated "option=value" pairs
                detail = ",".join(
                    (f"{key}={value}" for key, value in compression_detail.items())
                )
            options_snips.append(
                sql.SQL("COMPRESSION_DETAIL {detail}").format(detail=detail)
            )
        if max_rate is not None:
            options_snips.append(
                sql.SQL("MAX_RATE {max_rate}").format(max_rate=max_rate)
            )
        if tablespace_map is not None:
            if old_style_options:
                if tablespace_map:
                    options_snips.append(sql.SQL("TABLESPACE_MAP"))
            else:
                options_snips.append(
                    sql.SQL("TABLESPACE_MAP {tablespace_map}").format(
                        tablespace_map=str(bool(tablespace_map)).lower()
                    )
                )
        if verify_checksums is not None:
            if old_style_options:
                if not verify_checksums:
                    options_snips.append(sql.SQL("NOVERIFY_CHECKSUMS"))
            else:
                options_snips.append(
                    sql.SQL("VERIFY_CHECKSUMS {verify_checksums}").format(
                        verify_checksums=str(bool(verify_checksums)).lower()
                    )
                )
        if manifest is not None:
            # DISCUSS: should this be set to YES by default?
            options_snips.append(
                sql.SQL("MANIFEST {manifest}").format(manifest=manifest)
            )
        if manifest_checksums is not None:
            # TODO: should we check that manifest is yes or force-encode?
            options_snips.append(
                sql.SQL("MANIFEST_CHECKSUMS {manifest_checksums}").format(
                    manifest_checksums=manifest_checksums
                )
            )
        if incremental:
            if old_style_options:
                raise self._raise_unsupported_error("<15", "INCREMENTAL", "BASE_BACKUP")
            options_snips.append(sql.SQL("INCREMENTAL"))

        if options_snips:
            if old_style_options:
                statement = statement + sql.SQL(" {options}").format(
                    options=sql.SQL(" ").join(options_snips)
                )
            else:
                statement = statement + sql.SQL(" ({options})").format(
                    options=sql.SQL(", ").join(options_snips)
                )
        await self._conn.wait(self._start_base_backup_gen(statement))
        self._base_backup_started = True
        start_position = await self.fetchone()
        if start_position is None:
            raise e.DataError(
                "Expected a row indicating the start_position of the base backup, "
                + "but received None"
            )
        self.nextset()
        tablespace_data = await self.fetchall()
        return (start_position, tablespace_data)

    def _read_backup_gen(self, timeout: float = 0.0) -> PQGen[memoryview]:
        """Generator to read backup data via COPY protocol."""
        res = yield from generators.base_backup(self._pgconn, timeout=timeout)

        if isinstance(res, memoryview):
            return res

        self._set_results(res)

        return memoryview(b"")

    async def consume_base_backup(
        self,
        consume: Callable[[AsyncBaseReplicationCursor[Row], BackupMessage | Row], Any],
    ) -> None:
        """
        Accepts a callable `consume` that is called for each backup message.
        The callable must accept two positional parameters: the cursor and the current
        message.
        """
        if self._conn.info.server_version < 150000:
            # NOTE: The message stream is totally different for BASE_BACKUPS
            # See https://www.postgresql.org/docs/14/protocol-replication.html#PROTOCOL-REPLICATION-BASE-BACKUP  # noqa: E501
            raise NotImplementedError(
                "psycopg does not support consume_base_backup for PostgreSQL <15"
            )

        while (msg := await self.read_backup_message()) is not None:
            consume(self, msg)

    async def read_backup_message(
        self, timeout: float = 0.0
    ) -> BackupMessage | Row | None:
        """
        Read the next backup message after start_base_backup().

        Returns different message types based on the backup protocol:

        * `~psycopg.replication.base_backup_messages.BackupNewArchive`:
          Notification of a new tar archive
        * `~psycopg.replication.base_backup_messages.BackupData`:
          Chunk of tar archive or manifest data
        * `~psycopg.replication.base_backup_messages.BackupManifestStart`:
          Marker indicating manifest data will follow
        * `~psycopg.replication.base_backup_messages.BackupProgress`:
          Progress report with total bytes
        * `~psycopg.rows.Row`: A regular row containing the end lsn of the backup
        * `None`: Backup is complete

        This should be called repeatedly until it returns `None` to receive all backup
        data and the final result row which indicates the end LSN of the backup.

        See https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-BASE-BACKUP
        """  # noqa: E501
        if not self._base_backup_started:
            # DISCUSS: should raise an error here instead?
            return None

        if self._conn.info.server_version < 150000:
            # NOTE: The message stream is totally different for BASE_BACKUPS
            # See https://www.postgresql.org/docs/14/protocol-replication.html#PROTOCOL-REPLICATION-BASE-BACKUP  # noqa: E501
            raise NotImplementedError(
                "psycopg does not support read_backup_message for PostgreSQL <15"
            )

        data = await self._conn.wait_no_cancel(self._read_backup_gen(timeout=timeout))

        if not data:
            # Get the final regular row indicating the end LSN
            try:
                row = await self.fetchone()
            except _INTERRUPTED:
                raise
            finally:
                self._base_backup_started = False
            if row is None:
                raise e.DataError("Expected row containing end LSN, got None")
            return row

        msg_type = chr(data[0])

        if msg_type == "d":  # Archive or manifest data
            # Data chunk for current archive or manifest
            return BackupData(data=data[1:])
        elif msg_type == "n":  # New archive
            # New archive notification - archive name and optional tablespace path
            # as null-terminated strings
            payload = bytes(data[1:])
            archive_name_bytes, tablespace_path_bytes, _unexpected = payload.split(
                b"\x00", 2
            )
            if _unexpected:
                logger.debug(
                    "Unexpected content in new archive message: "
                    + f"{_unexpected.decode('utf-8')}"
                )
            archive_name = archive_name_bytes.decode("utf-8")
            # tablespace_path is empty for base archive (no tablespace)
            # FIXME: is this in the server encoding?
            tablespace_path = (
                tablespace_path_bytes.decode("utf-8") if tablespace_path_bytes else None
            )
            return BackupNewArchive(
                archive_name=archive_name, tablespace_path=tablespace_path
            )
        elif msg_type == "m":  # Manifest start
            # no data, just indicates manifest will follow
            return BackupManifestStart()
        elif msg_type == "p":  # Progress
            # total size downloaded so far
            total_bytes = unpack_backup_progress(data[1:9])[0]
            return BackupProgress(total_bytes=total_bytes)
        else:
            # Unknown message type
            raise ValueError(f"Unknown BASE_BACKUP message type: {msg_type!r}")
