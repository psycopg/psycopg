from __future__ import annotations

from struct import Struct
from typing import Callable, ClassVar, Literal, cast
from dataclasses import dataclass

from ..abc import Buffer


class BackupMessage:
    __slots__ = ()
    message_type: ClassVar[Literal["d", "n", "m", "p"]]
    message_type_name: ClassVar[
        Literal[
            "manifest or backup data",
            "new archive",
            "manifest start",
            "progress",
        ]
    ]


@dataclass(slots=True, weakref_slot=True)
class BackupData(BackupMessage):
    """Archive or manifest data chunk from BASE_BACKUP ('d' message)."""

    message_type = "d"
    message_type_name = "manifest or backup data"

    data: Buffer


@dataclass(slots=True, weakref_slot=True)
class BackupNewArchive(BackupMessage):
    """
    New archive notification from BASE_BACKUP ('n' message).
    Indicates the start of a new tar archive.
    """

    message_type = "n"
    message_type_name = "manifest or backup data"

    archive_name: str
    tablespace_path: str | None  # Only for tablespace archives


class BackupManifestStart(BackupMessage):
    """
    Backup manifest start marker from BASE_BACKUP ('m' message).
    Indicates that manifest data will follow in 'd' messages.
    """

    __slots__ = ()

    message_type = "m"
    message_type_name = "manifest start"

    _instance: BackupManifestStart

    def __new__(cls: type[BackupManifestStart]) -> BackupManifestStart:
        return cls._instance


BackupManifestStart._instance = object.__new__(BackupManifestStart)


@dataclass(slots=True, weakref_slot=True)
class BackupProgress(BackupMessage):
    """Progress report from BASE_BACKUP ('p' message)."""

    message_type = "p"
    message_type_name = "progress"

    total_bytes: int


_backup_progress_struct = Struct(">Q")
unpack_backup_progress = cast(
    Callable[[Buffer], tuple[int]], _backup_progress_struct.unpack
)
