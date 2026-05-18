import pytest

from psycopg.replication import (
    PrimaryKeepaliveMessage,
    StandbyStatusUpdate,
    XLogDataMessage,
)
from psycopg.replication.base_backup_messages import (
    BackupData,
    BackupManifestStart,
    BackupNewArchive,
    BackupProgress,
)

from .params import tname_param


@pytest.mark.parametrize(
    "msg",
    [
        tname_param(PrimaryKeepaliveMessage(1, True, 12)),
        tname_param(StandbyStatusUpdate(1, 2, 2, 12, True)),
        tname_param(XLogDataMessage(b"", 1, 1, 12)),
        tname_param(BackupData(b"")),
        tname_param(BackupManifestStart()),
        tname_param(BackupNewArchive("test", None)),
        tname_param(BackupProgress(200)),
    ],
)
def test_slots(msg):
    with pytest.raises(AttributeError, match="no __dict__ for setting new attributes"):
        msg.xxx_does_not_exist = 9
