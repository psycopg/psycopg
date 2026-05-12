Replication Message Classes
===========================

.. currentmodule:: psycopg.replication

Replication message classes represent messages received from or sent to the
PostgreSQL server on a replication connection that has executed :sql:`BASE_BACKUP` or
:sql:`START_REPLICATION`. Class instances are returned by
`BaseReplicationCursor.read_backup_message`, `BaseReplicationCursor.read_message`, and
`BaseReplicationCursor.send_feedback` (and their
`async variants<AsyncBaseReplicationCursor>`).


Replication messages
--------------------
Messages returned by `BaseReplicationCursor.read_message` and
`BaseReplicationCursor.send_feedback` (and their
`async variants<AsyncBaseReplicationCursor>`).

.. autoclass:: psycopg.replication.ReplicationMessage
    :undoc-members:

.. autoclass:: psycopg.replication.XLogDataMessage
    :undoc-members:
    :inherited-members:

.. autoclass:: psycopg.replication.PrimaryKeepaliveMessage
    :undoc-members:
    :inherited-members:

.. autoclass:: psycopg.replication.StandbyStatusUpdate
    :undoc-members:
    :inherited-members:


.. _psycopg.backup_messages:

Backup messages
---------------
Messages returned by `BaseReplicationCursor.read_backup_message` (and its
`async variant<AsyncBaseReplicationCursor.read_backup_message>`).

.. autoclass:: psycopg.replication.base_backup_messages.BackupMessage
    :members:
    :undoc-members:

    Base class for all other backup messages.


.. autoclass:: psycopg.replication.base_backup_messages.BackupData
    :members:
    :undoc-members:
    :exclude-members: message_type, message_type_name

    .. attribute:: message_type: ClassVar[bytes] = b'd'
    .. attribute:: message_type_name: ClassVar[str] = 'manifest or backup data'

.. autoclass:: psycopg.replication.base_backup_messages.BackupNewArchive
    :members:
    :undoc-members:
    :exclude-members: message_type, message_type_name

    .. attribute:: message_type: ClassVar[bytes] = b'd'
    .. attribute:: message_type_name: ClassVar[str] = 'manifest or backup data'

.. autoclass:: psycopg.replication.base_backup_messages.BackupManifestStart
    :members:
    :undoc-members:
    :exclude-members: message_type, message_type_name

    .. attribute:: message_type: ClassVar[bytes] = b'd'
    .. attribute:: message_type_name: ClassVar[str] = 'manifest or backup data'

.. autoclass:: psycopg.replication.base_backup_messages.BackupProgress
    :members:
    :undoc-members:
    :exclude-members: message_type, message_type_name

    .. attribute:: message_type: ClassVar[bytes] = b'd'
    .. attribute:: message_type_name: ClassVar[str] = 'manifest or backup data'
