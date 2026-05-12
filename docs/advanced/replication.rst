Replication
===========

.. module:: psycopg.replication

.. warning::

    Replication is an experimental feature.

    Its behaviour, especially around the decoding interface, hasn't
    been explored thoroughly in production use-cases.

    As we gain more experience and feedback (which is welcome), we might find
    bugs and shortcomings forcing us to change the current interface or
    behaviour.

`psycopg` includes support for PostgreSQL replication connections, supporting
both physical and logical replication and all replication commands provided by
the `PostgreSQL replication protocol`_.

.. _`PostgreSQL replication protocol`: https://www.postgresql.org/docs/current/protocol-replication.html

.. note::

    `psycopg` only officially supports replication on PostgreSQL versions ≥ 14, but some
    replication commands may work on older versions.

    `psycopg` does not support the `BASE_BACKUP` command on PostgreSQL version 14
    and lower, but the facilities to implement it are present.  See the
    implementation of :class:`AsyncBaseReplicationCursor.start_base_backup()` and the
    `PostgreSQL documentation`__.

.. __: https://www.postgresql.org/docs/14/protocol-replication.html#PROTOCOL-REPLICATION-BASE-BACKUP

The Basic Interface
-------------------
The interface for replication is the same for logical and physical replication, though
the parameters may differ.

`LogicalReplicationCursor.start_replication()` and
`PhysicalReplicationCursor.start_replication()` (or the async variants)
begin the replication stream.  After calling one of these methods, the connection
will only be usable for receiving replication messages or sending replication
feedback to the server.

After the stream has started, call `BaseReplicationCursor.read_message()` (or the
`async variant<AsyncBaseReplicationCursor.read_message()>`) to receive the next
`~replication_messages.ReplicationMessage`.

Alternatively, you may call `BaseReplicationCursor.consume_stream()` with a
`consume` function that is called for each `~replication_messages.ReplicationMessage`
and accepts the cursor and the current message as arguments.

The replication cursors have the facilities for managing feedback automatically, but
if you require more granular control, `BaseReplicationCursor.send_feedback()` can be
called directly as required, returning a `StandbyStatusUpdate`
message indicating the feedback that was sent.

A `ReplicationMessage` returned by
`BaseReplicationCursor.read_message()` will be either a
`XLogDataMessage` or a
`~PrimaryKeepaliveMessage` by default.  If you only want to
handle data messages, pass `return_keepalive_messages=False`.


Logical Replication
-------------------

.. note::
    Make sure that replication connections are permitted for the connection user in
    `pg_hba.conf`_.  You also need to set `wal_level`_ = logical and max_wal_senders_,
    max_replication_slots_ to a value greater than zero in postgresql.conf. These
    changes require a server restart.  Consider setting `other replication settings`_
    as appropriate for your use-case.

By default, `LogicalReplicationCursor.start_replication()` (and its
`async variant<AsyncLogicalReplicationCursor.start_replication()>`) will
cause `XLogDataMessage.payload <replication_messages.XLogDataMessage.payload>` to be decoded into
a python object as enumerated in :ref:`psycopg.pgoutput_messages`.  If you don't want to
decode the message (i.e. you want a binary blob), pass `decoder=None`.  You may also
implement and pass a :ref:`custom decoder <decoder-protocol>`.

.. _`pg_hba.conf`: https://www.postgresql.org/docs/current/auth-pg-hba-conf.html
.. _max_replication_slots: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-MAX-REPLICATION-SLOTS
.. _wal_level: https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-LEVEL
.. _max_wal_senders: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-MAX-WAL-SENDERS
.. _`other replication settings`: https://www.postgresql.org/docs/current/runtime-config-replication.html#RUNTIME-CONFIG-REPLICATION

.. _logical-row-factories:

Logical Row Factories
~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: psycopg.replication.logical_output_plugins.pgoutput

:ref:`Logical row factories <psycopg.logical_rows>` are analogous to
:ref:`row-factories` for regular queries,
but operate on the tuples present in `~pgoutput_messages.InsertMessage`,
`~pgoutput_messages.DeleteMessage`, and `~pgoutput_messages.UpdateMessage`
when using the `PgOutputDecoder` (which is also the default decoder
used by `~psycopg.replication.logical_output_plugins.DispatchingDecoder`
for the `pgoutput` logical output plugin).

The builtin logical row factories are documented in :ref:`psycopg.logical_rows`.
There are logical replication variants of
:ref:`all the regular row factories<psycopg.rows>`, with the exception of
`~psycopg.rows.scalar_row()`.


Creating new row factories
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. currentmodule:: psycopg.replication.logical_output_plugins

A *logical row factory* is a callable that accepts a
`~abc.LogicalRowFactoryXLogDataDecoder` object and a `relation_id`
and returns another callable, a *logical row maker*, which takes raw data
(as a sequence of values) and returns the desired object.

The role of the row factory is to inspect a decoder and relation_id and to prepare
a callable which is efficient to call repeatedly.  The decoder itself may employ
caching to make this more efficient (e.g. by reusing the same logical row maker
for messages representing operations on the same relation).

Formally, these objects are represented by the `~logical_rows.LogicalRowFactory` and
`~logical_rows.LogicalRowMaker` protocols.


Using other output plugins
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: psycopg.replication.logical_output_plugins

To register support for decoding the payloads from other logical output
plugins, an appropriate decoder must be provided.  It can either
be provided directly in a call to
`start_replication("myoutputplugin_slot", decoder=MyDecoder)
<psycopg.replication.LogicalReplicationCursor.start_replication()>`
or it can be registered as the default decoder for your output plugin by
calling `DispatchingDecoder.register_decoder("myoutputplugin", MyDecoder)
<DispatchingDecoder.register_decoder()>`.
See :ref:`decoder-protocol` for details on creating a decoder class.

If your output plugin supports options that you need to set, you'll likely
want to implement the `abc.OutputPluginOptions` protocol and
register your implementation by calling
`register_output_plugin_options("myoutputplugin", MyOutputPluginOptions)
<register_output_plugin_options()>`. See the implementations
of `TestDecodingOptions` and `~pgoutput.PgOutputOptions` for guidance. The base
class `OutputPluginOptionsBase` is available to make the implementation simpler.

Alternatively, the `raw_output_plugin_options` argument to
`~psycopg.replication.LogicalReplicationCursor.start_replication()` can be passed,
but it must contain the exact string values to interpolate into the `START_REPLICATION`
command.


Base Backups
------------

.. currentmodule:: psycopg.replication

Both logical and physical replication connections support the creation of `base backups`_
using the `BASE_BACKUP` replication command.

.. _`base backups`: https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-BASE-BACKUP

The implementation is similar to the replication interface.

To begin a base backup, call `BaseReplicationCursor.start_base_backup()`.

.. note::
    If you are performing an incremental backup, you'll need to call
    `BaseReplicationCursor.upload_manifest()` first.

To process each backup message, call `BaseReplicationCursor.read_backup_message()`
The result of `~BaseReplicationCursor.read_backup_message()` is an object of type
`~psycopg.replication.base_backup_messages.BackupMessage` until the stream is ended.
The available messages are documented in :ref:`psycopg.backup_messages`. At the end of
the stream, `~BaseReplicationCursor.read_backup_message()` will return a regular
`~psycopg.rows.Row` (the form of which is dictated by the cursor's
:ref:`row factory<row-factories>`) that contains the `end_lsn` of the base backup.
Subsequent calls will return `None`.

Alternatively, you can call `BaseReplicationCursor.consume_base_backup()` with an
appropriate `consume` callable, which will be called with the cursor and the message
for each `~psycopg.replication.base_backup_messages.BackupMessage` received and the final
`~psycopg.rows.Row`.
