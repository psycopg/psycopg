.. _psycopg.replication_decoders:

Replication Decoders
====================

.. currentmodule:: psycopg.replication

Replication decoders are used to decode the payload of an `XLogDataMessage`.

They are passed to :class:`PhysicalReplicationCursor.start_replication()` and
:class:`LogicalReplicationCursor.start_replication()`. If `decoder=None` (the default
for :class:`PhysicalReplicationCursor`) then the payload is the exact binary blob
delivered by the replication stream.

There are three builtin decoders for logical replication, outlined below.

`psycopg` only natively supports decoding from the pgoutput_ and test_decoding_ output
plugins that are builtin to PostgreSQL.  However, `~logical_output_plugins.TextDecoder`
is suitable for use with any output plugin that delivers a text payload in the server
encoding and can be :ref:`registered for a given output plugin<decoder-registration>`.
There are also :ref:`facilities for writing your own decoder <decoder-protocol>` to
decode from other output plugins.

.. _pgoutput: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
.. _test_decoding: https://www.postgresql.org/docs/current/test-decoding.html


.. autoclass:: psycopg.replication.logical_output_plugins.DispatchingDecoder
    :members:
    :undoc-members:
    :special-members: __call__
    :private-members: _tx

    `DispatchingDecoder` is the default decoder
    for `~psycopg.replication.LogicalReplicationCursor.start_replication()` (and its
    `async variant<psycopg.replication.AsyncLogicalReplicationCursor.start_replication()>`
    ). It replaces itself with a registered decoder based on the output plugin of the
    slot used for replication. By default, it supports delegating to
    `~pgoutput.PgOutputDecoder` for pgoutput_
    and `~TextDecoder` for test_decoding_.

    :meth:`register_decoder()` can be used to
    register custom `~abc.LogicalXLogDataDecoder` classes
    for other output plugins (or to override the existing defaults).

.. autoclass:: psycopg.replication.logical_output_plugins.pgoutput.PgOutputDecoder
    :members:
    :undoc-members:
    :special-members: __call__
    :private-members: _tx

    `PgOutputDecoder` is the default decoder for the pgoutput_ output plugin.
    It uses the `~psycopg.adapt.AdaptersMap` of the
    `~psycopg.replication.LogicalReplicationCursor` to adapt `PostgreSQL` types
    to `Python` types and supports a `row_factory` parameter for outputting types
    other than simple tuples for relation rows.  The messages it outputs are documented
    in `pgoutput_messages`.

.. autoclass:: psycopg.replication.logical_output_plugins.TextDecoder
    :members:
    :undoc-members:
    :special-members: __call__

    `~TextDecoder` is the default decoder for the test_decoding_ output plugin.
    It converts the payload into a `str` by decoding it according to
    :attr:`server_encoding`.


.. _decoder-protocol:

Formal Decoder Protocols
------------------------

These objects can be used to describe your own `XLogDataMessage.payload`
decoders for static typing checks, such as mypy_.  These objects additionally
provide concrete implementations of all required methods except the `__call__()`
and `~logical_output_plugins.abc.LogicalRowFactoryXLogDataDecoder.get_relation()`
methods for convenience.

`~logical_output_plugins.abc.LogicalRowFactoryXLogDataDecoder` should be used if your
decoder wants to use the facilities documented in :ref:`psycopg.logical_rows`.

`server_encoding` is assigned to all decoders by `start_replication()` after their
creation.

`_tx` and `plugin_options` are assigned to
`~logical_output_plugins.abc.LogicalXLogDataDecoder` in
`LogicalReplicationCursor.start_replication()`.  `_tx` is additionally reassigned
whenever the cursor's `~psycopg.adapt.AdaptersMap` changes.

.. _mypy: https://mypy.readthedocs.io/


.. autoclass:: psycopg.replication.abc.XLogDataDecoder
    :members:
    :undoc-members:
    :special-members: __call__


.. autoclass:: psycopg.replication.logical_output_plugins.abc.LogicalXLogDataDecoder
    :members:
    :inherited-members:
    :undoc-members:
    :special-members: __init__, __call__
    :private-members: _tx, _plugin_options



.. autoclass:: psycopg.replication.logical_output_plugins.abc.LogicalRowFactoryXLogDataDecoder
    :members:
    :inherited-members:
    :undoc-members:
    :special-members: __init__, __call__
    :private-members: _tx, _plugin_options


Relation Protocols
~~~~~~~~~~~~~~~~~~
`LogicalRowFactoryXLogDataDecoder.get_relation()
<logical_output_plugins.abc.LogicalRowFactoryXLogDataDecoder.get_relation()>`
depends on the following protocols.

.. autoclass:: psycopg.replication.logical_output_plugins.abc.Relation
    :members:
    :undoc-members:


.. autoclass:: psycopg.replication.logical_output_plugins.abc.ColumnDefinition
    :members:
    :undoc-members:


.. _decoder-registration:

Decoder Registration
--------------------

`~logical_output_plugins.DispatchingDecoder`, the default decoder for
`LogicalReplicationCursor.start_replication()` additionally includes facilities
for registering your custom decoders to be used automatically for a given output plugin.

.. automethod:: psycopg.replication.logical_output_plugins.DispatchingDecoder.register_decoder
    :no-index:
