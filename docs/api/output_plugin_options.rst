.. _psycopg.output_options:

.. currentmodule:: psycopg.replication.logical_output_plugins

Output Plugin Options Transformation
====================================

In order to add support for an output plugin that accepts options,
an `~abc.OutputPluginOptions` implementation must be registered for the plugin.

.. autoclass:: psycopg.replication.logical_output_plugins.abc.OutputPluginOptions
    :members:
    :special-members: __init__

The base class `OutputPluginOptionsBase` is available to make the implementation
simpler.

.. autoclass:: psycopg.replication.logical_output_plugins.OutputPluginOptionsBase
    :members:
    :undoc-members:

New options classes can be registered using `register_output_plugin_options()`.

.. autofunction:: psycopg.replication.logical_output_plugins.register_output_plugin_options


Included `~abc.OutputPluginOptions` implementations
---------------------------------------------------
`psycopg` includes two bundled `~abc.OutputPluginOptions` implementations to handle
the builtin pgoutput_ plugin and the `contrib`-provided test_decoding_ plugin.

.. _pgoutput: https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
.. _test_decoding: https://github.com/postgres/postgres/blob/master/contrib/test_decoding/test_decoding.c#L180

.. autoclass:: psycopg.replication.logical_output_plugins.pgoutput.PgOutputOptions
    :members:
    :undoc-members:


.. autoclass:: psycopg.replication.logical_output_plugins.TestDecodingOptions
    :members:
    :undoc-members:
