.. currentmodule:: psycopg

.. _pipeline-mode:

Pipeline mode support
=====================

.. versionadded:: 3.1

The *pipeline mode* allows PostgreSQL client applications to send a query
without having to read the result of the previously sent query. Taking
advantage of the pipeline mode, a client will wait less for the server, since
multiple queries/results can be sent/received in a single network roundtrip.
Pipeline mode can provide a significant performance boost to the application.

Pipeline mode is most useful when the server is distant, i.e., network latency
(“ping time”) is high, and also when many small operations are being performed
in rapid succession. There is usually less benefit in using pipelined commands
when each query takes many multiples of the client/server round-trip time to
execute. A 100-statement operation run on a server 300 ms round-trip-time away
would take 30 seconds in network latency alone without pipelining; with
pipelining it may spend as little as 0.3 s waiting for results from the
server.

The server executes statements, and returns results, in the order the client
sends them. The server will begin executing the commands in the pipeline
immediately, not waiting for the end of the pipeline. Note that results are
buffered on the server side; the server flushes that buffer when a
:ref:`synchronization point <pipeline-sync>` is established.

.. seealso::

    The PostgreSQL documentation about:

    - `pipeline mode`__
    - `extended query message flow`__

    contains many details around when it is most useful to use the pipeline
    mode and about errors management and interaction with transactions.

    .. __: https://www.postgresql.org/docs/current/libpq-pipeline-mode.html
    .. __: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY


.. _pipeline-usage:

Pipeline mode usage
-------------------

Psycopg supports the pipeline mode via the `Connection.pipeline()` method. The
method is a context manager: at the end of the ``with`` block, the connection
resumes the normal operation mode.

Within the pipeline block, you can use one or more cursors to execute several
operations, using `~Cursor.execute()` and `~Cursor.executemany()`. Unlike in
normal mode, Psycopg will not wait for the server to receive the result of
each query, which will be received in batches when the server flushes it
output buffer.

When a flush (or a sync) is performed, all pending results are sent back to
the cursors which executed them. If a cursor had run more than one query, it
will receive more than one result; results after the first will be available,
in their execution order, using `~Cursor.nextset()`.

If any statement encounters an error, the server aborts the current
transaction and does not execute any subsequent command in the queue until the
next :ref:`synchronization point <pipeline-sync>`; a `~errors.PipelineAborted`
exception is raised for each such command. Query processing resumes after the
synchronization point.

Note that, even in :ref:`autocommit <autocommit>`, the server wraps the
statements sent in pipeline mode in an implicit transaction, which will be
only committed when the sync is received. As such, a failure in a group of
statement will probably invalidate the effect of statements executed after the
previous sync.

.. warning::

    Certain features are not available in pipeline mode, including:

    - COPY is not supported in pipeline mode by PostgreSQL.
    - `Cursor.stream()` doesn't make sense in pipeline mode (its job is the
      opposite of batching!)
    - `ServerCursor` are currently not implemented in pipeline mode.

.. note::

    Starting from Psycopg 3.1, `~Cursor.executemany()` makes use internally of
    the pipeline mode; as a consequence there is no need to handle a pipeline
    block just to call `!executemany()` once.


.. _pipeline-sync:

Synchronization points
----------------------

Flushing query results to the client can happen either when a synchronization
point is established by Psycopg:

- using the `Pipeline.sync()` method,
- on `~Connection.rollback()`,
- at the end of a `!Pipeline` block;

or using a fetch method such as `Cursor.fetchone()`.

The server might perform a flush on its own initiative, for instance when the
output buffer is full.

In contrast with a synchronization point, a flush request (i.e. calling a
fetch method) will not reset the pipeline error state.


The fine prints
---------------

.. warning::

    The Pipeline mode is an experimental feature.

    Its behaviour, especially around error condtions, hasn't been explored as
    much as the normal request-response messages pattern, and its async nature
    makes it inherently more complex.

    As we gain more experience and feedback (which is welcome), we might find
    bugs and shortcomings forcing us to change the current interface or
    behaviour.

The pipeline mode is available on any currently supported PostgreSQL version,
but, in order to make use of it, the client must use a libpq from PostgreSQL
14 or higher. You can use `Pipeline.is_supported()` to make sure your client
has the right library.
