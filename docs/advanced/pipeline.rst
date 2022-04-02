.. currentmodule:: psycopg

.. _pipeline-mode:

Pipeline mode support
=====================

.. versionadded:: 3.1

The *pipeline mode* allows PostgreSQL client applications to send a query
without having to read the result of the previously sent query. Taking
advantage of the pipeline mode, a client will wait less for the server, since
multiple queries/results can be sent/received in a single network transaction.
Pipeline mode can provide a significant performance boost to the application.

The server executes statements, and returns results, in the order the client
sends them. The server will begin executing the commands in the pipeline
immediately, not waiting for the end of the pipeline. Note that results are
buffered on the server side; the server flushes that buffer when a
*synchronization point* is established. If any statement encounters an error,
the server aborts the current transaction and does not execute any subsequent
command in the queue until the next synchronization point; a
`~errors.PipelineAborted` exception is raised for each such command (including
a `~Connection.rollback()`). Query processing resumes after the
synchronization point.

Pipeline mode is most useful when the server is distant, i.e., network latency
(“ping time”) is high, and also when many small operations are being performed
in rapid succession. There is usually less benefit in using pipelined commands
when each query takes many multiples of the client/server round-trip time to
execute. A 100-statement operation run on a server 300 ms round-trip-time away
would take 30 seconds in network latency alone without pipelining; with
pipelining it may spend as little as 0.3 s waiting for results from the
server.

The pipeline mode is available on any currently supported PostgreSQL version
but, in order to make use of it, the client must use a libpq from PostgreSQL
14 or higher. You can use `Pipeline.is_supported()` to make sure your client
has the right library.

.. seealso:: The `PostgreSQL pipeline mode documentation`__ contains many
    details around when it is most useful to use the pipeline mode and about
    errors management and interaction with transactions.

    .. __: https://www.postgresql.org/docs/14/libpq-pipeline-mode.html

Psycopg supports the pipeline mode via the `Connection.pipeline()` method. The
method is a context manager: at the end of the ``with`` block, the connection
resumes the normal operation mode.

Within the pipeline block, you can use one or more cursors to execute several
operations, using `~Cursor.execute()` and `~Cursor.executemany()`. Unlike in
normal mode, Psycopg will not wait for the server to receive the result of
each query, which will be received in batches when a synchronization point is
established.

Psycopg can establish a synchronization points:

- using the `Pipeline.sync()` method;
- at the end of a `!Pipeline` block;
- using a fetch method such as `Cursor.fetchone()`.

The server might perform a sync on its own initiative, for instance when the
query buffer is full.

When a sync is performed, all the pending results are sent back to the cursors
which executed them. If a cursor had run more than one query, it will receive
more than one result; results after the first will be available, in their
execution order, using `~Cursor.nextset()`.

.. note::
    Starting from Psycopg 3.1, `Cursor.executemany()` is optimised to make use
    of pipeline mode.
