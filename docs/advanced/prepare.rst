.. currentmodule:: psycopg

.. index::
    single: Prepared statements

.. _prepared-statements:

Prepared statements
===================

Psycopg uses an automatic system to manage *prepared statements*. When a
query is prepared, its parsing and planning is stored in the server session,
so that further executions of the same query on the same connection (even with
different parameters) are optimised.

A query is prepared automatically after it is executed more than
`~Connection.prepare_threshold` times on a connection. `!psycopg` will make
sure that no more than `~Connection.prepared_max` statements are planned: if
further queries are executed, the least recently used ones are deallocated and
the associated resources freed.

Statement preparation can be controlled in several ways:

- You can decide to prepare a query immediately by passing `!prepare=True` to
  `Connection.execute()` or `Cursor.execute()`. The query is prepared, if it
  wasn't already, and executed as prepared from its first use.

- Conversely, passing `!prepare=False` to `!execute()` will avoid to prepare
  the query, regardless of the number of times it is executed. The default for
  the parameter is `!None`, meaning that the query is prepared if the
  conditions described above are met.

- You can disable the use of prepared statements on a connection by setting
  its `~Connection.prepare_threshold` attribute to `!None`.

.. versionchanged:: 3.1
    You can set `!prepare_threshold` as a `~Connection.connect()` keyword
    parameter too.

.. seealso::

    The `PREPARE`__ PostgreSQL documentation contains plenty of details about
    prepared statements in PostgreSQL.

    Note however that Psycopg doesn't use SQL statements such as
    :sql:`PREPARE` and :sql:`EXECUTE`, but protocol level commands such as the
    ones exposed by :pq:`PQsendPrepare`, :pq:`PQsendQueryPrepared`.

    .. __: https://www.postgresql.org/docs/current/sql-prepare.html


.. _pgbouncer:

Using prepared statements with PgBouncer
----------------------------------------

.. warning::

    Unless a connection pooling middleware explicitly declares otherwise, they
    are not compatible with prepared statements, because the same client
    connection may change the server session it refers to. If such middleware
    is used you should disable prepared statements, by setting the
    `Connection.prepare_threshold` attribute to `!None`.

Starting from 3.2, Psycopg supports prepared statements when using the
PgBouncer__ middleware, using the following caveats:

- PgBouncer version must be version `1.22`__ or newer.
- PgBouncer `max_prepared_statements`__ must be greater than 0.
- The libpq version on the client must be from PostgreSQL 17 or newer
  (you can check the `~Capabilities.has_send_close_prepared()` capability to
  verify that the libpq implements the features required by PgBouncer).

.. __: https://www.pgbouncer.org/
.. __: https://www.pgbouncer.org/2024/01/pgbouncer-1-22-0
.. __: https://www.pgbouncer.org/config.html#max_prepared_statements

.. hint::

    If libpq 17 is not available on your client, but PgBouncer is 1.22 or
    higher, you can still use Psycopg *as long as you disable deallocation*.

    You can do so by setting `Connection.prepared_max` to `!None`.
