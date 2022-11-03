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

.. warning::

    Using external connection poolers, such as PgBouncer, is not compatible
    with prepared statements, because the same client connection may change
    the server session it refers to. If such middleware is used you should
    disable prepared statements, by setting the `Connection.prepare_threshold`
    attribute to `!None`.
