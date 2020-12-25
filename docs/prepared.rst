.. currentmodule:: psycopg3

.. index::
    single: Prepared statements

.. _prepared-statements:

Prepared statements
===================

`!psycopg3` uses an automatic system to manage *prepared statements*. When a
query is prepared, its parsing and planning is stored in the server session,
so that further executions of the same query on the same connection (even with
different parameters) are optimised.

A query is prepared automatically after it is executed more than
`~Connection.prepare_threshold` times on a connection. `!psycopg3` will make
sure that no more than `~Connection.prepared_max` statements are planned: if
further queries are executed, the least recently used ones are deallocated and
the associated resources freed.

Statement preparation can be controlled in several ways:

- You can decide to prepare a query immediately by passing ``prepare=True`` to
  `Connection.execute()` or `Cursor.execute()`. The query is prepared, if it
  wasn't already, and executed as prepared from its first use.

- Conversely, passing ``prepare=False`` to `!execute()` will avoid to prepare
  the query, regardless of the number of times it is executed. The default of
  the parameter is `!None`, meaning that the query is prepared if the
  conditions described above are met.

- You can disable the use of prepared statements on a connection by setting
  its `~Connection.prepare_threshold` attribute to `!None`.

.. seealso::

    The `PREPARE`__ PostgreSQL documentation contains plenty of details about
    prepared statements in PostgreSQL.

    Note however that `!psycopg3` doesn't use SQL statements such as
    :sql:`PREPARE` and :sql:`EXECUTE`, but protocol level commands such as the
    ones exposed by :pq:`PQsendPrepare`, :pq:`PQsendQueryPrepared`.

    .. __: https://www.postgresql.org/docs/current/sql-prepare.html
