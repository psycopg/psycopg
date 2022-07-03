`sql` -- SQL string composition
===============================

.. index::
    double: Binding; Client-Side

.. module:: psycopg.sql

The module contains objects and functions useful to generate SQL dynamically,
in a convenient and safe way. SQL identifiers (e.g. names of tables and
fields) cannot be passed to the `~psycopg.Cursor.execute()` method like query
arguments::

    # This will not work
    table_name = 'my_table'
    cur.execute("INSERT INTO %s VALUES (%s, %s)", [table_name, 10, 20])

The SQL query should be composed before the arguments are merged, for
instance::

    # This works, but it is not optimal
    table_name = 'my_table'
    cur.execute(
        "INSERT INTO %s VALUES (%%s, %%s)" % table_name,
        [10, 20])

This sort of works, but it is an accident waiting to happen: the table name
may be an invalid SQL literal and need quoting; even more serious is the
security problem in case the table name comes from an untrusted source. The
name should be escaped using `~psycopg.pq.Escaping.escape_identifier()`::

    from psycopg.pq import Escaping

    # This works, but it is not optimal
    table_name = 'my_table'
    cur.execute(
        "INSERT INTO %s VALUES (%%s, %%s)" % Escaping.escape_identifier(table_name),
        [10, 20])

This is now safe, but it somewhat ad-hoc. In case, for some reason, it is
necessary to include a value in the query string (as opposite as in a value)
the merging rule is still different. It is also still relatively dangerous: if
`!escape_identifier()` is forgotten somewhere, the program will usually work,
but will eventually crash in the presence of a table or field name with
containing characters to escape, or will present a potentially exploitable
weakness.

The objects exposed by the `!psycopg.sql` module allow generating SQL
statements on the fly, separating clearly the variable parts of the statement
from the query parameters::

    from psycopg import sql

    cur.execute(
        sql.SQL("INSERT INTO {} VALUES (%s, %s)")
            .format(sql.Identifier('my_table')),
        [10, 20])


Module usage
------------

Usually you should express the template of your query as an `SQL` instance
with ``{}``\-style placeholders and use `~SQL.format()` to merge the variable
parts into them, all of which must be `Composable` subclasses. You can still
have ``%s``\-style placeholders in your query and pass values to
`~psycopg.Cursor.execute()`: such value placeholders will be untouched by
`!format()`::

    query = sql.SQL("SELECT {field} FROM {table} WHERE {pkey} = %s").format(
        field=sql.Identifier('my_name'),
        table=sql.Identifier('some_table'),
        pkey=sql.Identifier('id'))

The resulting object is meant to be passed directly to cursor methods such as
`~psycopg.Cursor.execute()`, `~psycopg.Cursor.executemany()`,
`~psycopg.Cursor.copy()`, but can also be used to compose a query as a Python
string, using the `~Composable.as_string()` method::

    cur.execute(query, (42,))
    full_query = query.as_string(cur)

If part of your query is a variable sequence of arguments, such as a
comma-separated list of field names, you can use the `SQL.join()` method to
pass them to the query::

    query = sql.SQL("SELECT {fields} FROM {table}").format(
        fields=sql.SQL(',').join([
            sql.Identifier('field1'),
            sql.Identifier('field2'),
            sql.Identifier('field3'),
        ]),
        table=sql.Identifier('some_table'))


`!sql` objects
--------------

The `!sql` objects are in the following inheritance hierarchy:

|   `Composable`: the base class exposing the common interface
|   ``|__`` `SQL`: a literal snippet of an SQL query
|   ``|__`` `Identifier`: a PostgreSQL identifier or dot-separated sequence of identifiers
|   ``|__`` `Literal`: a value hardcoded into a query
|   ``|__`` `Placeholder`: a `%s`\ -style placeholder whose value will be added later e.g. by `~psycopg.Cursor.execute()`
|   ``|__`` `Composed`: a sequence of `!Composable` instances.


.. autoclass:: Composable()

    .. automethod:: as_bytes
    .. automethod:: as_string


.. autoclass:: SQL

    .. versionchanged:: 3.1

        The input object should be a `~typing.LiteralString`. See :pep:`675`
        for details.

    .. automethod:: format

    .. automethod:: join


.. autoclass:: Identifier

.. autoclass:: Literal

    .. versionchanged:: 3.1
        Add a type cast to the representation if useful in ambiguous context
        (e.g. ``'2000-01-01'::date``)

.. autoclass:: Placeholder

.. autoclass:: Composed

    .. automethod:: join


Utility functions
-----------------

.. autofunction:: quote

.. data::
    NULL
    DEFAULT

    `sql.SQL` objects often useful in queries.
