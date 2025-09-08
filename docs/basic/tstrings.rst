.. currentmodule:: psycopg

.. index::
    pair: Query; Template strings

.. _template-strings:

Template string queries
=======================

.. versionadded:: 3.3

.. warning::

    This is an experimental feature, still under active development,
    documented here for preview. Details may change before the final Psycopg
    3.3 release.

    Template strings are a Python language feature under development too,
    planned for release in Python 3.14. Template string queries are currently
    tested in Python 3.14 rc 2.

    If you want to test the feature you can install a `test version of Pyscopg
    from test pypi`__:

    .. code:: shell

        $ pip install -i https://test.pypi.org/simple/ "psycopg[binary]==3.3.0.dev1"

    .. __: https://test.pypi.org/project/psycopg/3.3.0.dev1/

Psycopg can process queries expressed as `template strings`__ defined in
:pep:`750` and implemented for the first time in Python 3.14.

.. __: https://docs.python.org/3.14/whatsnew/3.14.html#pep-750-template-strings

Template strings are similar to f-strings__: they are string literals
interspersed with variables or expressions marked by ``{}``. They use a ``t``
prefix instead of ``f``, and can be used to express queries:

.. __: https://docs.python.org/3/tutorial/inputoutput.html#tut-f-strings

.. code:: python

    cursor.execute(t"SELECT * FROM mytable WHERE id = {id}")

The difference between the two is that f-strings are immediately evaluated by
Python and passed to the rest of the program as an already formatted regular
string; t-strings, instead, are evaluated by Psycopg, which has a chance to
process query parameters in a safe way.

For example you can pass to a query some strings parameters, which may contain
unsafe characters such as ``'``, or come from untrusted sources such as web
form, and leave Psycopg to perform the right processing:

.. code:: python

    cursor.execute(
        t"INSERT INTO mytable (first_name, last_name) VALUES ({first_name}, {last_name})"
    )

This statement has the same effect as a classic:

.. code:: python

    cursor.execute(
        "INSERT INTO mytable (first_name, last_name) VALUES (%s, %s)",
        (first_name, last_name),
    )

but has a clear readability advantage because the Python variable names or
expressions appear directly in the place where they will be used in the query
(no more forgetting to add a placeholder when adding a field in an INSERT...).

Like in normal queries, according to the :ref:`type of cursor <cursor-types>`
used, Psycopg will either send parameters separately from the query, or will
compose the query on the client side using safe escaping rules, guaranteeing
protection from SQL injections.


Format specifiers
-----------------

Format specifiers can be associated to template strings interpolation using a
``:`` in the placeholder, for example in ``{id:b}``. Psycopg supports a few
format specifiers specifying :ref:`how to pass a parameter to the server
<binary-data>` and a few format specifiers specifying how to compose query
parts on the client, in a way similar to what can be obtained using the
`psycopg.sql` objects.

The supported specifiers for parameter formats are:

- ``s``: automatic parameter format, similar to using ``%s`` in a classic
  query. This is the same effect of using no format specifier.
- ``b``: use the binary format to pass the parameter, similar to using ``%b``
  in a classic query.
- ``t``: use the text format to pass the parameter, similar to using ``%t``
  in a classic query.

The supported specifiers for query composition are:

- ``i``: the parameter is an identifier_, for example a table or column name.
  The parameter must be a string or a `sql.Identifier` instance.
- ``l``: the parameter is a literal value, which will be merged to the
  query on the client. This allows to parametrize statements that :ref:`don't
  support parametrization in PostgreSQL <server-side-binding>`.
- ``q``: the parameter is a snippet of statement to be included verbatim in
  the query. The parameter must be another template string or a
  `sql.SQL`\/\ `~sql.Composed` instance.

.. _identifier: https://www.postgresql.org/docs/current/sql-syntax-lexical.html
                #SQL-SYNTAX-IDENTIFIERS


.. _tstring-template-notify:

Example: NOTIFY
---------------

The NOTIFY_ command takes a *channel* parameter (an identifier, so it must be
quoted with double quotes if it contains any non-alphanumeric character), and
a *payload* parameter as a string (which must be escaped with string syntax,
hence with single quotes).

.. _NOTIFY: https://www.postgresql.org/docs/current/sql-notify.html

The :sql:`NOTIFY` command cannot be parametrized by PostgreSQL, so it must be
composed entirely on the client side. Using template strings this could be as
simple as:

.. code:: python

    def send_notify(conn: Connection, channel: str, payload: str) -> None:
        conn.execute(t"NOTIFY {channel:i}, {payload:l}")

Calling the function with channel ``foo.bar`` and payload ``O'Reilly`` will
result in executing the statement ``NOTIFY "foo.bar", 'O''Reilly'``.


.. _tstring-template-nested:

Example: nested templates
-------------------------

A string template merges literal parts of the query with parameter. It is also
possible to pass templates to templates in order to compose more and more
complex and dynamic SQL statements.

For example let's say we have an `!User` Python object mapping to an ``users``
database table. We might want to implement a `!search()` function to return
users by a list of ids, by names pattern, by group. This function might
be written as:

.. code:: python

    def search_users(
        conn: Connection,
        ids: Sequence[int] | None = None,
        name_pattern: str | None = None,
        group_id: int | None = None,
    ) -> list[User]:
        filters = []
        if ids is not None:
            ids = list(ids)
            filters.append(t"u.id = ANY({ids})")
        if name_pattern is not None:
            filters.append(t"u.name ~* {name_pattern}")
        if group_id is not None:
            filters.append(t"u.group_id = {group_id}")
        if not filters:
            raise TypeError("please specify at least one search parameter")

        joined = sql.SQL(" AND ").join(filters)
        cur = conn.cursor(row_factory=class_row(User))
        cur.execute(t"SELECT * FROM users AS u WHERE {joined:q}")
        return cur.fetchall()

In this example we have used the `sql.SQL.join()` method overload that takes a
list of templates and returns a joined template in order to create an AND-ed
sequence of conditions.
