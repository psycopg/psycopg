.. currentmodule:: psycopg

.. index::
    pair: Query; Parameters

.. _query-parameters:

Passing parameters to SQL queries
=================================

Most of the times, writing a program you will have to mix bits of SQL
statements with values provided by the rest of the program:

.. code::

    SELECT some, fields FROM some_table WHERE id = ...

:sql:`id` equals what? Probably you will have a Python value you are looking
for.


`!execute()` arguments
----------------------

Passing parameters to a SQL statement happens in functions such as
`Cursor.execute()` by using ``%s`` placeholders in the SQL statement, and
passing a sequence of values as the second argument of the function. For
example the Python function call:

.. code:: python

    cur.execute("""
        INSERT INTO some_table (id, created_at, last_name)
        VALUES (%s, %s, %s);
        """,
        (10, datetime.date(2020, 11, 18), "O'Reilly"))

is *roughly* equivalent to the SQL command:

.. code-block:: sql

    INSERT INTO some_table (id, created_at, last_name)
    VALUES (10, '2020-11-18', 'O''Reilly');

Note that the parameters will not be really merged to the query: query and the
parameters are sent to the server separately: see :ref:`server-side-binding`
for details.

Named arguments are supported too using :samp:`%({name})s` placeholders in the
query and specifying the values into a mapping.  Using named arguments allows
to specify the values in any order and to repeat the same value in several
places in the query::

    cur.execute("""
        INSERT INTO some_table (id, created_at, updated_at, last_name)
        VALUES (%(id)s, %(created)s, %(created)s, %(name)s);
        """,
        {'id': 10, 'name': "O'Reilly", 'created': datetime.date(2020, 11, 18)})

Using characters ``%``, ``(``, ``)`` in the argument names is not supported.

When parameters are used, in order to include a literal ``%`` in the query you
can use the ``%%`` string::

    cur.execute("SELECT (%s % 2) = 0 AS even", (10,))       # WRONG
    cur.execute("SELECT (%s %% 2) = 0 AS even", (10,))      # correct

While the mechanism resembles regular Python strings manipulation, there are a
few subtle differences you should care about when passing parameters to a
query.

- The Python string operator ``%`` *must not be used*: the `~cursor.execute()`
  method accepts a tuple or dictionary of values as second parameter.
  |sql-warn|__:

  .. |sql-warn| replace:: **Never** use ``%`` or ``+`` to merge values
      into queries

  .. code:: python

    cur.execute("INSERT INTO numbers VALUES (%s, %s)" % (10, 20)) # WRONG
    cur.execute("INSERT INTO numbers VALUES (%s, %s)", (10, 20))  # correct

  .. __: sql-injection_

- For positional variables binding, *the second argument must always be a
  sequence*, even if it contains a single variable (remember that Python
  requires a comma to create a single element tuple)::

    cur.execute("INSERT INTO foo VALUES (%s)", "bar")    # WRONG
    cur.execute("INSERT INTO foo VALUES (%s)", ("bar"))  # WRONG
    cur.execute("INSERT INTO foo VALUES (%s)", ("bar",)) # correct
    cur.execute("INSERT INTO foo VALUES (%s)", ["bar"])  # correct

- The placeholder *must not be quoted*::

    cur.execute("INSERT INTO numbers VALUES ('%s')", ("Hello",)) # WRONG
    cur.execute("INSERT INTO numbers VALUES (%s)", ("Hello",))   # correct

- The variables placeholder *must always be a* ``%s``, even if a different
  placeholder (such as a ``%d`` for integers or ``%f`` for floats) may look
  more appropriate for the type. You may find other placeholders used in
  Psycopg queries (``%b`` and ``%t``) but they are not related to the
  type of the argument: see :ref:`binary-data` if you want to read more::

    cur.execute("INSERT INTO numbers VALUES (%d)", (10,))   # WRONG
    cur.execute("INSERT INTO numbers VALUES (%s)", (10,))   # correct

- Only query values should be bound via this method: it shouldn't be used to
  merge table or field names to the query. If you need to generate SQL queries
  dynamically (for instance choosing a table name at runtime) you can use the
  functionalities provided in the `psycopg.sql` module::

    cur.execute("INSERT INTO %s VALUES (%s)", ('numbers', 10))  # WRONG
    cur.execute(                                                # correct
        SQL("INSERT INTO {} VALUES (%s)").format(Identifier('numbers')),
        (10,))


.. index:: Security, SQL injection

.. _sql-injection:

Danger: SQL injection
---------------------

The SQL representation of many data types is often different from their Python
string representation. The typical example is with single quotes in strings:
in SQL single quotes are used as string literal delimiters, so the ones
appearing inside the string itself must be escaped, whereas in Python single
quotes can be left unescaped if the string is delimited by double quotes.

Because of the difference, sometimes subtle, between the data types
representations, a naïve approach to query strings composition, such as using
Python strings concatenation, is a recipe for *terrible* problems::

    SQL = "INSERT INTO authors (name) VALUES ('%s')" # NEVER DO THIS
    data = ("O'Reilly", )
    cur.execute(SQL % data) # THIS WILL FAIL MISERABLY
    # SyntaxError: syntax error at or near "Reilly"

If the variables containing the data to send to the database come from an
untrusted source (such as data coming from a form on a web site) an attacker
could easily craft a malformed string, either gaining access to unauthorized
data or performing destructive operations on the database. This form of attack
is called `SQL injection`_ and is known to be one of the most widespread forms
of attack on database systems. Before continuing, please print `this page`__
as a memo and hang it onto your desk.

.. _SQL injection: https://en.wikipedia.org/wiki/SQL_injection
.. __: https://xkcd.com/327/

Psycopg can :ref:`automatically convert Python objects to SQL
values<types-adaptation>`: using this feature your code will be more robust
and reliable. We must stress this point:

.. warning::

    - Don't manually merge values to a query: hackers from a foreign country
      will break into your computer and steal not only your disks, but also
      your cds, leaving you only with the three most embarrassing records you
      ever bought. On cassette tapes.

    - If you use the ``%`` operator to merge values to a query, con artists
      will seduce your cat, who will run away taking your credit card
      and your sunglasses with them.

    - If you use ``+`` to merge a textual value to a string, bad guys in
      balaclava will find their way to your fridge, drink all your beer, and
      leave your toilet seat up and your toilet paper in the wrong orientation.

    - You don't want to manually merge values to a query: :ref:`use the
      provided methods <query-parameters>` instead.

The correct way to pass variables in a SQL command is using the second
argument of the `Cursor.execute()` method::

    SQL = "INSERT INTO authors (name) VALUES (%s)"  # Note: no quotes
    data = ("O'Reilly", )
    cur.execute(SQL, data)  # Note: no % operator

.. note::

    Python static code checkers are not quite there yet, but, in the future,
    it will be possible to check your code for improper use of string
    expressions in queries. See :ref:`literal-string` for details.

.. seealso::

    Now that you know how to pass parameters to queries, you can take a look
    at :ref:`how Psycopg converts data types <types-adaptation>`.


.. index::
    pair: Binary; Parameters

.. _binary-data:

Binary parameters and results
-----------------------------

PostgreSQL has two different ways to transmit data between client and server:
`~psycopg.pq.Format.TEXT`, always available, and `~psycopg.pq.Format.BINARY`,
available most of the times but not always. Usually the binary format is more
efficient to use.

Psycopg can support both formats for each data type. Whenever a value
is passed to a query using the normal ``%s`` placeholder, the best format
available is chosen (often, but not always, the binary format is picked as the
best choice).

If you have a reason to select explicitly the binary format or the text format
for a value you can use respectively a ``%b`` placeholder or a ``%t``
placeholder instead of the normal ``%s``. `~Cursor.execute()` will fail if a
`~psycopg.adapt.Dumper` for the right data type and format is not available.

The same two formats, text or binary, are used by PostgreSQL to return data
from a query to the client. Unlike with parameters, where you can choose the
format value-by-value, all the columns returned by a query will have the same
format. Every type returned by the query should have a `~psycopg.adapt.Loader`
configured, otherwise the data will be returned as unparsed `!str` (for text
results) or buffer (for binary results).

.. note::
    The `pg_type`_ table defines which format is supported for each PostgreSQL
    data type. Text input/output is managed by the functions declared in the
    ``typinput`` and ``typoutput`` fields (always present), binary
    input/output is managed by the ``typsend`` and ``typreceive`` (which are
    optional).

    .. _pg_type: https://www.postgresql.org/docs/current/catalog-pg-type.html

Because not every PostgreSQL type supports binary output, by default, the data
will be returned in text format. In order to return data in binary format you
can create the cursor using `Connection.cursor`\ `!(binary=True)` or execute
the query using `Cursor.execute`\ `!(binary=True)`. A case in which
requesting binary results is a clear winner is when you have large binary data
in the database, such as images::

    cur.execute(
        "SELECT image_data FROM images WHERE id = %s", [image_id], binary=True)
    data = cur.fetchone()[0]
