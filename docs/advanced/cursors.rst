.. currentmodule:: psycopg

.. index::
    single: Cursor

.. _cursor-types:

Cursor types
============

Cursors are objects used to send commands to a PostgreSQL connection and to
manage the results returned by it. They are normally created by the
connection's `~Connection.cursor()` method.

Psycopg can manage different kinds of "cursors", the objects used to send
queries and retrieve results from the server. They differ from each other in
aspects such as:

- Are the parameters bound on the client or on the server?
  :ref:`server-side-binding` can offer better performance (for instance
  allowing to use prepared statements) and reduced memory footprint, but may
  require stricter query definition and certain queries that work in
  `!psycopg2` might need to be adapted.

- Is the query result stored on the client or on the server? Server-side
  cursors allow partial retrieval of large datasets, but they might offer less
  performance in everyday usage.

- Are queries manipulated by Python (to handle placeholders in ``%s`` and
  ``%(name)s`` Python-style) or sent as they are to the PostgreSQL server
  (which only supports ``$1``, ``$2`` parameters)?

Psycopg exposes the following classes to implement the different strategies.
All the classes are exposed by the main `!psycopg` package. Every class has
also an `!Async`\ -prefixed counterparts, designed to be used in conjunction
with `AsyncConnection` in `asyncio` programs.

================= =========== =========== ==================== ==================================
Class             Binding     Storage     Placeholders         See also
================= =========== =========== ==================== ==================================
`Cursor`          server-side client-side ``%s``, ``%(name)s`` :ref:`client-side-cursors`
`ClientCursor`    client-side client-side ``%s``, ``%(name)s`` :ref:`client-side-binding-cursors`
`ServerCursor`    server-side server-side ``%s``, ``%(name)s`` :ref:`server-side-cursors`
`RawCursor`       server-side client-side ``$1``               :ref:`raw-query-cursors`
`RawServerCursor` server-side server-side ``$1``               :ref:`raw-query-cursors`
================= =========== =========== ==================== ==================================

If not specified by a `~Connection.cursor_factory`, `~Connection.cursor()`
will usually produce `Cursor` objects.


.. index::
    double: Cursor; Client-side

.. _client-side-cursors:

Client-side cursors
-------------------

Client-side cursors are what Psycopg uses in its normal querying process.
They are implemented by the `Cursor` and `AsyncCursor` classes. In such
querying pattern, after a cursor sends a query to the server (usually calling
`~Cursor.execute()`), the server replies transferring to the client the whole
set of results requested, which is stored in the state of the same cursor and
from where it can be read from Python code (using methods such as
`~Cursor.fetchone()` and siblings).

This querying process is very scalable because, after a query result has been
transmitted to the client, the server doesn't keep any state. Because the
results are already in the client memory, iterating its rows is very quick.

The downside of this querying method is that the entire result has to be
transmitted completely to the client (with a time proportional to its size)
and the client needs enough memory to hold it, so it is only suitable for
reasonably small result sets.


.. index::
    double: Cursor; Client-binding

.. _client-side-binding-cursors:

Client-side-binding cursors
---------------------------

.. versionadded:: 3.1

The previously described :ref:`client-side cursors <client-side-cursors>` send
the query and the parameters separately to the server. This is the most
efficient way to process parametrised queries and allows to build several
features and optimizations. However, not all types of queries can be bound
server-side; in particular no Data Definition Language query can. See
:ref:`server-side-binding` for the description of these problems.

The `ClientCursor` (and its `AsyncClientCursor` async counterpart) merge the
query on the client and send the query and the parameters merged together to
the server. This allows to parametrize any type of PostgreSQL statement, not
only queries (:sql:`SELECT`) and Data Manipulation statements (:sql:`INSERT`,
:sql:`UPDATE`, :sql:`DELETE`).

Using `!ClientCursor`, Psycopg 3 behaviour will be more similar to `psycopg2`
(which only implements client-side binding) and could be useful to port
Psycopg 2 programs more easily to Psycopg 3. The objects in the `sql` module
allow for greater flexibility (for instance to parametrize a table name too,
not only values); however, for simple cases, a `!ClientCursor` could be the
right object.

In order to obtain `!ClientCursor` from a connection, you can set its
`~Connection.cursor_factory` (at init time or changing its attribute
afterwards):

.. code:: python

    from psycopg import connect, ClientCursor

    conn = psycopg.connect(DSN, cursor_factory=ClientCursor)
    cur = conn.cursor()
    # <psycopg.ClientCursor [no result] [IDLE] (database=piro) at 0x7fd977ae2880>

If you need to create a one-off client-side-binding cursor out of a normal
connection, you can just use the `~ClientCursor` class passing the connection
as argument.

.. code:: python

    conn = psycopg.connect(DSN)
    cur = psycopg.ClientCursor(conn)


.. warning::

    Client-side cursors don't support :ref:`binary parameters and return
    values <binary-data>` and don't support :ref:`prepared statements
    <prepared-statements>`.

.. tip::

    The best use for client-side binding cursors is probably to port large
    Psycopg 2 code to Psycopg 3, especially for programs making wide use of
    Data Definition Language statements.

    The `psycopg.sql` module allows for more generic client-side query
    composition, to mix client- and server-side parameters binding, and allows
    to parametrize tables and fields names too, or entirely generic SQL
    snippets.


.. index::
    single: PgBouncer
    double: Query protocol; simple

.. _simple-query-protocol:

Simple query protocol
^^^^^^^^^^^^^^^^^^^^^

Using the `!ClientCursor` should ensure that psycopg will always use the
`simple query protocol`__ for querying. In most cases, the choice of the
fronted/backend protocol used is transparent on PostgreSQL. However, in some
case using the simple query protocol is mandatory. This is the case querying
the `PgBouncer admin console`__ for instance, which doesn't support the
extended query protocol.

.. __: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY
.. __: https://www.pgbouncer.org/usage.html#admin-console

.. code:: python

    from psycopg import connect, ClientCursor

    conn = psycopg.connect(ADMIN_DSN, cursor_factory=ClientCursor)
    cur = conn.cursor()
    cur.execute("SHOW STATS")
    cur.fetchall()

.. versionchanged:: 3.1.20
    While querying using the `!ClientCursor` works well with PgBouncer, the
    connection's COMMIT and ROLLBACK commands are only ensured to be executed
    using the simple query protocol starting from Psycopg 3.1.20.

    In previous versions you should use an autocommit connection in order to
    query the PgBouncer admin console:

    .. code:: python

        from psycopg import connect, ClientCursor

        conn = psycopg.connect(ADMIN_DSN, cursor_factory=ClientCursor, autocommit=True)
        ...


.. index::
    double: Cursor; Server-side
    single: Portal
    double: Cursor; Named

.. _server-side-cursors:

Server-side cursors
-------------------

PostgreSQL has its own concept of *cursor* too (sometimes also called
*portal*). When a database cursor is created, the query is not necessarily
completely processed: the server might be able to produce results only as they
are needed. Only the results requested are transmitted to the client: if the
query result is very large but the client only needs the first few records it
is possible to transmit only them.

The downside is that the server needs to keep track of the partially
processed results, so it uses more memory and resources on the server.

Psycopg allows the use of server-side cursors using the classes `ServerCursor`
and `AsyncServerCursor`. They are usually created by passing the `!name`
parameter to the `~Connection.cursor()` method (reason for which, in
`!psycopg2`, they are usually called *named cursors*). The use of these classes
is similar to their client-side counterparts: their interface is the same, but
behind the scene they send commands to control the state of the cursor on the
server (for instance when fetching new records or when moving using
`~Cursor.scroll()`).

Using a server-side cursor it is possible to process datasets larger than what
would fit in the client's memory. However for small queries they are less
efficient because it takes more commands to receive their result, so you
should use them only if you need to process huge results or if only a partial
result is needed.

.. seealso::

    Server-side cursors are created and managed by `ServerCursor` using SQL
    commands such as DECLARE_, FETCH_, MOVE_. The PostgreSQL documentation
    gives a good idea of what is possible to do with them.

    .. _DECLARE: https://www.postgresql.org/docs/current/sql-declare.html
    .. _FETCH: https://www.postgresql.org/docs/current/sql-fetch.html
    .. _MOVE: https://www.postgresql.org/docs/current/sql-move.html


.. _cursor-steal:

"Stealing" an existing cursor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A Psycopg `ServerCursor` can be also used to consume a cursor which was
created in other ways than the :sql:`DECLARE` that `ServerCursor.execute()`
runs behind the scene.

For instance if you have a `PL/pgSQL function returning a cursor`__:

.. __: https://www.postgresql.org/docs/current/plpgsql-cursors.html

.. code:: postgres

    CREATE FUNCTION reffunc(refcursor) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT col FROM test;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;

you can run a one-off command in the same connection to call it (e.g. using
`Connection.execute()`) in order to create the cursor on the server:

.. code:: python

    conn.execute("SELECT reffunc('curname')")

after which you can create a server-side cursor declared by the same name, and
directly call the fetch methods, skipping the `~ServerCursor.execute()` call:

.. code:: python

    cur = conn.cursor('curname')
    # no cur.execute()
    for record in cur:  # or cur.fetchone(), cur.fetchmany()...
        # do something with record


.. _raw-query-cursors:

Raw query cursors
-----------------

.. versionadded:: 3.2

The `RawCursor` and `AsyncRawCursor` classes allow users to use PostgreSQL
native placeholders (``$1``, ``$2``, etc.) in their queries instead of the
standard ``%s`` placeholder. This can be useful when it's desirable to pass
the query unmodified to PostgreSQL and rely on PostgreSQL's placeholder
functionality, such as when dealing with a very complex query containing
``%s`` inside strings, dollar-quoted strings or elsewhere.

One important note is that raw query cursors only accept positional arguments
in the form of a list or tuple. This means you cannot use named arguments
(i.e., dictionaries).

`!RawCursor` behaves like `Cursor`, in returning the complete result from the
server to the client. The `RawServerCursor` and `AsyncRawServerCursor`
implement :ref:`server-side-cursors` with raw PostgreSQL placeholders.

There are two ways to use raw query cursors:

1. Using the cursor factory:

.. code:: python

    from psycopg import connect, RawCursor

    with connect(dsn, cursor_factory=RawCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT $1, $2", [1, "Hello"])
            assert cur.fetchone() == (1, "Hello")

2. Instantiating a cursor:

.. code:: python

    from psycopg import connect, RawCursor

    with connect(dsn) as conn:
        with RawCursor(conn) as cur:
            cur.execute("SELECT $1, $2", [1, "Hello"])
            assert cur.fetchone() == (1, "Hello")
