.. currentmodule:: psycopg

.. index::
    single: Cursor

.. _cursor-types:

Cursor types
============

Psycopg can manage kinds of "cursors" which differ in where the state of a
query being processed is stored: :ref:`client-side-cursors` and
:ref:`server-side-cursors`.

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
    double: Cursor; Server-side
    single: Portal
    double: Cursor; Named

.. _server-side-cursors:

Server-side cursors
-------------------

PostgreSQL also has its own concept of *cursor* (sometimes also called
*portal*). When a database cursor is created, the query is not necessarily
completely processed: the server might be able to produce results only as they
are needed. Only the results requested are transmitted to the client: if the
query result is very large but the client only needs the first few records it
is possible to transmit only them.

The downside is that the server needs to keep track of the partially
processed results, so it uses more memory and resources on the server.

Psycopg allows the use of server-side cursors using the classes `ServerCursor`
and `AsyncServerCursor`. They are usually created by passing the *name*
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
-----------------------------

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
