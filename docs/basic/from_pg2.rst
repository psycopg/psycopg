.. index::
    pair: psycopg2; Differences

.. currentmodule:: psycopg


Differences from ``psycopg2``
=============================

Psycopg 3 uses the common DBAPI structure of many other database adapter and
tries to behave as close as possible to `!psycopg2`. There are however a few
differences to be aware of.


.. _server-side-binding:

Server-side binding
-------------------

Psycopg 3 sends the query and the parameters to the server separately,
instead of merging them client-side. PostgreSQL may behave slightly
differently in this case, usually throwing an error and suggesting to use an
explicit cast.

.. code:: python

    cur.execute("SELECT '[10,20,30]'::jsonb -> 1").fetchone()
    # returns (20,)

    cur.execute("SELECT '[10,20,30]'::jsonb -> %s", [1]).fetchone()
    # raises an exception:
    # UndefinedFunction: operator does not exist: jsonb -> numeric

    cur.execute("SELECT '[10,20,30]'::jsonb -> %s::int", [1]).fetchone()
    # returns (20,)

PostgreSQL will also reject the execution of several queries at once
(separated by semicolon), if they contain parameters. If parameters are used
you should use distinct `execute()` calls; otherwise you may consider merging
the query client-side, using `psycopg.sql` module.

Certain commands cannot be used with server-side binding, for instance
:sql:`SET` or :sql:`NOTIFY`::

    >>> cur.execute("SET timezone TO %s", ["utc"])
    ...
    psycopg.errors.SyntaxError: syntax error at or near "$1"

Sometimes PostgreSQL offers an alternative (e.g. :sql:`SELECT set_config()`,
:sql:`SELECT pg_notify()`). If no alternative exist you can use `psycopg.sql`
to compose the query client-side.

You cannot use :sql:`IN %s` and pass a tuple, because `IN ()` is an SQL
construct. You must use :sql:`= any(%s)` and pass a list. Note that this also
works for an empty list, whereas an empty tuple would have resulted in an
error.


.. _diff-adapt:

Different adaptation system
---------------------------

The adaptation system has been completely rewritten, in order to address
server-side parameters adaptation, but also to consider performance,
flexibility, ease of customization.

The behaviour with builtin data should be as expected; if you have customised
the way to adapt data, or you have your own extension types, you should look
at the new objects involved in adaptation.

.. seealso::

    - :ref:`types-adaptation` for the basic behaviour.
    - :ref:`adaptation` for more advanced use.


.. _diff-copy:

Copy is no more file-based
--------------------------

`!psycopg2` exposes :ref:`a few copy methods <pg2:copy>` to interact with
PostgreSQL :sql:`COPY`. The interface doesn't make easy to load
dynamically-generated data to the database.

There is now a single `~Cursor.copy()` method, which is similar to
`!psycopg2` `!copy_expert()` in accepting a free-form :sql:`COPY` command and
returns an object to read/write data, block-wise or record-wise. The different
usage pattern also enables :sql:`COPY` to be used in async interactions.

.. seealso:: See :ref:`copy` for the details.


.. _diff-with:

``with`` connection
-------------------

In `!psycopg2`, using the syntax :ref:`with connection <pg2:with>`,
only the transaction is closed, not the connection. This behaviour is
surprising for people used to several other Python classes wrapping resources,
such as files.

In psycopg3, using :ref:`with connection <with-connection>` will close the
connection at the end of the `!with` block, making handling the connection
resources more familiar.

In order to manage transactions as blocks you can use the
`Connection.transaction()` method, which allows for finer control, for
instance to use nested transactions.

.. seealso:: See :ref:`transaction-block` for details.


.. _diff-callproc:

``callproc()`` is gone
----------------------

`cursor.callproc()` is not implemented. The method has a simplistic semantic
which doesn't account for PostgreSQL positional parameters, procedures,
set-returning functions... Use a normal `~Cursor.execute()` with :sql:`SELECT
function_name(...)` or :sql:`CALL procedure_name(...)` instead.


What's new in Psycopg 3
-----------------------

.. admonition:: TODO

    to be completed

- `asyncio` support.
- Several data types are adapted out-of-the-box: uuid, network, range, bytea,
  array of any supported type are dealt with automatically.
- Access to the low-level libpq functions.
