
.. currentmodule:: psycopg

.. _module-example:

A Psycopg Example
=================

This section provides model code showing how Psycopg is often used.
The example is simple, but useful -- omitting only error handling.
The code walk-through explains how to
perform basic interactions with a Postgres database.
Tips and hints are provided to help with coding style and technique,
common mistakes, error handling, and related matters.

After completing this section the reader should be able to:

- Write a program that uses the basic features of Psycopg to read
  data from and write data to a database

- Have some notion of common pitfalls

- Simplify your code and transition from the style supported by
  Psycopg 2

- Work with transactions

- Take advantage of Psycopg's context manager related features

- Perform basic error handling

- Have some idea of which advanced Psycopg features you might want
  to use


.. index::
    pair: Example; Usage

.. _usage:

Psycopg 3's core objects
------------------------

Here is an interactive session showing some of the basic commands:

.. code:: python

    # Note: the module name is psycopg, not psycopg3
    import psycopg

    # Connect to an existing database
    with psycopg.connect("dbname=test user=postgres") as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Execute a command: this creates a new table
            cur.execute("""
                CREATE TABLE test (
                    id serial PRIMARY KEY,
                    num integer,
                    data text)
                """)

            # Pass data to fill a query placeholders and let Psycopg perform
            # the correct conversion (no SQL injections!)
            cur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))

            # Query the database and obtain data as Python objects.
            cur.execute("SELECT * FROM test")
            cur.fetchone()
            # will return (1, 100, "abc'def")

            # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
            # of several records, or even iterate on the cursor.
            for record in cur:
                print(record)
            # The row previously inserted is printed

            # Make the changes to the database persistent.
            # Assuming this listing shows the entirety of this program,
            # this statement is unnecessary.  The transaction is
            # automatically committed when the connection's `with`
            # statement completes.  That said, it does not hurt to commit.
            conn.commit()


This example uses some of the core Psycopg objects, exhibiting their
most common methods, and demonstrating how they relate.

- The function `~Connection.connect()` creates a new database session
  and returns a new `Connection` instance. (To create an `asyncio`
  connection use `AsyncConnection.connect()`.)

- The `~Connection` class encapsulates a database session. Here we see
  it:

  - Create new `~Cursor` instances using the `~Connection.cursor()` method to
    execute database commands and queries.

  - Terminate an active transaction. The methods
    `~Connection.commit()` or `~Connection.rollback()` may be used.

- The class `~Cursor` allows interaction with the database, in this
  case to:

  - Send commands to the database using methods such as `~Cursor.execute()`
    and `~Cursor.executemany()`.

  - Retrieve data from the database, iterating on the cursor or using methods
    such as `~Cursor.fetchone()`, `~Cursor.fetchmany()`, `~Cursor.fetchall()`.

  - Obtain previously retrieved results, re-iterating over query
    results.
    Cursors are iterables so, as is usual when an iterable is used in
    an iteration context, Python creates a new iterator from the
    `~Cursor` iterable from `cur` when the `!for` loop is executed.

- Using these objects as context managers ensures that they are closed
  and their resources freed when the `!with` block exits. (Notice that
  this differs from :ref:`psycopg2's behavior <diff-with>`.)


.. seealso::

    A few important topics you will have to deal with are:

    - :ref:`query-parameters`.
    - :ref:`types-adaptation`.
    - :ref:`transactions`.


Shortcuts
---------

The pattern above is familiar to `!psycopg2` users. However, Psycopg 3 also
exposes a few simple extensions which make the above pattern leaner:

- the `Connection` objects exposes an `~Connection.execute()` method,
  equivalent to creating a cursor, calling its `~Cursor.execute()` method, and
  returning it.

  .. code::

      # In Psycopg 2
      cur = conn.cursor()
      cur.execute(...)

      # In Psycopg 3
      cur = conn.execute(...)

- The `Cursor.execute()` method returns `!self`. This means that you can chain
  a fetch operation, such as `~Cursor.fetchone()`, to the `!execute()` call:

  .. code::

      # In Psycopg 2
      cur.execute(...)
      record = cur.fetchone()

      cur.execute(...)
      for record in cur:
          ...

      # In Psycopg 3
      record = cur.execute(...).fetchone()

      for record in cur.execute(...):
          ...

Using them together, in simple cases, you can go from creating a connection to
using a result in a single expression:

.. code::

    print(psycopg.connect(DSN).execute("SELECT now()").fetchone()[0])
    # 2042-07-12 18:15:10.706497+01:00


.. index::
    pair: Connection; `!with`

.. _with-connection:

Connection context
------------------

In Psycopg 3 a `Connection` can be used as a context manager:

.. code:: python

    with psycopg.connect() as conn:
        ... # use the connection

    # the connection is now closed

When the block is exited, if there is a transaction open, it will be
committed. If an exception is raised within the block the transaction is
rolled back. In both cases the connection is closed. It is roughly the
equivalent of:

.. code:: python

    conn = psycopg.connect()
    try:
        ... # use the connection
    except BaseException:
        conn.rollback()
    else:
        conn.commit()
    finally:
        conn.close()

.. note::
    This behaviour is not what `!psycopg2` does: in `!psycopg2` :ref:`there is
    no final close() <pg2:with>` and the connection can be used in several
    `!with` statements to manage different transactions. This behaviour has
    been considered non-standard and surprising so it has been replaced by the
    more explicit `~Connection.transaction()` block.

While the above pattern is what most people would use, there are other
ways to use `connect()`.
Calling `connect()` without using a `!with` still returns a
connection.  But in this case the developer must explicitly call
`~Connection.commit()`, `~Connection.rollback()`, and
`~Connection.close()` as and where needed.

.. warning::
    Altering database content always begins a transaction.
    By default, this is transaction that is left open and must be
    committed or the alterations are discarded!

    Psycopg does not always automatically commit your transactions!

    If a connection is left to go out of scope, and there are no calls
    to the connection's transaction management methods, what happens
    to an open transaction differs depending on whether a `!with` block
    is in use:

    - If there is no `!with` block and the connection has an open
      transaction and the program exits, the transaction is rolled
      back before the connection is closed.
      (This is also the behavior seen when a connection like this is
      garbage collected.)

    - If the connection is used as a context manager in a `!with`
      block and a transaction is open when the block is exited, the
      transaction is committed before the connection is closed.

Use a `!with` block when your intention is to execute a set of
operations and commit the result.
This is the usual case.
But if your connection life cycle and transaction pattern is unusual,
or you want more control, it may be more convenient to avoid using
`!with`\.

See :ref:`transactions` for more information.

`AsyncConnection` can be also used as context manager, using ``async
with``.
But be careful because it is quirky: see :ref:`async-with` for details.


Adapting pyscopg to your program
--------------------------------

The above :ref:`pattern of use <usage>` only shows the default behaviour of
the adapter. Psycopg can be customized in several ways, to allow the smoothest
integration between your Python program and your PostgreSQL database:

- If your program is concurrent and based on `asyncio` instead of on
  threads/processes, you can use :ref:`async connections and cursors <async>`.

- If you want to customize the objects that the cursor returns, instead of
  receiving tuples, you can specify your :ref:`row factories <row-factories>`.

- If you want to customize how Python values and PostgreSQL types are mapped
  into each other, beside the :ref:`basic type mapping <types-adaptation>`,
  you can :ref:`configure your types <adaptation>`.
