.. currentmodule:: psycopg

.. _module-usage:

Basic module usage
==================

The basic Psycopg usage is common to all the database adapters implementing
the `DB-API`__ protocol. Other database adapters, such as the builtin
`sqlite3` or `psycopg2`, have roughly the same pattern of interaction.

.. __: https://www.python.org/dev/peps/pep-0249/


.. index::
    pair: Example; Usage

.. _usage:

Main objects in Psycopg 3
-------------------------

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
            # of several records, or even iterate on the cursor
            for record in cur:
                print(record)

            # Make the changes to the database persistent
            conn.commit()


In the example you can see some of the main objects and methods and how they
relate to each other:

- The function `~Connection.connect()` creates a new database session and
  returns a new `Connection` instance. `AsyncConnection.connect()`
  creates an `asyncio` connection instead.

- The `~Connection` class encapsulates a database session. It allows to:

  - create new `~Cursor` instances using the `~Connection.cursor()` method to
    execute database commands and queries,

  - terminate transactions using the methods `~Connection.commit()` or
    `~Connection.rollback()`.

- The class `~Cursor` allows interaction with the database:

  - send commands to the database using methods such as `~Cursor.execute()`
    and `~Cursor.executemany()`,

  - retrieve data from the database, iterating on the cursor or using methods
    such as `~Cursor.fetchone()`, `~Cursor.fetchmany()`, `~Cursor.fetchall()`.

- Using these objects as context managers (i.e. using `!with`) will make sure
  to close them and free their resources at the end of the block (notice that
  :ref:`this is different from psycopg2 <diff-with>`).


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

Psycopg 3 `Connection` can be used as a context manager:

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

Note that, while the above pattern is what most people would use, `connect()`
doesn't enter a block itself, but returns an "un-entered" connection, so that
it is still possible to use a connection regardless of the code scope and the
developer is free to use (and responsible for calling) `~Connection.commit()`,
`~Connection.rollback()`, `~Connection.close()` as and where needed.

.. warning::
    If a connection is just left to go out of scope, the way it will behave
    with or without the use of a `!with` block is different:

    - if the connection is used without a `!with` block, the server will find
      a connection closed INTRANS and roll back the current transaction;

    - if the connection is used with a `!with` block, there will be an
      explicit COMMIT and the operations will be finalised.

    You should use a `!with` block when your intention is just to execute a
    set of operations and then committing the result, which is the most usual
    thing to do with a connection. If your connection life cycle and
    transaction pattern is different, and want more control on it, the use
    without `!with` might be more convenient.

    See :ref:`transactions` for more information.

`AsyncConnection` can be also used as context manager, using ``async with``,
but be careful about its quirkiness: see :ref:`async-with` for details.


Adapting pyscopg to your program
--------------------------------

The above :ref:`pattern of use <usage>` only shows the default behaviour of
the adapter. Psycopg can be customised in several ways, to allow the smoothest
integration between your Python program and your PostgreSQL database:

- If your program is concurrent and based on `asyncio` instead of on
  threads/processes, you can use :ref:`async connections and cursors <async>`.

- If you want to customise the objects that the cursor returns, instead of
  receiving tuples, you can specify your :ref:`row factories <row-factories>`.

- If you want to customise how Python values and PostgreSQL types are mapped
  into each other, beside the :ref:`basic type mapping <types-adaptation>`,
  you can :ref:`configure your types <adaptation>`.
