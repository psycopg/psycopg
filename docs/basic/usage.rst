.. currentmodule:: psycopg3

.. _module-usage:

Basic module usage
==================

The basic Psycopg usage is common to all the database adapters implementing
the `DB-API`__ protocol. Other database adapters, such as the builtin
`sqlite3` or `psycopg2`, have roughly the same pattern of interaction.


.. index::
    pair: Example; Usage

.. _usage:

Main objects in ``psycopg3``
----------------------------

Here is an interactive session showing some of the basic commands:

.. __: https://www.python.org/dev/peps/pep-0249/

.. code:: python

    import psycopg3

    # Connect to an existing database
    with psycopg3.connect("dbname=test user=postgres") as conn:

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

- Using these objects as context managers (i.e. using ``with``) will make sure
  to close them and free their resources at the end of the block (notice that
  :ref:`this is different from psycopg2 <diff-with>`).


.. seealso::

    A few important topics you will have to deal with are:

    - :ref:`query-parameters`.
    - :ref:`types-adaptation`.
    - :ref:`transactions`.


.. index::
    pair: Connection; ``with``

.. _with-connection:

Connection context
------------------

`!psycopg3` `Connection` can be used as a context manager:

.. code:: python

    with psycopg3.connect() as conn:
        ... # use the connection

    # the connection is now closed

When the block is exited, if there is a transaction open, it will be
committed. If an exception is raised within the block the transaction is
rolled back. In either case the connection is closed.

`AsyncConnection` can be also used as context manager, using ``async with``,
but be careful about its quirkiness: see :ref:`async-with` for details.
