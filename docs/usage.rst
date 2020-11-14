.. currentmodule:: psycopg3


.. index::
    pair: Example; Usage

.. _usage:

Basic module usage
==================

The basic Psycopg usage is common to all the database adapters implementing
the `DB API`__ protocol. Here is an interactive session showing some of the
basic commands:

.. __: https://www.python.org/dev/peps/pep-0249/

.. code:: python

    import psycopg3

    # Connect to an existing database
    conn = psycopg3.connect("dbname=test user=postgres")

    # Open a cursor to perform database operations
    cur = conn.cursor()

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

    # Close communication with the database
    cur.close()
    conn.close()


Note that the `cursor.execute()` method returns the cursor itself, so the
`fetch*()` methods can be appended right after it.

.. code:: python

    cur.execute("SELECT * FROM test").fetchone()

    for record in cur.execute("SELECT * FROM test"):
        print(record)


The main entry points of `!psycopg3` are:

- The function `connect()` creates a new database session and
  returns a new `connection` instance. `AsyncConnection.connect()`
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



.. index:: with

``with`` connections and cursors
--------------------------------

The connections and cursors act as context managers, so you can run:

.. code:: python

    with psycopg3.connect("dbname=test user=postgres") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))
            cur.execute("SELECT * FROM test").fetchone()
            # will return (1, 100, "abc'def")

        # the cursor is closed upon leaving the context

    # the transaction is committed on successful exit of the context
    # and the connection closed



.. index::
    pair: Query; Parameters

.. _query-parameters:

Passing parameters to SQL queries
---------------------------------

TODO: lift from psycopg2 docs



.. index::
    pair: Query; Parameters

.. _binary-data:

Binary parameters and results
-----------------------------

TODO: lift from psycopg2 docs



.. _transactions:

Transaction management
----------------------

TODO



.. index::
    pair: COPY; SQL command

.. _copy:

Using COPY TO and COPY FROM
---------------------------

`psycopg3` allows to operate with `PostgreSQL COPY protocol`__. :sql:`COPY` is
one of the most efficient ways to load data into the database (and to modify
it, with some SQL creativity).

.. __: https://www.postgresql.org/docs/current/sql-copy.html

Using `!psycopg3` you can do three things:

- loading data into the database row-by-row, from a stream of Python objects;
- loading data into the database block-by-block, with data already formatted in
  a way suitable for :sql:`COPY FROM`;
- reading data from the database block-by-block, with data emitted by a
  :sql:`COPY TO` statement.

The missing quadrant, copying data from database row-by-row, is not covered by
COPY because that's pretty much normal querying, and :sql:`COPY TO` doesn't
offer enough metadata to decode the data to Python objects.

The first option is the most powerful, because it allows to load data into the
database from any Python iterable (a list of tuple, or any iterable of
sequences): the Python values are adapted as they would be in normal querying.
To perform such operation use a :sql:`COPY [table] FROM STDIN` with
`Cursor.copy()` and use `~Copy.write_row()` on the resulting object in a
`!with` block. On exiting the block the operation will be concluded:

.. code:: python

    with cursor.copy("COPY table_name (col1, col2) FROM STDIN") as copy:
        for row in source:
            copy.write_row(row)

If an exception is raised inside the block, the operation is interrupted and
the records inserted so far discarded.

If data is already formatted in a way suitable for copy (for instance because
it is coming from a file resulting from a previous `COPY TO` operation) it can
be loaded using `Copy.write()` instead.

In order to read data in :sql:`COPY` format you can use a :sql:`COPY TO
STDOUT` statement and iterate over the resulting `Copy` object, which will
produce `!bytes`:

.. code:: python

    with open("data.out", "wb") as f:
        for data in cursor.copy("COPY table_name TO STDOUT") as copy:
            f.write(data)

Asynchronous operations are supported using the same patterns on an
`AsyncConnection`. For instance, if `!f` is an object supporting an
asynchronous `!read()` method and returning :sql:`COPY` data, a fully-async
copy operation could be:

.. code:: python

    async with (await cursor.copy("COPY data FROM STDIN")) as copy:
        data = await f.read()
        if not data:
            break

        await copy.write(data)

Binary data can be produced and consumed using :sql:`FORMAT BINARY` in the
:sql:`COPY` command: see :ref:`binary-data` for details and limitations.


.. index:: async

Async operations
================

psycopg3 `~Connection` and `~Cursor` have counterparts `~AsyncConnection` and
`~AsyncCursor` supporting an `asyncio` interface.

The design of the asynchronous objects is pretty much the same of the sync
ones: in order to use them you will only have to scatter the ``async`` keyword
here and there.

.. code:: python

    async with await psycopg3.AsyncConnection.connect(
            "dbname=test user=postgres") as aconn:
        async with await aconn.cursor() as acur:
            await acur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))
            await acur.execute("SELECT * FROM test")
            await acur.fetchone()
            # will return (1, 100, "abc'def")
            async for record in acur:
                print(record)



.. index::
    pair: Asynchronous; Notifications
    pair: LISTEN; SQL command
    pair: NOTIFY; SQL command

.. _async-notify:

Asynchronous notifications
--------------------------

Psycopg allows asynchronous interaction with other database sessions using the
facilities offered by PostgreSQL commands |LISTEN|_ and |NOTIFY|_. Please
refer to the PostgreSQL documentation for examples about how to use this form
of communication.

.. |LISTEN| replace:: :sql:`LISTEN`
.. _LISTEN: https://www.postgresql.org/docs/current/static/sql-listen.html
.. |NOTIFY| replace:: :sql:`NOTIFY`
.. _NOTIFY: https://www.postgresql.org/docs/current/static/sql-notify.html

Because of the way sessions interact with notifications (see |NOTIFY|_
documentation), you should keep the connection in `~Connection.autocommit`
mode if you wish to receive or send notifications in a timely manner.

Notifications are received as instances of `Notify`. If you are reserving a
connection only to receive notifications, the simplest way is to consume the
`Connection.notifies` generator. The generator can be stopped using
``close()``. The following example will print notifications and stop when one
containing the ``stop`` message is received.

.. code:: python

    import psycopg3
    conn = psycopg3.connect("", autocommit=True)
    conn.cursor().execute("LISTEN mychan")
    gen = conn.notifies()
    for notify in gen:
        print(notify)
        if notify.payload == "stop":
            gen.close()
    print("there, I stopped")

If you run some :sql:`NOTIFY` in a :program:`psql` session:

.. code:: psql

    =# notify mychan, 'hello';
    NOTIFY
    =# notify mychan, 'hey';
    NOTIFY
    =# notify mychan, 'stop';
    NOTIFY

You may get output from the Python process such as::

    Notify(channel='mychan', payload='hello', pid=961823)
    Notify(channel='mychan', payload='hey', pid=961823)
    Notify(channel='mychan', payload='stop', pid=961823)
    there, I stopped

Alternatively, you can use `~Connection.add_notify_handler()` to register a
callback function, which will be invoked whenever a notification is received,
during the normal query processing; you will be then able to use the
connection normally. Please note that in this case notifications will not be
received immediately, but only during a connection operation, such as a query.

.. code:: python

    conn.add_notify_handler(lambda n: print(f"got this: {n}"))

    # meanwhile in psql...
    # =# notify mychan, 'hey';
    # NOTIFY

    print(conn.cursor().execute("select 1").fetchone())
    # got this: Notify(channel='mychan', payload='hey', pid=961823)
    # (1,)
