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


If you are working in an `asyncio` project you can use a very similar pattern:

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


The main entry points of Psycopg are:

- The function `~psycopg3.connect()` creates a new database session and
  returns a new `connection` instance. `psycopg3.AsyncConnection.connect()`
  creates an asyncio connection instead.

- The `connection` class encapsulates a database session. It allows to:

  - create new `cursor` instances using the `~connection.cursor()` method to
    execute database commands and queries,

  - terminate transactions using the methods `~connection.commit()` or
    `~connection.rollback()`.

- The class `cursor` allows interaction with the database:

  - send commands to the database using methods such as `~cursor.execute()`
    and `~cursor.executemany()`,

  - retrieve data from the database :ref:`by iteration <cursor-iterable>` or
    using methods such as `~cursor.fetchone()`, `~cursor.fetchmany()`,
    `~cursor.fetchall()`.


.. index::
    pair: Query; Parameters

.. _query-parameters:

Passing parameters to SQL queries
---------------------------------

TODO: lift from psycopg2 docs


.. _transactions:

Transaction management
----------------------

TODO:


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
documentation), you should keep the connection in `~connection.autocommit`
mode if you wish to receive or send notifications in a timely manner.

Notifications are received as instances of `~psycopg3.Notify`. If you are
reserving a connection only to receive notifications, the simplest way is to
consume the `~psycopg3.Connection.notifies` generator. The generator can be
stopped using ``close()``. The following example will print notifications and
stop when one containing the ``stop`` message is received.

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

Alternatively, you can use `~psycopg3.Connection.add_notify_handler()` to
register a callback function, which will be invoked whenever a notification is
received, during the normal query processing; you will be then able to use the
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
