.. currentmodule:: psycopg3

.. index:: asyncio

.. _async:

Async operations
================

psycopg3 `~Connection` and `~Cursor` have counterparts `~AsyncConnection` and
`~AsyncCursor` supporting an `asyncio` interface.

The design of the asynchronous objects is pretty much the same of the sync
ones: in order to use them you will only have to scatter the ``await`` keyword
here and there.

.. code:: python

    async with await psycopg3.AsyncConnection.connect(
            "dbname=test user=postgres") as aconn:
        async with aconn.cursor() as acur:
            await acur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))
            await acur.execute("SELECT * FROM test")
            await acur.fetchone()
            # will return (1, 100, "abc'def")
            async for record in acur:
                print(record)


.. index:: with

.. _async-with:

``with`` async connections
--------------------------

As seen in :ref:`the basic usage <usage>`, connections and cursors can act as
context managers, so you can run:

.. code:: python

    with psycopg3.connect("dbname=test user=postgres") as conn:
        with conn.cursor() as cur:
            cur.execute(...)
        # the cursor is closed upon leaving the context
    # the transaction is committed, the connection closed

For asynchronous connections it's *almost* what you'd expect, but
not quite. Please note that `~Connection.connect()` and `~Connection.cursor()`
*don't return a context*: they are both factory methods which return *an
object which can be used as a context*. That's because there are several use
cases where it's useful to handle the object manually and `!close()` them when
required.

As a consequence you cannot use `!async with connect()`: you have to do it in
two steps instead, as in

.. code:: python

    aconn = await psycopg3.AsyncConnection.connect():
    async with aconn:
        async with aconn.cursor() as cur:
            await cur.execute(...)

which can be condensed as:

.. code:: python

    async with await psycopg3.AsyncConnection.connect() as aconn:
        async with aconn.cursor() as cur:
            await cur.execute(...)

...but no less than that: you still need to do the double async thing.

The `AsyncConnection.cursor()` function is not marked as `!async` (it never
performs I/O), so you don't need an `!await` on it and you can use the normal
`async with` context manager.



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
.. _LISTEN: https://www.postgresql.org/docs/current/sql-listen.html
.. |NOTIFY| replace:: :sql:`NOTIFY`
.. _NOTIFY: https://www.postgresql.org/docs/current/sql-notify.html

Because of the way sessions interact with notifications (see |NOTIFY|_
documentation), you should keep the connection in `~Connection.autocommit`
mode if you wish to receive or send notifications in a timely manner.

Notifications are received as instances of `Notify`. If you are reserving a
connection only to receive notifications, the simplest way is to consume the
`Connection.notifies` generator. The generator can be stopped using
``close()``.

.. note::

    You don't need an `AsyncConnection` to handle notifications: a normal
    blocking `Connection` is perfectly valid.

The following example will print notifications and stop when one containing
the ``stop`` message is received.

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


.. index:: disconnections

.. _disconnections:

Detecting disconnections
------------------------

Sometimes it is useful to detect immediately when the connection with the
database is lost. One brutal way to do so is to poll a connection in a loop
running an endless stream of :sql:`SELECT 1`... *Don't* do so: polling is *so*
out of fashion. Besides, it is inefficient (unless what you really want is a
client-server generator of ones), it generates useless traffic and will only
detect a disconnection with an average delay of half the polling time.

A more efficient and timely way to detect a server disconnection is to get a
notification from the OS that the connection has something to say: only then
you can test the connection. You can dedicate a thread (or an asyncio task) to
wait on a connection: such thread will perform no activity until awaken by the
OS.

In a normal (non asyncio) program you can use the `selectors` module. Because
the `!Connection` implements a `~Connection.fileno()` method you can just
register it as a file-like object. You can run such code in a dedicated thread
(and using a dedicated connection) if the rest of the program happens to have
something else to do too.

.. code:: python

    import selectors

    sel = selectors.DefaultSelector()
    sel.register(conn, selectors.EVENT_READ)
    while True:
        if not sel.select(timeout=60.0):
            continue  # No FD activity detected in one minute

        # Activity detected. Is the connection still ok?
        try:
            conn.execute("select 1")
        except psycopg3.OperationalError:
            # You were disconnected: do something useful such as panicking
            logger.error("we lost our database!")
            sys.exit(1)

In an `asyncio` program you can dedicate a `~asyncio.Task` instead and do
something similar using `~asyncio.loop.add_reader`:

.. code:: python

    import asyncio

    ev = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_reader(conn.fileno(), ev.set)

    while True:
        try:
            await asyncio.wait_for(ev.wait(), 60.0)
        except asyncio.TimeoutError:
            continue  # No FD activity detected in one minute

        # Activity detected. Is the connection still ok?
        try:
            await conn.execute("select 1")
        except psycopg3.OperationalError:
            # Guess what happened
            ...
