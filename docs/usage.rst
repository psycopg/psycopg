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

.. _with-statement:

``with`` connections and cursors
--------------------------------

Connections and cursors act as context managers, so you can run:

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

For asynchronous connections and cursor it's *almost* what you'd expect, but
not quite. Please note that `!connect()` and `!cursor()` *don't return a
context*: they are both factory methods which return *an object which can be
used as a context*. So you cannot use ``async with connect()``: you have to do
it in two steps instead, as in

.. code:: python

    aconn = await psycopg3.AsyncConnection.connect():
    async with aconn:
        cur = await aconn.cursor()
        async with cur:
            await cur.execute(...)

which can be condensed as:

.. code:: python

    async with (await psycopg3.AsyncConnection.connect()) as aconn:
        async with (await aconn.cursor()) as cur:
            await cur.execute(...)

...but no less than that: you still need to do the double async thing.


.. index::
    pair: Query; Parameters

.. _query-parameters:

Passing parameters to SQL queries
=================================

``psycopg3`` converts Python variables to SQL values using their types: the
Python type determines the function used to convert the object into a string
representation suitable for PostgreSQL.  Many standard Python types are
`adapted out of the box`__ to the correct SQL representation.

.. __: python-types-adaptation_

Passing parameters to an SQL statement happens in functions such as
`Cursor.execute()` by using ``%s`` placeholders in the SQL statement, and
passing a sequence of values as the second argument of the function. For
example the Python function call::

    >>> cur.execute("""
    ...     INSERT INTO some_table (an_int, a_date, a_string)
    ...     VALUES (%s, %s, %s);
    ...     """,
    ...     (10, datetime.date(2020, 11, 18), "O'Reilly"))

is roughly equivalent to the SQL command:

.. code-block:: sql

    INSERT INTO some_table (an_int, a_date, a_string)
    VALUES (10, '2020-11-18', 'O''Reilly');

Note that the parameters will not be really merged to the query: query and the
parameters are sent to the server separately: see :ref:`server-side-binding`
for details.

Named arguments are supported too using :samp:`%({name})s` placeholders in the
query and specifying the values into a mapping.  Using named arguments allows
to specify the values in any order and to repeat the same value in several
places in the query::

    >>> cur.execute("""
    ...     INSERT INTO some_table (an_int, a_date, another_date, a_string)
    ...     VALUES (%(int)s, %(date)s, %(date)s, %(str)s);
    ...     """,
    ...     {'int': 10, 'str': "O'Reilly", 'date': datetime.date(2020, 11, 18)})

Using characters ``%``, ``(``, ``)`` in the argument names is not supported.

When parameters are used, in order to include a literal ``%`` in the query you
can use the ``%%`` string::

    >>> cur.execute("SELECT (%s % 2) = 0 AS even", (10,))       # WRONG
    >>> cur.execute("SELECT (%s %% 2) = 0 AS even", (10,))      # correct

While the mechanism resembles regular Python strings manipulation, there are a
few subtle differences you should care about when passing parameters to a
query.

- The Python string operator ``%`` *must not be used*: the `~cursor.execute()`
  method accepts a tuple or dictionary of values as second parameter.
  |sql-warn|__:

  .. |sql-warn| replace:: **Never** use ``%`` or ``+`` to merge values
      into queries

  .. __: sql-injection_

    >>> cur.execute("INSERT INTO numbers VALUES (%s, %s)" % (10, 20)) # WRONG
    >>> cur.execute("INSERT INTO numbers VALUES (%s, %s)", (10, 20))  # correct

- For positional variables binding, *the second argument must always be a
  sequence*, even if it contains a single variable (remember that Python
  requires a comma to create a single element tuple)::

    >>> cur.execute("INSERT INTO foo VALUES (%s)", "bar")    # WRONG
    >>> cur.execute("INSERT INTO foo VALUES (%s)", ("bar"))  # WRONG
    >>> cur.execute("INSERT INTO foo VALUES (%s)", ("bar",)) # correct
    >>> cur.execute("INSERT INTO foo VALUES (%s)", ["bar"])  # correct

- The placeholder *must not be quoted*. Psycopg will add quotes where needed::

    >>> cur.execute("INSERT INTO numbers VALUES ('%s')", (10,)) # WRONG
    >>> cur.execute("INSERT INTO numbers VALUES (%s)", (10,))   # correct

- The variables placeholder *must always be a* ``%s``, even if a different
  placeholder (such as a ``%d`` for integers or ``%f`` for floats) may look
  more appropriate. Another placeholder used is ``%b``, to :ref:`adapt the
  object to binary type <binary-data>`::

    >>> cur.execute("INSERT INTO numbers VALUES (%d)", (10,))   # WRONG
    >>> cur.execute("INSERT INTO numbers VALUES (%s)", (10,))   # correct

- Only query values should be bound via this method: it shouldn't be used to
  merge table or field names to the query. If you need to generate dynamically
  SQL queries (for instance choosing dynamically a table name) you can use the
  facilities provided by the `psycopg3.sql` module::

    >>> cur.execute("INSERT INTO %s VALUES (%s)", ('numbers', 10))  # WRONG
    >>> cur.execute(                                                # correct
    ...     SQL("INSERT INTO {} VALUES (%s)").format(Identifier('numbers')),
    ...     (10,))


.. index:: Security, SQL injection

.. _sql-injection:

The problem with the query parameters
-------------------------------------

The SQL representation of many data types is often different from their Python
string representation. The typical example is with single quotes in strings:
in SQL single quotes are used as string literal delimiters, so the ones
appearing inside the string itself must be escaped, whereas in Python single
quotes can be left unescaped if the string is delimited by double quotes.

Because of the difference, sometime subtle, between the data types
representations, a naïve approach to query strings composition, such as using
Python strings concatenation, is a recipe for *terrible* problems::

    >>> SQL = "INSERT INTO authors (name) VALUES ('%s')" # NEVER DO THIS
    >>> data = ("O'Reilly", )
    >>> cur.execute(SQL % data) # THIS WILL FAIL MISERABLY
    SyntaxError: syntax error at or near "Reilly"

If the variables containing the data to send to the database come from an
untrusted source (such as a form published on a web site) an attacker could
easily craft a malformed string, either gaining access to unauthorized data or
performing destructive operations on the database. This form of attack is
called `SQL injection`_ and is known to be one of the most widespread forms of
attack to database servers. Before continuing, please print `this page`__ as a
memo and hang it onto your desk.

.. _SQL injection: https://en.wikipedia.org/wiki/SQL_injection
.. __: https://xkcd.com/327/

Psycopg can `automatically convert Python objects to SQL values`__: using this
feature your code will be more robust and reliable. We must stress this point:

.. __: python-types-adaptation_

.. warning::

    - Don't merge manually values to a query: hackers from a foreign country
      will break into your computer and steal not only your disks, but also
      your cds, leaving you only with the three most embarrassing records you
      ever bought. On tape.

    - If you use the ``%`` operator to merge values to a query, con artists
      will seduce your cat, who will run away taking your credit card
      and your sunglasses with them.

    - If you use ``+`` to merge a textual value to a string, bad guys in
      balaclava will find their way to your fridge, drink all your beer, and
      leave your toilet sit up and your toilet paper in the wrong orientation.

    - You don't want to merge manually values to a query: :ref:`use the
      provided methods <query-parameters>` instead.

The correct way to pass variables in a SQL command is using the second
argument of the `Cursor.execute()` method::

    >>> SQL = "INSERT INTO authors (name) VALUES (%s)"  # Note: no quotes
    >>> data = ("O'Reilly", )
    >>> cur.execute(SQL, data)  # Note: no % operator


.. index::
    single: Adaptation
    pair: Objects; Adaptation
    single: Data types; Adaptation

.. _python-types-adaptation:

Adaptation of Python values to SQL types
----------------------------------------

Many standard Python types are adapted into SQL and returned as Python
objects when a query is executed.

The following table shows the default mapping between Python and PostgreSQL
types:

TODO: complete table

.. only:: html

  .. table::
    :class: data-types

    +--------------------+-------------------------+--------------------------+
    | Python             | PostgreSQL              | See also                 |
    +====================+=========================+==========================+
    | `!bool`            | :sql:`bool`             | :ref:`adapt-bool`        |
    +--------------------+-------------------------+--------------------------+
    | `!float`           | | :sql:`real`           | :ref:`adapt-numbers`     |
    |                    | | :sql:`double`         |                          |
    +--------------------+-------------------------+                          |
    | | `!int`           | | :sql:`smallint`       |                          |
    | |                  | | :sql:`integer`        |                          |
    |                    | | :sql:`bigint`         |                          |
    +--------------------+-------------------------+                          |
    | `~decimal.Decimal` | :sql:`numeric`          |                          |
    +--------------------+-------------------------+--------------------------+
    | | `!str`           | | :sql:`varchar`        | :ref:`adapt-string`      |
    | |                  | | :sql:`text`           |                          |
    +--------------------+-------------------------+--------------------------+
    | | `bytes`          | :sql:`bytea`            | :ref:`adapt-binary`      |
    +--------------------+-------------------------+--------------------------+
    | `!date`            | :sql:`date`             | :ref:`adapt-date`        |
    +--------------------+-------------------------+                          |
    | `!time`            | | :sql:`time`           |                          |
    |                    | | :sql:`timetz`         |                          |
    +--------------------+-------------------------+                          |
    | `!datetime`        | | :sql:`timestamp`      |                          |
    |                    | | :sql:`timestamptz`    |                          |
    +--------------------+-------------------------+                          |
    | `!timedelta`       | :sql:`interval`         |                          |
    +--------------------+-------------------------+--------------------------+
    | `!list`            | :sql:`ARRAY`            | :ref:`adapt-list`        |
    +--------------------+-------------------------+--------------------------+
    | | `!tuple`         | Composite types         |:ref:`adapt-composite`    |
    | | `!namedtuple`    |                         |                          |
    +--------------------+-------------------------+--------------------------+
    | `!dict`            | :sql:`hstore`           | :ref:`adapt-hstore`      |
    +--------------------+-------------------------+--------------------------+
    | Psycopg's `!Range` | :sql:`range`            | :ref:`adapt-range`       |
    +--------------------+-------------------------+--------------------------+
    | Anything\ |tm|     | :sql:`json`             | :ref:`adapt-json`        |
    +--------------------+-------------------------+--------------------------+
    | `~uuid.UUID`       | :sql:`uuid`             | :ref:`adapt-uuid`        |
    +--------------------+-------------------------+--------------------------+
    | `ipaddress`        | | :sql:`inet`           | :ref:`adapt-network`     |
    | objects            | | :sql:`cidr`           |                          |
    +--------------------+-------------------------+--------------------------+

.. |tm| unicode:: U+2122



.. index::
    pair: Binary; Parameters

.. _binary-data:

Binary parameters and results
-----------------------------

TODO


.. index:: Transactions management
.. index:: InFailedSqlTransaction
.. index:: idle in transaction

.. _transactions:

Transaction management
======================

`!psycopg3` has a behaviour that may result surprising compared to
:program:`psql`: by default, any database operation will start a new
transaction. As a consequence, changes made by any cursor of the connection
will not be visible until `Connection.commit()` is called, and will be
discarded by `Connection.rollback()`. The following operation on the same
connection will start a new transaction.

If a database operation fails, the server will refuse further commands, until
a `~rollback()` is called.

.. hint::

    If a database operation fails with an error message such as
    *InFailedSqlTransaction: current transaction is aborted, commands ignored
    until end of transaction block*, it means that **a previous operation
    failed** and the database session is in a state of error. You need to call
    `!rollback()` if you want to keep on using the same connection.

The manual commit requirement can be suspended using `~Connection.autocommit`,
either as connection attribute or as `~psycopg3.Connection.connect()`
parameter. This may be required to run operations that need to run outside a
transaction, such as :sql:`CREATE DATABASE`, :sql:`VACUUM`, :sql:`CALL` on
`stored procedures`__ using transaction control.

.. __: https://www.postgresql.org/docs/current/xproc.html

.. warning::

    By default even a simple :sql:`SELECT` will start a transaction: in
    long-running programs, if no further action is taken, the session will
    remain *idle in transaction*, an undesirable condition for several
    reasons (locks are held by the session, tables bloat...). For long lived
    scripts, either make sure to terminate a transaction as soon as possible or
    use an `~Connection.autocommit` connection.


.. _transaction-block:

Transaction blocks
------------------

A more transparent way to make sure that transactions are finalised at the
right time is to use ``with`` `Connection.transaction()` to create a
transaction block. When the block is entered a transaction is started; when
leaving the block the transaction is committed, or it is rolled back if an
exception is raised inside the block.

For instance, an hypothetical but extremely secure bank may have the following
code to avoid that no accident between the following two lines leaves the
accounts unbalanced:

.. code:: python

    with conn.transaction():
        move_money(conn, account1, -100)
        move_money(conn, account2, +100)

    # The transaction is now committed

But because the bank is, like, *extremely secure*, they also verify that no
account goes negative:

.. code:: python

    def move_money(conn, account, amount):
        new_balance = add_to_balance(conn, account, amount)
        if new_balance < 0:
            raise ValueError("account balance cannot go negative")

In case this function raises an exception, be it the `!ValueError` in the
example or any other exception expected or not, the transaction will be rolled
back, and the exception will propagate out of the `with` block, further down
the call stack.

Transaction blocks can also be nested (internal transaction blocks are
implemented using SAVEPOINT__): an exception raised inside an inner block
has a chance of being handled and not completely fail outer operations. The
following is an example where a series of operations interact with the
database: operations are allowed to fail, plus we also want to store the
number of operations successfully processed.

.. __: https://www.postgresql.org/docs/current/sql-savepoint.html

.. code:: python

    with conn.transaction() as tx1:
        num_ok = 0
        for operation in operations:
            try:
                with conn.transaction() as tx2:
                    unreliable_operation(conn, operation)
            except Exception:
                logger.exception(f"{operation} failed")
            else:
                num_ok += 1

        save_number_of_successes(conn, num_ok)

If `!unreliable_operation()` causes an error, including an operation causing a
database error, all its changes will be reverted. The exception bubbles up
outside the block: in the example it is intercepted by the ``try`` so that the
loop can complete. The outermost block is unaffected (unless other errors
happen there).

You can also write code to explicitly roll back any currently active
transaction block, by raising the `Rollback` exception. The exception "jumps"
to the end of a transaction block, rolling back its transaction but allowing
the program execution to continue from there. By default the exception rolls
back the innermost transaction block, but any current block can be specified
as the target. In the following example, an hypothetical `!CancelCommand`
may stop the processing and cancel any operation previously performed,
but not entirely committed yet.

.. code:: python

    from psycopg3 import Rollback

    with conn.transaction() as outer_tx:
        for command in commands():
            with conn.transaction() as inner_tx:
                if isinstance(command, CancelCommand):
                    raise Rollback(outer_tx)
            process_command(command)

    # If `Rollback` is raised, it would propagate only up to this block,
    # and the program would continue from here with no exception.


.. index::
    pair: COPY; SQL command

.. _copy:

Using COPY TO and COPY FROM
===========================

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

The missing quadrant, copying data from the database row-by-row, is not
covered by COPY because that's pretty much normal querying, and :sql:`COPY TO`
doesn't offer enough metadata to decode the data to Python objects.

The first option is the most powerful, because it allows to load data into the
database from any Python iterable (a list of tuple, or any iterable of
sequences): the Python values are adapted as they would be in normal querying.
To perform such operation use a :sql:`COPY [table] FROM STDIN` with
`Cursor.copy()` and use `~Copy.write_row()` on the resulting object in a
``with`` block. On exiting the block the operation will be concluded:

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
        with cursor.copy("COPY table_name TO STDOUT") as copy:
            for data in copy:
                f.write(data)

Asynchronous operations are supported using the same patterns on an
`AsyncConnection`. For instance, if `!f` is an object supporting an
asynchronous `!read()` method returning :sql:`COPY` data, a fully-async copy
operation could be:

.. code:: python

    async with cursor.copy("COPY data FROM STDIN") as copy:
        while data := await f.read()
            await copy.write(data)

Binary data can be produced and consumed using :sql:`FORMAT BINARY` in the
:sql:`COPY` command: see :ref:`binary-data` for details and limitations.


.. index:: asyncio

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
