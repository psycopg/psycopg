.. currentmodule:: psycopg

.. _module-usage:

Basic module usage
==================

The basic Psycopg usage is common to all the database adapters
implementing the `DB-API`_ protocol.
Other database adapters, such as the builtin `sqlite3` or `psycopg2`,
have roughly the same pattern of interaction.

.. _DB-API: https://www.python.org/dev/peps/pep-0249/

.. index:: concepts

.. _concepts:

Concepts and Vocabulary
-----------------------

.. index::
    single: connection string
            connection object
            cursor

The parameters needed to interact with `Postgres`__ are
:ref:`assembled <psycopg.conninfo>` into a `connection string`__\.
This value is given to a connection method, typically the `~psycopg`
module's `~psycopg.connect()` method, the `database server`__ is
contacted, and a database `Connection` object is returned.
Each connection object represents a communication channel to a
Postgres database.
Alternately, connections may be obtained from a :ref:`pool
<connection-pools>` of pre-established connections, to mitigate
connection startup delay.
A connection's `~Connection.cursor()` method is used to obtain (one,
often, or more) `~Cursor` objects, which are then used to interact
with the connected database.

.. __: https://www.postgresql.org
.. __: https://www.postgresql.org/docs/current/
       libpq-connect.html#LIBPQ-CONNSTRING
.. __: https://www.postgresql.org/docs/current/tutorial-arch.html

.. index::
    single: SQL
            query parameters
    pair: SQL; substituting data values
          SQL; construction
          SQL; escaping
          SQL; quoting
          SQL; dynamic

The `~psycopg.sql` module may be used to construct `~psycopg.sql.SQL`
objects, which represent `SQL`__ statements into which data values can
be substituted at run time.
When properly constructed these are impervious to `SQL injection`__
attacks.
:ref:`Other techniques <query-parameters>`, which involve :ref:`less
code <usage>`, are also available to safely put dynamic data into SQL.
Relying on Psycopg, using `~psycopg.sql` in particular, for safe SQL
construction means that the application need not concern itself with
either quoting and escaping data values and `identifiers`__, or
handling similar subtle aspects of SQL syntax and parsing.

.. __: https://en.wikibooks.org/wiki/SQL
.. __: https://en.wikipedia.org/wiki/Sql_injection
.. __: https://www.postgresql.org/docs/current/
       sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

.. index::
    single: efficiency
    single: performance
    pair: SQL; execution

The `~Cursor.execute()` cursor method takes SQL and sends it to the
Postgres server for execution.
It may optionally be given data values to be (safely) incorporated
into the text of the SQL upon execution.
For network efficiency, or for other reasons, the SQL supplied to
`~Cursor.execute()` may consist of more than one SQL statement.
In a similar vein, `~Cursor.executemany()` may be used to efficiently
re-execute the same SQL, incorporating different data values into the
SQL on each execution.

Psycopg supports other features which improve performance.
Among these are:
:ref:`Prepared statements <prepared-statements>`, which reduce server parsing
and planning load;
:ref:`Pipeline mode <pipeline-mode>`, which mitigates problems with network
latency;
and :ref:`COPY <copy>` methods, for efficient bulk data transfer in
and out of the database.

.. index::
    single: result set
    pair: cursor; returning rows
          SQL; returning rows

SQL statements which produce results do so in `result sets`__\.
Psycopg provides `~Cursor` methods to retrieve one or more rows from
result sets but the usual approach is to retrieve rows by iterating on
the cursor.
This can be seen in the :ref:`usage <usage>` example below.
After retrieving all of a result set's rows, calling the
`~Cursor.nextset()` cursor method switches to the next result set.

.. __: https://www.postgresql.org/docs/current/
       glossary.html#GLOSSARY-RESULT-SET

.. index::
    pair:: SQL; result status
           execution; result status

Once all rows in a result set are retrieved from the Postgres server
(which some kinds of cursors do automatically upon SQL execution)
cursor attributes are available to obtain information on the status of
the SQL statement just executed; such as `~Cursor.rowcount`, which
contains the number of database rows the statement affected.

.. index::
    single: Adaptation
            Data types; Adaptation

Should an :ref:`error <dbapi-exceptions>` occur, at any time, `an
exception`__ is raised.

.. __: https://docs.python.org/3/tutorial/errors.html#exceptions

:ref:`Adapters <types-adaptation>` are responsible for converting
between PostgreSQL data types and Python data types, and between the
data types used in the various communication protocols and related
data structures.
Adapters may be customized.

.. index::
    single: Transactions management
    single: database changes are discarded
    single: failure to change database content
    single: updates fail
    single: writes fail

`Transactions`__ are, by default, :ref:`managed by Psycopg
<transactions>`\.
They are a property of database connections; a connection is either in
a transaction or is not.
The Python `DB-API`_ demands particular default behaviors.
By default, any change made to database content begins a transaction.
Absent transaction management on the part of your code, further
content changes become part of the same transaction.
Again, by default, closing the connection (or exiting your Python
program without closing) does not commit an ongoing transaction;
database content changes are lost unless the `~Connection.commit()`
method is explicitly called.
As shown in the :ref:`example below <usage>`, automatic commit upon
connection close can be obtained by using a connection object as a
context manager.

.. __: https://www.postgresql.org/docs/current/tutorial-transactions.html

.. index::
    single: autocommit

To obtain a more intuitive transaction handling, some experienced
developers prefer using :ref:`a particular <common-transaction-idiom>`
software design pattern employing the Psycopg :ref:`autocommit
<autocommit>` feature.
A variety of transaction design patterns are possible.

.. index::
   pair: context manager; cursor
         context manager; connection
         cursor; closing
         connection; closing

When the server has finished executing all the SQL statements sent to
it and there are no more result sets available to a cursor, the cursor
may be re-used.
When a cursor, or a connection, is no longer needed it should be
closed.
This is usually accomplished, :ref:`as shown <usage>` below, by using
the cursor or connection object as the context manager in a Python
`!with` statement.

There are various kinds of connection objects, cursor objects, SQL
representations, and so forth, to be used as needed.

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
            # of several records, or even iterate on the cursor.
            for record in cur:
                print(record)
            # As is usual when an iterable is used in an iteration context,
            # Python creates a new iterator from the iterable.  In this
            # case, from `cur`, the cursor object.  So the row inserted
            # above is printed.

            # Make the changes to the database persistent.
            # Assuming this listing shows the entirety of this program,
            # this statement is unnecessary.  The transaction is
            # automatically commmited when the connection's `with`
            # statement completes.  That said, it does not hurt to commit.
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
