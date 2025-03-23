.. currentmodule:: psycopg

.. _module-concepts:

.. index::
    single: concepts
    single: feature overview

Concepts and Features
=====================

This section provides a high-level overview of the concepts which need
to be understood when using Psycopg, and summarizes Psycopg's major
features.

The reader may not be interested in every concept or feature mentioned.
Readers should feel free to skip or skim over the parts of this section
which are not relevant or not suited to the problem at-hand.

Someone who has read through all of this section should:

- Have a minimal understanding of:

  - The purpose of connection strings

  - What a database connection object represents

  - What a database cursor object does

  - What a result set is

  - What happens when there is a problem performing a requested
    operation

  - What row factories do

  - What adapters do

  - How transactions might be managed in your Psycopg applications

  - Why context managers help with transaction management

- Know something of:

  - How to obtain and use a database connection object

  - How to obtain and use a cursor object

  - The purpose behind using Psycopg to construct SQL statements

  - The purpose behind letting Psycopg manage the run time insertion
    of data values into SQL

  - The methods which might be used to execute an SQL statement or
    statements

  - The facilities which manage runtime insertion of data values into
    SQL

  - How to obtain the rows produced by a SQL statement

  - The default transaction management behaviors

  - The possibilities for alternate transaction management patterns

  - Expectations for freeing unused resources

  - Options for freeing unused resources

- Be aware:

  - That static type checking is supported

  - Of the existence of a standardized Python API for database
    interaction

  - Of some advanced features which improve performance

  - Of some of the methods used to obtain database rows from result
    sets

  - Of the method used to discard the product of the SQL statement
    just executed and prepare the server for the processing of another
    -- when the server has already been given the SQL it is to execute

  - That it is possible to get information from the server about the
    effects of the SQL statement just executed

.. index::
   pair: Python; Database API Specification
   pair: Python; DB-API v2.0

The Python Database API Specification v2.0
------------------------------------------

Psycopg is an implementation of Python's `DB-API`_ v2.0 standard.
So using Psycopg's basic features is no different from using any other
database adapter which implements the DB-API standard.

The pattern of interaction is roughly the same as when using other
database adapters, like the Python-builtin `sqlite3`, or the
`psycopg2` adapter.
We say "roughly" because it seems there invariably is some implementation
quirk that is taken advantage of, or some special feature used, which
is available only in a particular database adapter.

Psycopg has a number of advanced features which go beyond the DB-API
standard.
These can lead to: improvements in code clarity; improved efficiency;
support for alternate software design patterns, patterns better suited
to your problem domain; etc.

.. _DB-API: https://www.python.org/dev/peps/pep-0249/

.. _concepts:

A Summary of DB-API Concepts and Psycopg Features
-------------------------------------------------

This sub-section provides an overview of Psycopg's features.
It describes how these features interact with each other, utilizing
the interface concepts defined in the Python Database API standard.

The organization of this sub-section follows the control flow of a
typical Psycopg-based application.
Concepts, the objects which represent them, and Psycopg features are
introduced in the order in which they typically appear during program
execution.

After sufficient explanation of the concepts and features already
introduced, at points where program architecture decisions must be
made, previously unmentioned concepts and features, those that could
make sense to use, are introduced.
So let's get started!

Psycopg is :ref:`type annotated <static-typing>`.
If your application is also, static type checking tools can report any
invalid usage of Psycopg contained in your code.

.. index::
    single: connection; string
    single: connection; object

The parameters needed to interact with `Postgres`__ are
:ref:`assembled <psycopg.conninfo>` into a `connection string`__\.
This value is given to a connection method, typically the `~psycopg`
module's `~psycopg.connect()` method, the `database server`__ is
contacted, and a database `Connection` object is returned.
Each connection object represents an open communication channel to a
Postgres database.
`Database connections`__ have state.
They are sometimes called database sessions.

.. __: https://www.postgresql.org
.. __: https://www.postgresql.org/docs/current/
       libpq-connect.html#LIBPQ-CONNSTRING
.. __: https://www.postgresql.org/docs/current/tutorial-arch.html
.. __: https://www.postgresql.org/docs/current/
       glossary.html#GLOSSARY-CONNECTION

Alternately, connections may be obtained from a :ref:`pool
<connection-pools>` of pre-established connections, to mitigate
connection startup delay.

.. index::
    single: cursor
   
A connection's `~Connection.cursor()` method is used to obtain (one,
often, or more) `~Cursor` objects.
Cursors are used to interact with the connected database.
Cursors contain state.
A cursor represents the state of a current database interaction, if
the cursor is being used and a requested operation has not finished.
Otherwise, a cursor represents the state of the last database
interaction for which it was used.

.. index::
    single: isolation

Cursors created from any one given connection are not `isolated`__\.
Any changes done to the database by the given connection's cursor are
immediately visible to all other cursors created from the same
connection.

.. __: https://www.postgresql.org/docs/current/
       glossary.html#GLOSSARY-ISOLATION

.. index::
    single: transaction isolation

Cursors created from different connections may or may not be isolated.
Whether they are depends on the `transaction isolation level`__ of
their associated connection.

.. __: https://www.postgresql.org/docs/current/transaction-iso.html

.. index::
    single: SQL
    single: query parameters
    single: SQL; construction
    single: SQL; dynamic
    pair: SQL; substituting data values
    pair: SQL; escaping
    pair: SQL; quoting

The `~psycopg.sql` module may be used to construct `~psycopg.sql.SQL`
objects.
SQL objects represent the `SQL`__ statements they contain.
Data values can be, in effect -- although this is not the actual
implementation, substituted into an SQL object's SQL text at run time.
When properly constructed SQL objects are impervious to `SQL
injection`__ attacks.
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
It may optionally be given data values to (safely), in effect,
incorporate into the text of the SQL before execution.
For network efficiency, or for other reasons, the SQL supplied to
`~Cursor.execute()` may consist of more than one SQL statement.
In a similar vein, `~Cursor.executemany()` may be used to efficiently
re-execute the same SQL, incorporating different data values into the
SQL on each execution.

Psycopg has other features which improve performance.
Among these are:
:ref:`Prepared statements <prepared-statements>`, which reduce server parsing
and planning load;
:ref:`Pipeline mode <pipeline-mode>`, which mitigates problems with network
latency;
:ref:`Asynchronous support <async>`, which minimizes total elapsed
time by allowing all processes, whether on client or server, to run
concurrently;
and :ref:`COPY <copy>` methods, for efficient bulk data transfer in
and out of the database.

.. index::
    single: result set
    single: SQL; returning rows
    single: returning rows
    pair: cursor; returning rows

SQL statements which produce results do so in `result sets`__\.
Psycopg provides `~Cursor` methods which return one or more rows from
result sets, but the usual approach is to retrieve rows by iterating.
Cursors are `iterable`__\s that produce `iterators`__ which return
database rows.
This can be seen in the :ref:`usage example <usage>`, below, where a
`!for` statement is used to iterate over the rows returned by a
database query.

.. __: https://www.postgresql.org/docs/current/
       glossary.html#GLOSSARY-RESULT-SET
.. __: https://docs.python.org/3/glossary.html#term-iterable
.. __: https://docs.python.org/3/glossary.html#term-iterator

After retrieving all of a result set's rows, calling the
`~Cursor.nextset()` cursor method switches to the next result set.
The rows returned by the next SQL statement can then be obtained.
After a `~Cursor.nextset()` call the previous result set is unavailable.

Typically, SQL is supplied to the server one statement at a time.
`~Cursor.nextset()` is not usually used, unless a single statement is
repeatedly executed and different data values substituted into the
SQL upon each execution.
So result set management is not often front-of-mind.

.. index::
    single: SQL result status
    pair: SQL; result status
    pair: execution; result status

Once all rows in a result set are retrieved from the Postgres server
(which some kinds of cursors do automatically upon SQL execution)
cursor attributes are available to obtain information on the status of
the SQL statement just executed.
E.g. `~Cursor.rowcount`, which contains the number of database rows
the statement affected.

Should an :ref:`error <dbapi-exceptions>` occur, at any time, `an
exception`__ is raised.

.. __: https://docs.python.org/3/tutorial/errors.html#exceptions

.. index::
    single: row factories
    single: database row data representation
    pair: data type; row

:ref:`Row factories <row-factories>` determine the Python data type
cursors return when producing row of database content.
The default is to represent a row as a `tuple`__\.  
Several other data types are available.
You can also write your own row factory.

.. __: https://docs.python.org/3/tutorial/datastructures.html#tuples-and-sequences

.. index::
    single: adaptation
    single: data type; adaptation

:ref:`Adapters <types-adaptation>` are responsible for converting
between PostgreSQL's data types and Python data types, and between the
data types used in the various communication protocols and related
data structures.
Adapters may be customized.
You can also write your own.

.. index::
    single: transaction management
    single: database changes are discarded
    single: failure to change database content
    single: updates fail
    single: writes fail

`Transactions`__ are, by default, :ref:`managed by Psycopg
<transactions>`\.
They are a property of database connections; a connection is either in
a transaction or it is not.
The Python `DB-API`_ demands particular default behaviors.
By default, any change made to database content begins a transaction.
Absent transaction management on the part of your code, further
content changes become part of the same transaction.
Again, by default, closing the connection (or exiting your Python
program without closing) does not commit an ongoing transaction;
database content changes are lost unless the `~Connection.commit()`
method is explicitly called.
As explained :ref:`below <with-connection>`, using a connection object
as a context manager does make closing a connection trigger an
automatic commit.

.. __: https://www.postgresql.org/docs/current/tutorial-transactions.html

.. index::
    single: autocommit

For more intuitive transaction handling, some experienced developers
prefer using :ref:`a particular <common-transaction-idiom>` software
design pattern employing the Psycopg :ref:`autocommit <autocommit>`
feature.
A variety of transaction management design patterns are possible.

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
the cursor or connection object as the context manager of a Python
`!with` statement.

Closing connections and cursors frees the resources associated with
them, and they become unusable after closing.
Cursors also become unusable when their associated database connection
is closed.

There are various kinds of connection objects, cursor objects, SQL
representations, and so forth, to be used as needed.
