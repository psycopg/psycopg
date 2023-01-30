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

  - What a database connection object represents and how to obtain one

  - What a database cursor object does and how to obtain one

  - What a result set is

  - What happens when there is a problem performing a requested
    operation

  - What adapters do

  - How transactions might be managed in your Psycopg applications

  - Why context managers help with transaction management

- Know something of the:

  - Purpose behind letting Psycopg manage the run time insertion
    of data values into SQL

  - Methods which might be used to execute an SQL statement or
    statements

  - Facilities which manage runtime insertion of data values into
    SQL

  - Default transaction management behaviors

  - Possibilities for alternate transaction management patterns

  - Expectations for freeing unused resources

  - Options for freeing unused resources

- Be aware:

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

Concepts and Features
---------------------

.. index::
    single: connection; string
    single: connection; object
    single: cursor

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
    single: query parameters
    single: SQL; construction
    single: SQL; dynamic
    pair: SQL; substituting data values
    pair: SQL; escaping
    pair: SQL; quoting

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
result sets, but the usual approach is to retrieve rows by iterating
on the cursor.
This can be seen in the :ref:`usage <usage>` example below.
After retrieving all of a result set's rows, calling the
`~Cursor.nextset()` cursor method switches to the next result set.
After a `~Cursor.nextset()` call the previous result set is unavailable.

.. __: https://www.postgresql.org/docs/current/
       glossary.html#GLOSSARY-RESULT-SET

.. index::
    pair: SQL; result status
    pair: execution; result status
    pair: execution; SQL result status

Once all rows in a result set are retrieved from the Postgres server
(which some kinds of cursors do automatically upon SQL execution)
cursor attributes are available to obtain information on the status of
the SQL statement just executed; such as `~Cursor.rowcount`, which
contains the number of database rows the statement affected.

Should an :ref:`error <dbapi-exceptions>` occur, at any time, `an
exception`__ is raised.

.. __: https://docs.python.org/3/tutorial/errors.html#exceptions

.. index::
    single: Adaptation
    single: Data types; Adaptation

:ref:`Adapters <types-adaptation>` are responsible for converting
between PostgreSQL data types and Python data types, and between the
data types used in the various communication protocols and related
data structures.
Adapters may be customized.

.. index::
    single: Transaction management
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
As explained in the :ref:`example below <usage>`, automatic commit
upon connection close can be obtained by using a connection object as
a context manager.

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
