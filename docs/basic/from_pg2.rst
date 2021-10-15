.. index::
    pair: psycopg2; Differences

.. currentmodule:: psycopg


Differences from ``psycopg2``
=============================

Psycopg 3 uses the common DBAPI structure of many other database adapter and
tries to behave as close as possible to `!psycopg2`. There are however a few
differences to be aware of.

.. note::
    Most of the times, the workarounds suggested here will work with both
    Psycopg 2 and 3, which could be useful if you are porting a program or
    writing a program that should work with both Psycopg 2 and 3.


.. _server-side-binding:

Server-side binding
-------------------

Psycopg 3 sends the query and the parameters to the server separately, instead
of merging them on the client side. Server-side binding works for normal
:sql:`SELECT` and data manipulation statements (:sql:`INSERT`, :sql:`UPDATE`,
:sql:`DELETE`), but it doesn't work with many other statements. For instance,
it doesn't work with :sql:`SET` or with :sql:`NOTIFY`::

    >>> conn.execute("SET TimeZone TO %s", ["UTC"])
    Traceback (most recent call last):
    ...
    psycopg.errors.SyntaxError: syntax error at or near "$1"
    LINE 1: SET TimeZone TO $1
                            ^

    >>> conn.execute("NOTIFY %s, %s", ["chan", 42])
    Traceback (most recent call last):
    ...
    psycopg.errors.SyntaxError: syntax error at or near "$1"
    LINE 1: NOTIFY $1, $2
                   ^

and with any data definition statement::

    >>> conn.execute("CREATE TABLE foo (id int DEFAULT %s)", [42])
    Traceback (most recent call last):
    ...
    psycopg.errors.UndefinedParameter: there is no parameter $1
    LINE 1: CREATE TABLE foo (id int DEFAULT $1)
                                             ^

Sometimes, PostgreSQL offers an alternative: for instance the `set_config()`__
function can be used instead of the :sql:`SET` statement, the `pg_notify()`__
function can be used instead of :sql:`NOTIFY`::

    >>> conn.execute("SELECT set_config('TimeZone', %s, false)", ["UTC"])

    >>> conn.execute("SELECT pg_notify(%s, %s)", ["chan", "42"])

.. __: https://www.postgresql.org/docs/current/functions-admin.html
    #FUNCTIONS-ADMIN-SET

.. __: https://www.postgresql.org/docs/current/sql-notify.html
    #id-1.9.3.157.7.5

If this is not possible, you can use client-side binding using the objects
from the `sql` module::

    >>> from psycopg import sql
    >>> conn.execute(sql.SQL("CREATE TABLE foo (id int DEFAULT {})").format(42))


.. _multi-statements:

Multiple statements in the same query
-------------------------------------

As a consequence of using :ref:`server-side bindings <server-side-binding>`,
when parameters are used, it is not possible to execute several statements in
the same `!execute()` call, separating them with a semicolon::

    >>> conn.execute(
    ...     "insert into foo values (%s); insert into foo values (%s)",
    ...     (10, 20))
    Traceback (most recent call last):
    ...
    psycopg.errors.SyntaxError: cannot insert multiple commands into a prepared statement

One obvious way to work around the problem is to use several `!execute()`
calls.

There is no such limitation if no parameters are used. This allows one to generate
batches of statements entirely on the client side (for instance using the
`psycopg.sql` objects) and to run them in the same `!execute()` call::

    >>> from psycopg import sql
    >>> query = sql.SQL(
    ...     "insert into foo values ({}); insert into foo values ({})"
    ... ).format(10, 20))
    >>> conn.execute(query)

Note that statements that must be run outside a transaction (such as
:sql:`CREATE DATABASE`) can never be executed in batch with other statements,
even if the connection is in autocommit mode::

    >>> conn.autocommit = True
    >>> conn.execute("create database foo; select 1")
    Traceback (most recent call last):
    ...
    psycopg.errors.ActiveSqlTransaction: CREATE DATABASE cannot run inside a transaction block

This happens because PostgreSQL will wrap multiple statements in a transaction
itself and is different from how :program:`psql` behaves (:program:`psql` will
split the queries on semicolons and send them separately). This is not new in
Psycopg 3: the same limitation is present in `!psycopg2` too.


.. _difference-cast-rules:

Different cast rules
--------------------

In rare cases, especially around variadic functions, PostgreSQL might fail to
find a function candidate for the given data types::

    >>> conn.execute("SELECT json_build_array(%s, %s)", ["foo", "bar"])
    Traceback (most recent call last):
    ...
    psycopg.errors.IndeterminateDatatype: could not determine data type of parameter $1

This can be worked around specifying the argument types explicitly via a cast::

    >>> conn.execute("SELECT json_build_array(%s::text, %s::text)", ["foo", "bar"])


.. _in-and-tuple:

You cannot use ``IN %s`` with a tuple
-------------------------------------

``IN`` cannot be used with a tuple as single parameter, as was possible with
``psycopg2``::

    >>> conn.execute("SELECT * FROM foo WHERE id IN %s", [(10,20,30)])
    Traceback (most recent call last):
    ...
    psycopg.errors.SyntaxError: syntax error at or near "$1"
    LINE 1: SELECT * FROM foo WHERE id IN $1
                                          ^

What you can do is to use the `= ANY()`__ construct and pass the candidate
values as a list instead of a tuple, which will be adapted to a PostgreSQL
array::

    >>> conn.execute("SELECT * FROM foo WHERE id = ANY(%s)", [[10,20,30]])

Note that this variant is also superior because, unlike :sql:`IN`, it works
with an empty list of values.

.. __: https://www.postgresql.org/docs/current/functions-comparisons.html
    #id-1.5.8.30.16


.. _diff-adapt:

Different adaptation system
---------------------------

The adaptation system has been completely rewritten, in order to address
server-side parameters adaptation, but also to consider performance,
flexibility, ease of customization.

The default behaviour with builtin data should be :ref:`what you would expect
<types-adaptation>`. If you have customised the way to adapt data, or if you
are managing your own extension types, you should look at the :ref:`new
adaptation system <adaptation>`.

.. seealso::

    - :ref:`types-adaptation` for the basic behaviour.
    - :ref:`adaptation` for more advanced use.


.. _diff-copy:

Copy is no more file-based
--------------------------

`!psycopg2` exposes :ref:`a few copy methods <pg2:copy>` to interact with
PostgreSQL :sql:`COPY`. Their file-based interface doesn't make easy to load
dynamically-generated data into a database.

There is now a single `~Cursor.copy()` method, which is similar to
`!psycopg2` `!copy_expert()` in accepting a free-form :sql:`COPY` command and
returns an object to read/write data, block-wise or record-wise. The different
usage pattern also enables :sql:`COPY` to be used in async interactions.

.. seealso:: See :ref:`copy` for the details.


.. _diff-with:

``with`` connection
-------------------

In `!psycopg2`, using the syntax :ref:`with connection <pg2:with>`,
only the transaction is closed, not the connection. This behaviour is
surprising for people used to several other Python classes wrapping resources,
such as files.

In psycopg3, using :ref:`with connection <with-connection>` will close the
connection at the end of the `!with` block, making handling the connection
resources more familiar.

In order to manage transactions as blocks you can use the
`Connection.transaction()` method, which allows for finer control, for
instance to use nested transactions.

.. seealso:: See :ref:`transaction-context` for details.


.. _diff-callproc:

``callproc()`` is gone
----------------------

`cursor.callproc()` is not implemented. The method has a simplistic semantic
which doesn't account for PostgreSQL positional parameters, procedures,
set-returning functions... Use a normal `~Cursor.execute()` with :sql:`SELECT
function_name(...)` or :sql:`CALL procedure_name(...)` instead.


.. _diff-client-encoding:

``client_encoding`` is gone
---------------------------

Psycopg uses automatically the database client encoding to decode data to
Unicode strings. Use `ConnectionInfo.encoding` if you need to read the
encoding. You can select an encoding at connection time using the
``client_encoding`` connection parameter and you can change the encoding of a
connection by running a :sql:`SET client_encoding` statement... But why would
you?


What's new in Psycopg 3
-----------------------

- :ref:`Asynchronous support <async>`
- :ref:`Server-side parameters binding <server-side-binding>`
- :ref:`Prepared statements <prepared-statements>`
- :ref:`Binary communication <binary-data>`
- :ref:`Python-based COPY support <copy>`
- :ref:`Support for static typing <static-typing>`
- :ref:`A redesigned connection pool <connection-pools>`
- :ref:`Direct access to the libpq functionalities <psycopg.pq>`
