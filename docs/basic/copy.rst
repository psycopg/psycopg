.. currentmodule:: psycopg

.. index::
    pair: COPY; SQL command

.. _copy:

Using COPY TO and COPY FROM
===========================

Psycopg allows to operate with `PostgreSQL COPY protocol`__. :sql:`COPY` is
one of the most efficient ways to load data into the database (and to modify
it, with some SQL creativity).

.. __: https://www.postgresql.org/docs/current/sql-copy.html

Copy is supported using the `Cursor.copy()` method, passing it a query of the
form :sql:`COPY ... FROM STDIN` or :sql:`COPY ... TO STDOUT`, and managing the
resulting `Copy` object in a `!with` block:

.. code:: python

    with cursor.copy("COPY table_name (col1, col2) FROM STDIN") as copy:
        # pass data to the 'copy' object using write()/write_row()

You can compose a COPY statement dynamically by using objects from the
`psycopg.sql` module:

.. code:: python

    with cursor.copy(
        sql.SQL("COPY {} TO STDOUT").format(sql.Identifier("table_name"))
    ) as copy:
        # read data from the 'copy' object using read()/read_row()

.. versionchanged:: 3.1

    You can also pass parameters to `!copy()`, like in `~Cursor.execute()`:

    .. code:: python

        with cur.copy("COPY (SELECT * FROM table_name LIMIT %s) TO STDOUT", (3,)) as copy:
            # expect no more than three records

The connection is subject to the usual transaction behaviour, so, unless the
connection is in autocommit, at the end of the COPY operation you will still
have to commit the pending changes and you can still roll them back. See
:ref:`transactions` for details.


.. _copy-in-row:

Writing data row-by-row
-----------------------

Using a copy operation you can load data into the database from any Python
iterable (a list of tuples, or any iterable of sequences): the Python values
are adapted as they would be in normal querying. To perform such operation use
a :sql:`COPY ... FROM STDIN` with `Cursor.copy()` and use `~Copy.write_row()`
on the resulting object in a `!with` block. On exiting the block the
operation will be concluded:

.. code:: python

    records = [(10, 20, "hello"), (40, None, "world")]

    with cursor.copy("COPY sample (col1, col2, col3) FROM STDIN") as copy:
        for record in records:
            copy.write_row(record)

If an exception is raised inside the block, the operation is interrupted and
the records inserted so far are discarded.

In order to read or write from `!Copy` row-by-row you must not specify
:sql:`COPY` options such as :sql:`FORMAT CSV`, :sql:`DELIMITER`, :sql:`NULL`:
please leave these details alone, thank you :)


.. _copy-out-row:

Reading data row-by-row
-----------------------

You can also do the opposite, reading rows out of a :sql:`COPY ... TO STDOUT`
operation, by iterating on `~Copy.rows()`. However this is not something you
may want to do normally: usually the normal query process will be easier to
use.

PostgreSQL, currently, doesn't give complete type information on :sql:`COPY
TO`, so the rows returned will have unparsed data, as strings or bytes,
according to the format.

.. code:: python

    with cur.copy("COPY (VALUES (10::int, current_date)) TO STDOUT") as copy:
        for row in copy.rows():
            print(row)  # return unparsed data: ('10', '2046-12-24')

You can improve the results by using `~Copy.set_types()` before reading, but
you have to specify them yourself.

.. code:: python

    with cur.copy("COPY (VALUES (10::int, current_date)) TO STDOUT") as copy:
        copy.set_types(["int4", "date"])
        for row in copy.rows():
            print(row)  # (10, datetime.date(2046, 12, 24))


.. _copy-block:

Copying block-by-block
----------------------

If data is already formatted in a way suitable for copy (for instance because
it is coming from a file resulting from a previous `COPY TO` operation) it can
be loaded into the database using `Copy.write()` instead.

.. code:: python

    with open("data", "r") as f:
        with cursor.copy("COPY data FROM STDIN") as copy:
            while data := f.read(BLOCK_SIZE):
                copy.write(data)

In this case you can use any :sql:`COPY` option and format, as long as the
input data is compatible with what the operation in `!copy()` expects. Data
can be passed as `!str`, if the copy is in :sql:`FORMAT TEXT`, or as `!bytes`,
which works with both :sql:`FORMAT TEXT` and :sql:`FORMAT BINARY`.

In order to produce data in :sql:`COPY` format you can use a :sql:`COPY ... TO
STDOUT` statement and iterate over the resulting `Copy` object, which will
produce a stream of `!bytes` objects:

.. code:: python

    with open("data.out", "wb") as f:
        with cursor.copy("COPY table_name TO STDOUT") as copy:
            for data in copy:
                f.write(data)


.. _copy-binary:

Binary copy
-----------

Binary copy is supported by specifying :sql:`FORMAT BINARY` in the :sql:`COPY`
statement. In order to import binary data using `~Copy.write_row()`, all the
types passed to the database must have a binary dumper registered; this is not
necessary if the data is copied :ref:`block-by-block <copy-block>` using
`~Copy.write()`.

.. warning::

    PostgreSQL is particularly finicky when loading data in binary mode and
    will apply **no cast rules**. This means, for example, that passing the
    value 100 to an `integer` column **will fail**, because Psycopg will pass
    it as a `smallint` value, and the server will reject it because its size
    doesn't match what expected.

    You can work around the problem using the `~Copy.set_types()` method of
    the `!Copy` object and specifying carefully the types to load.

.. seealso:: See :ref:`binary-data` for further info about binary querying.


.. _copy-async:

Asynchronous copy support
-------------------------

Asynchronous operations are supported using the same patterns as above, using
the objects obtained by an `AsyncConnection`. For instance, if `!f` is an
object supporting an asynchronous `!read()` method returning :sql:`COPY` data,
a fully-async copy operation could be:

.. code:: python

    async with cursor.copy("COPY data FROM STDIN") as copy:
        while data := await f.read():
            await copy.write(data)

The `AsyncCopy` object documentation describes the signature of the
asynchronous methods and the differences from its sync `Copy` counterpart.

.. seealso:: See :ref:`async` for further info about using async objects.


Example: copying a table across servers
---------------------------------------

In order to copy a table, or a portion of a table, across servers, you can use
two COPY operations on two different connections, reading from the first and
writing to the second.

.. code:: python

    with psycopg.connect(dsn_src) as conn1, psycopg.connect(dsn_tgt) as conn2:
        with conn1.cursor().copy("COPY src TO STDOUT (FORMAT BINARY)") as copy1:
            with conn2.cursor().copy("COPY tgt FROM STDIN (FORMAT BINARY)") as copy2:
                for data in copy1:
                    copy2.write(data)

Using :sql:`FORMAT BINARY` usually gives a performance boost, but it only
works if the source and target schema are *perfectly identical*. If the tables
are only *compatible* (for example, if you are copying an :sql:`integer` field
into a :sql:`bigint` destination field) you should omit the `BINARY` option and
perform a text-based copy. See :ref:`copy-binary` for details.

The same pattern can be adapted to use :ref:`async objects <async>` in order
to perform an :ref:`async copy <copy-async>`.
