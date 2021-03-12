.. currentmodule:: psycopg3

.. index::
    pair: COPY; SQL command

.. _copy:

Using COPY TO and COPY FROM
===========================

`psycopg3` allows to operate with `PostgreSQL COPY protocol`__. :sql:`COPY` is
one of the most efficient ways to load data into the database (and to modify
it, with some SQL creativity).

.. __: https://www.postgresql.org/docs/current/sql-copy.html

Copy is supported using the `Cursor.copy()` method, passing it a query of the
form :sql:`COPY ... FROM STDIN` or :sql:`COPY ... TO STDOUT`, and managing the
resulting `Copy` object in a ``with`` block:

.. code:: python

    with cursor.copy("COPY table_name (col1, col2) FROM STDIN") as copy:
        # pass data to the 'copy' object using write()/write_row()

You can compose a COPY statement dynamically by using objects from the
`psycopg3.sql` module:

.. code:: python

    with cursor.copy(
        sql.SQL("COPY {} TO STDOUT").format(sql.Identifier("table_name"))
    ) as copy:
        # read data from the 'copy' object using read()/read_row()

The connection is subject to the usual transaction behaviour, so, unless the
connection is in autocommit, at the end of the COPY operation you will still
have to commit the pending changes and you can still roll them back. See
:ref:`transactions` for details.


Writing data row-by-row
-----------------------

Using a copy operation you can load data into the database from any Python
iterable (a list of tuple, or any iterable of sequences): the Python values
are adapted as they would be in normal querying. To perform such operation use
a :sql:`COPY ... FROM STDIN` with `Cursor.copy()` and use `~Copy.write_row()`
on the resulting object in a ``with`` block. On exiting the block the
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

Binary copy is supported by specifying :sql:`FORMAT BINARY` in the :sql:`COPY`
statement. In order to load binary data, all the types passed to the database
must have a binary dumper registered (see see :ref:`binary-data`).

Note that PostgreSQL is particularly finicky when loading data in binary mode
and will apply *no cast rule*. This means that e.g. passing a Python `!int`
object to an :sql:`integer` column (aka :sql:`int4`) will likely fail, because
the default `!int` `~adapt.Dumper` will use the :sql:`bigint` aka :sql:`int8`
format. You can work around the problem by registering the right binary dumper
on the cursor or using the right data wrapper (see :ref:`adaptation`).


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
you have to specify them yourselves.

.. code:: python

    with cur.copy("COPY (VALUES (10::int, current_date)) TO STDOUT") as copy:
        copy.set_types(["int4", "date"])
        for row in copy.rows():
            print(row)  # (10, datetime.date(2046, 12, 24))


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
input data is compatible. Data can be passed as `!str`, if the copy is in
:sql:`FORMAT TEXT`, or as `!bytes`, which works with both :sql:`FORMAT TEXT`
and :sql:`FORMAT BINARY`.

In order to produce data in :sql:`COPY` format you can use a :sql:`COPY ... TO
STDOUT` statement and iterate over the resulting `Copy` object, which will
produce a stream of `!bytes`:

.. code:: python

    with open("data.out", "wb") as f:
        with cursor.copy("COPY table_name TO STDOUT") as copy:
            for data in copy:
                f.write(data)


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

The `AsyncCopy` object documentation describe the signature of the
asynchronous methods and the differences from its sync `Copy` counterpart.
