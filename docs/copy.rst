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
