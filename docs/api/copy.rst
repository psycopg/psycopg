.. currentmodule:: psycopg

COPY-related objects
====================

The main objects (`Copy`, `AsyncCopy`) present the main interface to exchange
data during a COPY operations. These objects are normally obtained by the
methods `Cursor.copy()` and `AsyncCursor.copy()`; however, they can be also
created directly, for instance to write to a destination which is not a
database (e.g. using a `~psycopg.copy.FileWriter`).

See :ref:`copy` for details.


Main Copy objects
-----------------

.. autoclass:: Copy()

    The object is normally returned by `!with` `Cursor.copy()`.

    .. automethod:: write_row

        The data in the tuple will be converted as configured on the cursor;
        see :ref:`adaptation` for details.

    .. automethod:: write
    .. automethod:: read

        Instead of using `!read()` you can iterate on the `!Copy` object to
        read its data row by row, using ``for row in copy: ...``.

    .. automethod:: rows

        Equivalent of iterating on `read_row()` until it returns `!None`

    .. automethod:: read_row
    .. automethod:: set_types


.. autoclass:: AsyncCopy()

    The object is normally returned by ``async with`` `AsyncCursor.copy()`.
    Its methods are similar to the ones of the `Copy` object but offering an
    `asyncio` interface (`await`, `async for`, `async with`).

    .. automethod:: write_row
    .. automethod:: write
    .. automethod:: read

        Instead of using `!read()` you can iterate on the `!AsyncCopy` object
        to read its data row by row, using ``async for row in copy: ...``.

    .. automethod:: rows

        Use it as `async for record in copy.rows():` ...

    .. automethod:: read_row


.. _copy-writers:

Writer objects
--------------

.. currentmodule:: psycopg.copy

.. versionadded:: 3.1

Copy writers are helper objects to specify where to write COPY-formatted data.
By default, data is written to the database (using the `LibpqWriter`). It is
possible to write copy-data for offline use by using a `FileWriter`, or to
customize further writing by implementing your own `Writer` or `AsyncWriter`
subclass.

Writers instances can be used passing them to the cursor
`~psycopg.Cursor.copy()` method or to the `~psycopg.Copy` constructor, as the
`!writer` argument.

.. autoclass:: Writer

    This is an abstract base class: subclasses are required to implement their
    `write()` method.

    .. automethod:: write
    .. automethod:: finish


.. autoclass:: LibpqWriter

    This is the writer used by default if none is specified.


.. autoclass:: FileWriter

    This writer should be used without executing a :sql:`COPY` operation on
    the database. For example, if `records` is a list of tuples containing
    data to save in COPY format to a file (e.g. for later import), it can be
    used as:

    .. code:: python

        with open("target-file.pgcopy", "wb") as f:
            with Copy(cur, writer=FileWriter(f)) as copy:
                for record in records
                    copy.write_row(record)


.. autoclass:: AsyncWriter

    This class methods have the same semantics of the ones of `Writer`, but
    offer an async interface.

    .. automethod:: write
    .. automethod:: finish

.. autoclass:: AsyncLibpqWriter
