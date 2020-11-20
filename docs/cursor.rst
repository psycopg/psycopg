Cursor classes
==============

.. currentmodule:: psycopg3

The `Cursor` and `AsyncCursor` classes are the main objects to send commands
to a PostgreSQL database session. They are normally created by the
connection's `~Connection.cursor()` method.

A `Connection` can create several cursors, but only one at time can perform
operations, so they are not the best way to achieve parallelism (you may want
to operate with several connections instead). All the cursors on the same
connection have a view of the same session, so they can see each other's
uncommitted data.


The `!Cursor` class
-------------------

.. autoclass:: Cursor()

    This class implements `DBAPI-compliant interface`__. It is what the
    classic `Connection.cursor()` method returns. `AsyncConnection.cursor()`
    will create instead `AsyncCursor` objects, which have the same set of
    method but expose an `asyncio` interface and require ``async`` and
    ``await`` keywords to operate.

    .. __: https://www.python.org/dev/peps/pep-0249/#cursor-objects

    Cursors behave as context managers: on block exit they are closed and
    further operation will not be possible. Closing a cursor will not
    terminate a transaction or a session though.

    .. automethod:: close

        .. note:: you can use :ref:`with conn.cursor(): ...<with-statement>`
            to close the cursor automatically when the block is exited.

    .. autoattribute:: closed
        :annotation: bool

    .. rubric:: Methods to send commands

    .. automethod:: execute

        Return the cursor itself, so that it will be possible to chain a fetch
        operation after the call

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. automethod:: executemany

        This is more efficient than performing separate queries, but in case of
        several :sql:`INSERT` (and with some SQL creativity for massive
        :sql:`UPDATE` too) you may consider using `copy()`.

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. automethod:: copy

        It must be called as ``with cur.copy() as copy: ...``

        See :ref:`copy` for information about :sql:`COPY`.

    .. automethod:: callproc

        This method exists for DBAPI compatibility but it's not much different
        than calling `execute()` on a :sql:`SELECT myproc(%s, %s, ...)`, which
        will give you more flexibility in passing arguments and retrieving
        results. Don't bother...

    .. rubric:: Methods to retrieve results

    Fetch methods are only available if the last operation produced results,
    e.g. a :sql:`SELECT` or a command with :sql:`RETURNING`. They will raise
    an exception if used with operations that don't return result, such as an
    :sql:`INSERT` with no :sql:`RETURNING` or an :sql:`ALTER TABLE`.

    .. note:: cursors are iterable objects, so just using ``for record in
        cursor`` syntax will iterate on the records in the current recordset.

    .. automethod:: fetchone
    .. automethod:: fetchmany
    .. automethod:: fetchall
    .. automethod:: nextset
    .. autoattribute:: pgresult

    .. rubric:: Information about the data

    .. autoproperty:: description
    .. autoproperty:: rowcount


The `!AsyncCursor` class
------------------------

.. autoclass:: AsyncCursor()

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines. Unless specified otherwise,
    non-blocking methods are shared with the `Cursor` class.

    The following methods have the same behaviour of the matching `!Cursor`
    methods, but should be called using the `await` keyword.

    .. automethod:: close

        .. note:: you can use ``async with`` to close the cursor
            automatically when the block is exited, but be careful about
            the async quirkness: see :ref:`with-statement` for details.

    .. automethod:: execute
    .. automethod:: executemany
    .. automethod:: copy

        It must be called as ``async with cur.copy() as copy: ...``

    .. automethod:: callproc
    .. automethod:: fetchone
    .. automethod:: fetchmany
    .. automethod:: fetchall

    .. note:: you can also use ``async for record in cursor`` to iterate on
        the async cursor results.


Cursor support objects
----------------------

.. autoclass:: Column()

    An object describing a column of data from a database result, `as described
    by the DBAPI`__, so it can also be unpacked as a 7-items tuple.

    The object is returned by `Cursor.description`.

    .. __: https://www.python.org/dev/peps/pep-0249/#description

    .. autoproperty:: name
    .. autoproperty:: type_code
    .. autoproperty:: display_size
    .. autoproperty:: internal_size
    .. autoproperty:: precision
    .. autoproperty:: scale


.. autoclass:: Copy()

    The object is normally returned by ``with`` `Cursor.copy()`.

    See :ref:`copy` for details.

    .. automethod:: read

        Instead of using `!read()` you can even iterate on the object to read
        its data row by row, using ``for row in copy: ...``.

    .. automethod:: write
    .. automethod:: write_row

        The data in the tuple will be converted as configured on the cursor;
        see :ref:`adaptation` for details.


.. autoclass:: AsyncCopy()

    The object is normally returned by ``async with`` `AsyncCursor.copy()`. Its methods are
    the same of the `Copy` object but offering an `asyncio` interface
    (`await`, `async for`, `async with`).

    .. automethod:: read

        Instead of using `!read()` you can even iterate on the object to read
        its data row by row, using ``async for row in copy: ...``.

    .. automethod:: write
    .. automethod:: write_row
