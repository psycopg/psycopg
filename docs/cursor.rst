.. currentmodule:: psycopg3

Cursor classes
==============

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

    .. attribute:: connection
        :type: Connection

        The connection this cursor is using.

    .. automethod:: close

        .. note:: you can use :ref:`with conn.cursor(): ...<usage>`
            to close the cursor automatically when the block is exited.

    .. autoattribute:: closed
        :annotation: bool

    .. rubric:: Methods to send commands

    .. automethod:: execute(query, params=None, prepare=None) -> Cursor

        :param query: The query to execute.
        :type query: `!str`, `!bytes`, or `sql.Composable`
        :param params: The parameters to pass to the query, if any.
        :type params: Sequence or Mapping
        :param prepare: Force (`!True`) or disallow (`!False`) preparation of
            the query. By default (`!None`) prepare automatically. See
            :ref:`prepared-statements`.

        Return the cursor itself, so that it will be possible to chain a fetch
        operation after the call.

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. automethod:: executemany(query: Query, params_seq: Sequence[Args])

        :param query: The query to execute
        :type query: `!str`, `!bytes`, or `sql.Composable`
        :param params_seq: The parameters to pass to the query
        :type params_seq: Sequence of Sequences or Mappings

        This is more efficient than performing separate queries, but in case of
        several :sql:`INSERT` (and with some SQL creativity for massive
        :sql:`UPDATE` too) you may consider using `copy()`.

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. automethod:: copy(statement: Query) -> Copy

        :param statement: The copy operation to execute
        :type statement: `!str`, `!bytes`, or `sql.Composable`

        .. note:: it must be called as ``with cur.copy() as copy: ...``

        See :ref:`copy` for information about :sql:`COPY`.

    .. attribute:: format

        The format of the data returned by the queries. It can be selected
        initially e.g. specifying `Connection.cursor`\ ``(binary=True)`` and
        changed during the cursor's lifetime.

        :type: pq.Format

        .. admonition:: TODO

            Add `execute`\ ``(binary=True)`` too?


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

    .. attribute:: description
        :type: Optional[List[Column]]

        A list of objects describing each column of the current queryset.

        `!None` if the last operation didn't return a queryset.

    .. autoattribute:: rowcount
        :annotation: int

    .. autoattribute:: query
        :annotation: Optional[bytes]

        The query will be in PostgreSQL format (with ``$1``, ``$2``...
        parameters), the parameters will *not* be merged to the query: see
        `params`.

    .. autoattribute:: params
        :annotation: Optional[List[Optional[bytes]]]

        The parameters are adapted to PostgreSQL format.


The `!AsyncCursor` class
------------------------

.. autoclass:: AsyncCursor()

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines. Unless specified otherwise,
    non-blocking methods are shared with the `Cursor` class.

    The following methods have the same behaviour of the matching `!Cursor`
    methods, but should be called using the `await` keyword.

    .. attribute:: connection
        :type: AsyncConnection

    .. automethod:: close

        .. note:: you can use ``async with`` to close the cursor
            automatically when the block is exited, but be careful about
            the async quirkness: see :ref:`async-with` for details.

    .. automethod:: execute(query, params=None, prepare=None) -> AsyncCursor
    .. automethod:: executemany(query: Query, params_seq: Sequence[Args])
    .. automethod:: copy(statement: Query) -> AsyncCopy

        .. note:: it must be called as ``async with cur.copy() as copy: ...``

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
