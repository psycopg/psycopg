.. currentmodule:: psycopg

Cursor classes
==============

The `Cursor` and `AsyncCursor` classes are the main objects to send commands
to a PostgreSQL database session. They are normally created by the
connection's `~Connection.cursor()` method.

Using the *name* parameter on `!cursor()` will create a `ServerCursor` or
`AsyncServerCursor`, which can be used to retrieve partial results from a
database.

A `Connection` can create several cursors, but only one at time can perform
operations, so they are not the best way to achieve parallelism (you may want
to operate with several connections instead). All the cursors on the same
connection have a view of the same session, so they can see each other's
uncommitted data.


The `!Cursor` class
-------------------

.. autoclass:: Cursor()

    This class implements a `DBAPI-compliant interface`__. It is what the
    classic `Connection.cursor()` method returns. `AsyncConnection.cursor()`
    will create instead `AsyncCursor` objects, which have the same set of
    method but expose an `asyncio` interface and require ``async`` and
    ``await`` keywords to operate.

    .. __: dbapi-cursor_

    Cursors behave as context managers: on block exit they are closed and
    further operation will not be possible. Closing a cursor will not
    terminate a transaction or a session though.

    .. attribute:: connection
        :type: Connection

        The connection this cursor is using.

    .. automethod:: close

        .. note::

            You can use::

                with conn.cursor() as cur:
                    ...

            to close the cursor automatically when the block is exited. See
            :ref:`usage`.

    .. autoattribute:: closed

    .. rubric:: Methods to send commands

    .. automethod:: execute(query, params=None, *, prepare=None) -> Cursor

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

        .. note::

            The method must be called with::

                with cursor.copy() as copy:
                    ...

        See :ref:`copy` for information about :sql:`COPY`.

    .. automethod:: stream(query, params=None) -> Iterable[Sequence[Any]]

        This command is similar to execute + iter; however it supports endless
        data streams. The feature is not available in PostgreSQL, but some
        implementations exist: Materialize `TAIL`__ and CockroachDB
        `CHANGEFEED`__ for instance.

        The feature, and the API supporting it, are still experimental.
        Beware... 👀

        .. __: https://materialize.com/docs/sql/tail/#main
        .. __: https://www.cockroachlabs.com/docs/stable/changefeed-for.html

        The parameters are the same of `execute()`.

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

    .. note::

        Cursors are iterable objects, so just using the::

            for record in cursor:
                ...

        syntax will iterate on the records in the current recordset.

    .. autoattribute:: row_factory

        The property affects the objects returned by the `fetchone()`,
        `fetchmany()`, `fetchall()` methods. The default
        (`~psycopg.rows.tuple_row`) returns a tuple for each record fetched.

        See :ref:`row-factories` for details.

    .. automethod:: fetchone
    .. automethod:: fetchmany
    .. automethod:: fetchall
    .. automethod:: nextset
    .. automethod:: scroll

    .. attribute:: pgresult
        :type: Optional[psycopg.pq.PGresult]

        The result returned by the last query and currently exposed by the
        cursor, if available, else `!None`.

        It can be used to obtain low level info about the last query result
        and to access to features not currently wrapped by Psycopg.


    .. rubric:: Information about the data

    .. autoattribute:: description

        A list of objects describing each column of the current queryset.

        `!None` if the last operation didn't return a queryset.

    .. autoattribute:: statusmessage

        This is the status tag you typically see in :program:`psql` after
        a successful command, such as ``CREATE TABLE`` or ``UPDATE 42``.

    .. autoattribute:: rowcount
    .. autoattribute:: rownumber

    .. attribute:: _query

        An helper object used to convert queries and parameters before sending
        them to PostgreSQL.

        .. note::
            This attribute is exposed because it might be helpful to debug
            problems when the communication between Python and PostgreSQL
            doesn't work as expected. For this reason, the attribute is
            available when a query fails too.

            .. warning::
                You shouldn't consider it part of the public interface of the
                object: it might change without warnings.

                Except this warning, I guess.

            If you would like to build reliable features using this object,
            please get in touch so we can try and design an useful interface
            for it.

        Among the properties currently exposed by this object:

        - `!query` (`!bytes`): the query effectively sent to PostgreSQL. It
          will have Python placeholders (``%s``\-style) replaced with
          PostgreSQL ones (``$1``, ``$2``\-style).

        - `!params` (sequence of `!bytes`): the parameters passed to
          PostgreSQL, adapted to the database format.

        - `!types` (sequence of `!int`): the OID of the parameters passed to
          PostgreSQL.

        - `!formats` (sequence of `pq.Format`): whether the parameter format
          is text or binary.


The `!ServerCursor` class
--------------------------

.. autoclass:: ServerCursor()

    This class also implements a `DBAPI-compliant interface`__. It is created
    by `Connection.cursor()` specifying the *name* parameter. Using this
    object results in the creation of an equivalent PostgreSQL cursor in the
    server. DBAPI-extension methods (such as `~Cursor.copy()` or
    `~Cursor.stream()`) are not implemented on this object: use a normal
    `Cursor` instead.

    .. __: dbapi-cursor_

    Most attribute and methods behave exactly like in `Cursor`, here are
    documented the differences:

    .. autoattribute:: name
    .. autoattribute:: scrollable

       .. seealso:: The PostgreSQL DECLARE_ statement documetation
          for the description of :sql:`[NO] SCROLL`.

    .. autoattribute:: withhold

       .. seealso:: The PostgreSQL DECLARE_ statement documetation
          for the description of :sql:`{WITH|WITHOUT} HOLD`.

    .. _DECLARE: https://www.postgresql.org/docs/current/sql-declare.html


    .. automethod:: close

        .. warning:: Closing a server-side cursor is more important than
            closing a client-side one because it also releases the resources
            on the server, which otherwise might remain allocated until the
            end of the session (memory, locks). Using the pattern::

                with conn.cursor():
                    ...

            is especially useful so that the cursor is closed at the end of
            the block.

    .. automethod:: execute(query, params=None, *) -> ServerCursor

        :param query: The query to execute.
        :type query: `!str`, `!bytes`, or `sql.Composable`
        :param params: The parameters to pass to the query, if any.
        :type params: Sequence or Mapping

        Create a server cursor with given `name` and the *query* in argument.

        If using :sql:`DECLARE` is not appropriate (for instance because the
        cursor is returned by calling a stored procedure) you can avoid to use
        `!execute()`, crete the cursor in other ways, and use directly the
        `!fetch*()` methods instead. See :ref:`cursor-steal` for an example.

        Using `!execute()` more than once will close the previous cursor and
        open a new one with the same name.

    .. automethod:: executemany(query: Query, params_seq: Sequence[Args])

    .. automethod:: fetchone
    .. automethod:: fetchmany
    .. automethod:: fetchall

        These methods use the FETCH_ SQL statement to retrieve some of the
        records from the cursor's current position.

        .. _FETCH: https://www.postgresql.org/docs/current/sql-fetch.html

        .. note::

            You can also iterate on the cursor to read its result one at
            time with::

                for record in cur:
                    ...

            In this case, the records are not fetched one at time from the
            server but they are retrieved in batches of `itersize` to reduce
            the number of server roundtrips.

    .. autoattribute:: itersize

        Number of records to fetch at time when iterating on the cursor. The
        default is 100.

    .. automethod:: scroll

        This method uses the MOVE_ SQL statement to move the current position
        in the server-side cursor, which will affect following `!fetch*()`
        operations. If you need to scroll backwards you should probably
        call `~Connection.cursor()` using `scrollable=True`.

        Note that PostgreSQL doesn't provide a reliable way to report when a
        cursor moves out of bound, so the method might not raise `!IndexError`
        when it happens, but it might rather stop at the cursor boundary.

        .. _MOVE: https://www.postgresql.org/docs/current/sql-fetch.html


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

        .. note::

            You can use::

                async with conn.cursor():
                    ...

            to close the cursor automatically when the block is exited.

    .. automethod:: execute(query, params=None, *, prepare=None) -> AsyncCursor
    .. automethod:: executemany(query: Query, params_seq: Sequence[Args])
    .. automethod:: copy(statement: Query) -> AsyncCopy

        .. note::

            The method must be called with::

                async with cursor.copy() as copy:
                    ...

    .. automethod:: stream(query, params=None) -> AsyncIterable[Sequence[Any]]

        .. note::

            The method must be called with::

                async for record in cursor.stream(query):
                    ...

    .. automethod:: fetchone
    .. automethod:: fetchmany
    .. automethod:: fetchall
    .. automethod:: scroll

    .. note::

        You can also use::

            async for record in cursor:
                ...

        to iterate on the async cursor results.


The `!AsyncServerCursor` class
------------------------------

.. autoclass:: AsyncServerCursor()

    This class implements a DBAPI-inspired interface as the `AsyncCursor`
    does, but wraps a server-side cursor like the `ServerCursor` class. It is
    created by `AsyncConnection.cursor()` specifying the *name* parameter.

    The following are the methods exposing a different (async) interface from
    the `ServerCursor` counterpart, but sharing the same semantics.

    .. automethod:: close

        .. note::
            You can close the cursor automatically using::

                async with conn.cursor("name") as cursor:
                    ...

    .. automethod:: execute(query, params=None) -> AsyncServerCursor
    .. automethod:: executemany(query: Query, params_seq: Sequence[Args])
    .. automethod:: fetchone
    .. automethod:: fetchmany
    .. automethod:: fetchall

        .. note::

            You can also iterate on the cursor using::

                async for record in cur:
                    ...

    .. automethod:: scroll


The description `Column` object
-------------------------------

.. autoclass:: Column()

    An object describing a column of data from a database result, `as described
    by the DBAPI`__, so it can also be unpacked as a 7-items tuple.

    The object is returned by `Cursor.description`.

    .. __: https://www.python.org/dev/peps/pep-0249/#description

    .. autoattribute:: name
    .. autoattribute:: type_code
    .. autoattribute:: display_size
    .. autoattribute:: internal_size
    .. autoattribute:: precision
    .. autoattribute:: scale


COPY-related objects
--------------------

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


.. _dbapi-cursor: https://www.python.org/dev/peps/pep-0249/#cursor-objects
