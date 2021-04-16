.. currentmodule:: psycopg3

Connection classes
==================

The `Connection` and `AsyncConnection` classes are the main wrappers for a
PostgreSQL database session. You can imagine them similar to a :program:`psql`
session.

One of the differences compared to :program:`psql` is that a `Connection`
usually handles a transaction automatically: other sessions will not be able
to see the changes until you have committed them, more or less explicitly.
Take a look to :ref:`transactions` for the details.


The `!Connection` class
-----------------------

.. autoclass:: Connection()

    This class implements a `DBAPI-compliant interface`__. It is what you want
    to use if you write a "classic", blocking program (eventually using
    threads or Eventlet/gevent for concurrency. If your program uses `asyncio`
    you might want to use `AsyncConnection` instead.

    .. __: https://www.python.org/dev/peps/pep-0249/#connection-objects

    Connections behave as context managers: on block exit, the current
    transaction will be committed (or rolled back, in case of exception) and
    the connection will be closed.

    .. automethod:: connect

        :param conninfo: The `connection string`__ (a ``postgresql://`` url or
                         a list of ``key=value pairs``) to specify where and
                         how to connect.
        :param kwargs: Further parameters specifying the connection string.
                       They override the ones specified in *conninfo*.
        :param autocommit: If `!True` don't start transactions automatically.
                           See `transactions` for details.
        :param row_factory: The row factory specifying what type of records
                            to create fetching data (default:
                            `~psycopg3.rows.tuple_row()`). See
                            :ref:`row-factories` for details.

        .. __: https://www.postgresql.org/docs/current/libpq-connect.html
            #LIBPQ-CONNSTRING

        This method is also aliased as `psycopg3.connect()`.

        .. seealso::

            - the list of `the accepted connection parameters`__
            - the `environment variables`__ affecting connection

            .. __: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
            .. __: https://www.postgresql.org/docs/current/libpq-envars.html

    .. automethod:: close

        .. note:: You can use :ref:`with connect(): ...<with-connection>` to
            close the connection automatically when the block is exited.

    .. autoattribute:: closed
    .. autoattribute:: broken


    .. method:: cursor(*, binary: bool = False, row_factory: Optional[RowFactory] = None) -> Cursor
    .. method:: cursor(name: str, *, binary: bool = False, row_factory: Optional[RowFactory] = None) -> ServerCursor
        :noindex:

        Return a new cursor to send commands and queries to the connection.

        :param name: If not specified create a client-side cursor, if
                     specified create a server-side cursor. See
                     :ref:`cursor-types` for details.
        :param binary: If `!True` return binary values from the database. All
                       the types returned by the query must have a binary
                       loader. See :ref:`binary-data` for details.
        :param row_factory: If specified override the `row_factory` set on the
                            connection. See :ref:`row-factories` for details.

        .. note:: You can use :ref:`with conn.cursor(): ...<usage>`
            to close the cursor automatically when the block is exited.

    .. automethod:: execute(query, params=None, prepare=None) -> Cursor

        :param query: The query to execute.
        :type query: `!str`, `!bytes`, or `sql.Composable`
        :param params: The parameters to pass to the query, if any.
        :type params: Sequence or Mapping
        :param prepare: Force (`!True`) or disallow (`!False`) preparation of
            the query. By default (`!None`) prepare automatically. See
            :ref:`prepared-statements`.

        The cursor is what returned calling `cursor()` without parameters. The
        parameters are passed to its `~Cursor.execute()` and the cursor is
        returned.

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. autoattribute:: row_factory
        :annotation: RowFactory

        Writable attribute to control how result rows are formed.
        See :ref:`row-factories` for details.

    .. rubric:: Transaction management methods

    For details see :ref:`transactions`.

    .. automethod:: commit()
    .. automethod:: rollback()
    .. automethod:: transaction

        .. note:: It must be called as ``with conn.transaction() as tx: ...``

        Inside a transaction block it will not be possible to call `commit()`
        or `rollback()`.

    .. autoattribute:: autocommit

        The property is writable for sync connections, read-only for async
        ones: you should call ``await`` `~AsyncConnection.set_autocommit`\
        :samp:`({value})` instead.

    .. rubric:: Checking and configuring the connection state

    .. autoattribute:: client_encoding

        The property is writable for sync connections, read-only for async
        ones: you should call ``await`` `~AsyncConnection.set_client_encoding`\
        :samp:`({value})` instead.

        The value returned is always normalized to the Python codec
        `~codecs.CodecInfo.name`::

            conn.client_encoding = 'latin9'
            conn.client_encoding
            'iso8859-15'

        and it reflects the current connection property, even if it is set
        outside Python::

            conn.execute("SET client_encoding TO LATIN1")
            conn.client_encoding
            'iso8859-1'

        A few PostgreSQL encodings are not available in Python and cannot be
        selected (currently ``EUC_TW``, ``MULE_INTERNAL``). The PostgreSQL
        ``SQL_ASCII`` encoding has the special meaning of "no encoding": see
        :ref:`adapt-string` for details.

        .. seealso::

            The `PostgreSQL supported encodings`__.

            .. __: https://www.postgresql.org/docs/current/multibyte.html


    .. autoattribute:: info

    .. automethod:: fileno

    .. autoattribute:: prepare_threshold

        See :ref:`prepared-statements` for details.


    .. autoattribute:: prepared_max

        If more queries need to be prepared, old ones are deallocated__.

        .. __: https://www.postgresql.org/docs/current/sql-deallocate.html


    .. rubric:: Methods you can use to do something cool

    .. automethod:: cancel

    .. automethod:: notifies

        Notifies are recevied after using :sql:`LISTEN` in a connection, when
        any sessions in the database generates a :sql:`NOTIFY` on one of the
        listened channels.

    .. automethod:: add_notify_handler

        :param callback: a callable taking a `Notify` parameter.

    .. automethod:: remove_notify_handler

    See :ref:`async-notify` for details.

    .. automethod:: add_notice_handler

        :param callback: a callable taking a `~psycopg3.errors.Diagnostic`
            object containing all the details about the notice.

    .. automethod:: remove_notice_handler


The `!AsyncConnection` class
----------------------------

.. autoclass:: AsyncConnection()

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines. Unless specified otherwise,
    non-blocking methods are shared with the `Connection` class.

    The following methods have the same behaviour of the matching `!Connection`
    methods, but should be called using the `await` keyword.

    .. automethod:: connect
    .. automethod:: close

        .. note:: You can use ``async with`` to close the connection
            automatically when the block is exited, but be careful about
            the async quirkness: see :ref:`async-with` for details.

    .. method:: cursor(*, binary: bool = False, row_factory: Optional[RowFactory] = None) -> AsyncCursor
    .. method:: cursor(name: str, *, binary: bool = False, row_factory: Optional[RowFactory] = None) -> AsyncServerCursor
        :noindex:

        .. note:: You can use ``async with conn.cursor() as cur: ...`` to
            close the cursor automatically when the block is exited.

    .. automethod:: execute(query, params=None, prepare=None) -> AsyncCursor
    .. automethod:: commit
    .. automethod:: rollback

    .. automethod:: transaction

        .. note:: It must be called as ``async with conn.transaction() as tx: ...``.

    .. automethod:: notifies
    .. automethod:: set_client_encoding
    .. automethod:: set_autocommit


Connection support objects
--------------------------

.. autoclass:: Notify()
    :members: channel, payload, pid

    The object is usually returned by `Connection.notifies()`.


.. autoclass:: ConnectionInfo()

    The object is usually returned by `Connection.info`.

    .. autoproperty:: status
    .. autoproperty:: transaction_status
    .. autoproperty:: server_version
    .. automethod:: get_parameters

    .. autoproperty:: host
    .. autoproperty:: port
    .. autoproperty:: dbname
    .. autoproperty:: user
    .. autoproperty:: password
    .. autoproperty:: options
    .. automethod:: parameter_status

        Example of parameters are ``server_version``,
        ``standard_conforming_string``... See :pq:`PQparameterStatus()` for
        all the available parameters.

    .. autoproperty:: protocol_version


.. rubric:: Objects involved in :ref:`transactions`

.. autoclass:: Transaction()

    .. autoproperty:: savepoint_name
    .. autoattribute:: connection

.. autoclass:: AsyncTransaction()

    .. autoattribute:: connection

.. autoexception:: Rollback

    It can be used as

    - ``raise Rollback``: roll back the operation that happened in the current
      transaction block and continue the program after the block.

    - ``raise Rollback()``: same effect as above

    - :samp:`raise Rollback({tx})`: roll back any operation that happened in
      the `Transaction` *tx* (returned by a statement such as :samp:`with
      conn.transaction() as {tx}:` and all the blocks nested within. The
      program will continue after the *tx* block.
