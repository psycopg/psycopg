.. currentmodule:: psycopg

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
                            `~psycopg.rows.tuple_row()`). See
                            :ref:`row-factories` for details.

        More specialized use:

        :param context: A context to copy the initial adapters configuration
                        from. It might be an `~psycopg.adapt.AdaptersMap` with
                        customized loaders and dumpers, used as a template to
                        create several connections. See :ref:`adaptation` for
                        further details.

        .. __: https://www.postgresql.org/docs/current/libpq-connect.html
            #LIBPQ-CONNSTRING

        This method is also aliased as `psycopg.connect()`.

        .. seealso::

            - the list of `the accepted connection parameters`__
            - the `environment variables`__ affecting connection

            .. __: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
            .. __: https://www.postgresql.org/docs/current/libpq-envars.html

    .. automethod:: close

        .. note::

            You can use::

                with psycopg.connect() as conn:
                    ...

            to close the connection automatically when the block is exited.
            See :ref:`with-connection`.

    .. autoattribute:: closed
    .. autoattribute:: broken

    .. method:: cursor(*, binary: bool = False, \
           row_factory: Optional[RowFactory] = None) -> Cursor
    .. method:: cursor(name: str, *, binary: bool = False, \
            row_factory: Optional[RowFactory] = None, \
            scrollable: Optional[bool] = None, withhold: bool = False) -> ServerCursor
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
        :param scrollable: Specify the `~ServerCursor.scrollable` property of
                           the server-side cursor created.
        :param withhold: Specify the `~ServerCursor.withhold` property of
                         the server-side cursor created.
        :return: A cursor of the class specified by `cursor_factory` (or
                 `server_cursor_factory` if *name* is specified).

        .. note::

            You can use::

                with conn.cursor() as cur:
                    ...

            to close the cursor automatically when the block is exited.

    .. autoattribute:: cursor_factory

        The type, or factory function, returned by `cursor()` and `execute()`.

        Default is `psycopg.Cursor`.

    .. autoattribute:: server_cursor_factory

        The type, or factory function, returned by `cursor()` when a name is
        specified.

        Default is `psycopg.ServerCursor`.

    .. autoattribute:: row_factory

        The row factory defining the type of rows returned by
        `~Cursor.fetchone()` and the other cursor fetch methods.

        The default is `~psycopg.rows.tuple_row`, which means that the fetch
        methods will return simple tuples.

        .. seealso:: See :ref:`row-factories` for details about defining the
            objects returned by cursors.

    .. automethod:: execute

        :param query: The query to execute.
        :type query: `!str`, `!bytes`, or `sql.Composable`
        :param params: The parameters to pass to the query, if any.
        :type params: Sequence or Mapping
        :param prepare: Force (`!True`) or disallow (`!False`) preparation of
            the query. By default (`!None`) prepare automatically. See
            :ref:`prepared-statements`.
        :param binary: If `!True` the cursor will return binary values from the
            database. All the types returned by the query must have a binary
            loader. See :ref:`binary-data` for details.

        The method simply creates a `Cursor` instance, `~Cursor.execute()` the
        query requested, and return it.

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. rubric:: Transaction management methods

    For details see :ref:`transactions`.

    .. automethod:: commit
    .. automethod:: rollback
    .. automethod:: transaction

        .. note::

            The method must be called with a syntax such as::

                with conn.transaction():
                    ...

                with conn.transaction() as tx:
                    ...

            The latter is useful if you need to interact with the
            `Transaction` object. See :ref:`transaction-block` for details.

        Inside a transaction block it will not be possible to call `commit()`
        or `rollback()`.

    .. autoattribute:: autocommit

        The property is writable for sync connections, read-only for async
        ones: you should call ``await`` `~AsyncConnection.set_autocommit`
        :samp:`({value})` instead.

    The following three properties control the characteristics of new
    transactions. See :ref:`transaction-characteristics` for details.

    .. autoattribute:: isolation_level

        `!None` means use the default set in the default_transaction_isolation__
        configuration parameter of the server.

        .. __: https://www.postgresql.org/docs/current/runtime-config-client.html
               #GUC-DEFAULT-TRANSACTION-ISOLATION

    .. autoattribute:: read_only

        `!None` means use the default set in the default_transaction_read_only__
        configuration parameter of the server.

        .. __: https://www.postgresql.org/docs/current/runtime-config-client.html
               #GUC-DEFAULT-TRANSACTION-READ-ONLY

    .. autoattribute:: deferrable

        `!None` means use the default set in the default_transaction_deferrable__
        configuration parameter of the server.

        .. __: https://www.postgresql.org/docs/current/runtime-config-client.html
               #GUC-DEFAULT-TRANSACTION-DEFERRABLE


    .. rubric:: Checking and configuring the connection state

    .. attribute:: pgconn
        :type: psycopg.pq.PGconn

        The `~pq.PGconn` libpq connection wrapper underlying the `!Connection`.

        It can be used to send low level commands to PostgreSQL and access to
        features not currently wrapped by Psycopg.

    .. autoattribute:: info

    .. autoattribute:: client_encoding

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

        :param callback: a callable taking a `~psycopg.errors.Diagnostic`
            object containing all the details about the notice.

    .. automethod:: remove_notice_handler

    .. automethod:: fileno


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

    .. method:: cursor(*, binary: bool = False, \
            row_factory: Optional[RowFactory] = None) -> AsyncCursor
    .. method:: cursor(name: str, *, binary: bool = False, \
            row_factory: Optional[RowFactory] = None, \
            scrollable: Optional[bool] = None, withhold: bool = False) -> AsyncServerCursor
        :noindex:

        .. note::

            You can use::

                async with conn.cursor() as cur:
                    ...

            to close the cursor automatically when the block is exited.

    .. autoattribute:: cursor_factory

        Default is `psycopg.AsyncCursor`.

    .. autoattribute:: server_cursor_factory

        Default is `psycopg.AsyncServerCursor`.

    .. autoattribute:: row_factory

    .. automethod:: execute
    .. automethod:: commit
    .. automethod:: rollback

    .. automethod:: transaction

        .. note::

            It must be called as::

                async with conn.transaction() as tx:
                    ...

    .. automethod:: notifies
    .. automethod:: set_autocommit
    .. automethod:: set_isolation_level
    .. automethod:: set_read_only
    .. automethod:: set_deferrable


Connection support objects
--------------------------

.. autoclass:: Notify()
    :members: channel, payload, pid

    The object is usually returned by `Connection.notifies()`.


.. autoclass:: ConnectionInfo()

    The object is usually returned by `Connection.info`.

    .. autoattribute:: dsn

        .. note:: The `get_parameters()` method returns the same information
            as a dict.

    .. autoattribute:: status

        The status can be one of a number of values. However, only two of
        these are seen outside of an asynchronous connection procedure:
        `~pq.ConnStatus.OK` and `~pq.ConnStatus.BAD`. A good connection to the
        database has the status `!OK`. Ordinarily, an `!OK` status will remain
        so until `Connection.close()`, but a communications failure might
        result in the status changing to `!BAD` prematurely.

    .. autoattribute:: transaction_status

        The status can be `~pq.TransactionStatus.IDLE` (currently idle),
        `~pq.TransactionStatus.ACTIVE` (a command is in progress),
        `~pq.TransactionStatus.INTRANS` (idle, in a valid transaction block),
        or `~pq.TransactionStatus.INERROR` (idle, in a failed transaction
        block). `~pq.TransactionStatus.UNKNOWN` is reported if the connection
        is bad. `!ACTIVE` is reported only when a query has been sent to the
        server and not yet completed.

    .. autoattribute:: backend_pid
    .. autoattribute:: server_version
    .. autoattribute:: error_message

    .. automethod:: get_parameters

        .. note:: The `dsn` attribute returns the same information in the form
                as a string.

    .. autoattribute:: timezone

        .. code:: pycon

            >>> conn.info.timezone
            zoneinfo.ZoneInfo(key='Europe/Rome')

    .. autoattribute:: host

        This can be a host name, an IP address, or a directory path if the
        connection is via Unix socket. (The path case can be distinguished
        because it will always be an absolute path, beginning with ``/``.)

    .. autoattribute:: hostaddr

        Only available if the libpq used is at least from PostgreSQL 12.
        Raise `~psycopg.NotSupportedError` otherwise.

    .. autoattribute:: port
    .. autoattribute:: dbname
    .. autoattribute:: user
    .. autoattribute:: password
    .. autoattribute:: options
    .. automethod:: parameter_status

        Example of parameters are ``server_version``,
        ``standard_conforming_strings``... See :pq:`PQparameterStatus()` for
        all the available parameters.


.. rubric:: Objects involved in :ref:`transactions`

.. autoclass:: IsolationLevel
    :members:

    The value is usually used with the `Connection.isolation_level` property.

    Check the PostgreSQL documentation for a description of the effects of the
    different `levels of transaction isolation`__.

    .. __: https://www.postgresql.org/docs/current/transaction-iso.html

.. autoclass:: Transaction()

    .. autoattribute:: savepoint_name
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
