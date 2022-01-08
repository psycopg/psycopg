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
    threads or Eventlet/gevent for concurrency). If your program uses `asyncio`
    you might want to use `AsyncConnection` instead.

    .. __: https://www.python.org/dev/peps/pep-0249/#connection-objects

    Connections behave as context managers: on block exit, the current
    transaction will be committed (or rolled back, in case of exception) and
    the connection will be closed.

    .. automethod:: connect

        :param conninfo: The `connection string`__ (a ``postgresql://`` url or
            a list of ``key=value`` pairs) to specify where and how to connect.
        :param kwargs: Further parameters specifying the connection string.
            They override the ones specified in `!conninfo`.
        :param autocommit: If `!True` don't start transactions automatically.
            See :ref:`transactions` for details.
        :param row_factory: The row factory specifying what type of records
            to create fetching data (default: `~psycopg.rows.tuple_row()`). See
            :ref:`row-factories` for details.
        :param cursor_factory: Initial value for the `cursor_factory` attribute
            of the connection (new in Psycopg 3.1).
        :param prepare_threshold: Initial value for the `prepare_threshold`
            attribute of the connection (new in Psycopg 3.1).

        More specialized use:

        :param context: A context to copy the initial adapters configuration
            from. It might be an `~psycopg.adapt.AdaptersMap` with customized
            loaders and dumpers, used as a template to create several connections.
            See :ref:`adaptation` for further details.

        .. __: https://www.postgresql.org/docs/current/libpq-connect.html
            #LIBPQ-CONNSTRING

        This method is also aliased as `psycopg.connect()`.

        .. seealso::

            - the list of `the accepted connection parameters`__
            - the `environment variables`__ affecting connection

            .. __: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
            .. __: https://www.postgresql.org/docs/current/libpq-envars.html

        .. versionchanged:: 3.1
            added `!prepare_threshold` and `!cursor_factory` parameters.

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
                 `server_cursor_factory` if `!name` is specified).

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
        :type query: `!str`, `!bytes`, `sql.SQL`, or `sql.Composed`
        :param params: The parameters to pass to the query, if any.
        :type params: Sequence or Mapping
        :param prepare: Force (`!True`) or disallow (`!False`) preparation of
            the query. By default (`!None`) prepare automatically. See
            :ref:`prepared-statements`.
        :param binary: If `!True` the cursor will return binary values from the
            database. All the types returned by the query must have a binary
            loader. See :ref:`binary-data` for details.

        The method simply creates a `Cursor` instance, `~Cursor.execute()` the
        query requested, and returns it.

        See :ref:`query-parameters` for all the details about executing
        queries.

    .. automethod:: pipeline

        The method is a context manager: you should call it using::

            with conn.pipeline() as p:
                ...

        At the end of the block, a synchronization point is established and
        the connection returns in normal mode.

        You can call the method recursively from within a pipeline block.
        Innermost blocks will establish a synchronization point on exit, but
        pipeline mode will be kept until the outermost block exits.

        See :ref:`pipeline-mode` for details.

        .. versionadded:: 3.1


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
            `Transaction` object. See :ref:`transaction-context` for details.

        Inside a transaction block it will not be possible to call `commit()`
        or `rollback()`.

    .. autoattribute:: autocommit

        The property is writable for sync connections, read-only for async
        ones: you should call `!await` `~AsyncConnection.set_autocommit`
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

        It can be used to send low level commands to PostgreSQL and access
        features not currently wrapped by Psycopg.

    .. autoattribute:: info

    .. autoattribute:: prepare_threshold

        See :ref:`prepared-statements` for details.


    .. autoattribute:: prepared_max

        If more queries need to be prepared, old ones are deallocated__.

        .. __: https://www.postgresql.org/docs/current/sql-deallocate.html


    .. rubric:: Methods you can use to do something cool

    .. automethod:: cancel

    .. automethod:: notifies

        Notifies are received after using :sql:`LISTEN` in a connection, when
        any sessions in the database generates a :sql:`NOTIFY` on one of the
        listened channels.

    .. automethod:: add_notify_handler

        See :ref:`async-notify` for details.

    .. automethod:: remove_notify_handler

    .. automethod:: add_notice_handler

        See :ref:`async-messages` for details.

    .. automethod:: remove_notice_handler

    .. automethod:: fileno


    .. _tpc-methods:

    .. rubric:: Two-Phase Commit support methods

    .. versionadded:: 3.1

    .. seealso:: :ref:`two-phase-commit` for an introductory explanation of
        these methods.

    .. automethod:: xid

    .. automethod:: tpc_begin

        :param xid: The id of the transaction
        :type xid: Xid or str

        This method should be called outside of a transaction (i.e. nothing
        may have executed since the last `commit()` or `rollback()` and
        `~ConnectionInfo.transaction_status` is `~pq.TransactionStatus.IDLE`).

        Furthermore, it is an error to call `!commit()` or `!rollback()`
        within the TPC transaction: in this case a `ProgrammingError`
        is raised.

        The `!xid` may be either an object returned by the `xid()` method or a
        plain string: the latter allows to create a transaction using the
        provided string as PostgreSQL transaction id. See also
        `tpc_recover()`.


    .. automethod:: tpc_prepare

        A `ProgrammingError` is raised if this method is used outside of a TPC
        transaction.

        After calling `!tpc_prepare()`, no statements can be executed until
        `tpc_commit()` or `tpc_rollback()` will be
        called.

        .. seealso:: The |PREPARE TRANSACTION|_ PostgreSQL command.

        .. |PREPARE TRANSACTION| replace:: :sql:`PREPARE TRANSACTION`
        .. _PREPARE TRANSACTION: https://www.postgresql.org/docs/current/static/sql-prepare-transaction.html


    .. automethod:: tpc_commit

        :param xid: The id of the transaction
        :type xid: Xid or str

        When called with no arguments, `!tpc_commit()` commits a TPC
        transaction previously prepared with `tpc_prepare()`.

        If `!tpc_commit()` is called prior to `!tpc_prepare()`, a single phase
        commit is performed.  A transaction manager may choose to do this if
        only a single resource is participating in the global transaction.

        When called with a transaction ID `!xid`, the database commits the
        given transaction.  If an invalid transaction ID is provided, a
        `ProgrammingError` will be raised.  This form should be called outside
        of a transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        .. seealso:: The |COMMIT PREPARED|_ PostgreSQL command.

        .. |COMMIT PREPARED| replace:: :sql:`COMMIT PREPARED`
        .. _COMMIT PREPARED: https://www.postgresql.org/docs/current/static/sql-commit-prepared.html


    .. automethod:: tpc_rollback

        :param xid: The id of the transaction
        :type xid: Xid or str

        When called with no arguments, `!tpc_rollback()` rolls back a TPC
        transaction.  It may be called before or after `tpc_prepare()`.

        When called with a transaction ID `!xid`, it rolls back the given
        transaction.  If an invalid transaction ID is provided, a
        `ProgrammingError` is raised.  This form should be called outside of a
        transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        .. seealso:: The |ROLLBACK PREPARED|_ PostgreSQL command.

        .. |ROLLBACK PREPARED| replace:: :sql:`ROLLBACK PREPARED`
        .. _ROLLBACK PREPARED: https://www.postgresql.org/docs/current/static/sql-rollback-prepared.html


    .. automethod:: tpc_recover

        Returns a list of `Xid` representing pending transactions, suitable
        for use with `tpc_commit()` or `tpc_rollback()`.

        If a transaction was not initiated by Psycopg, the returned Xids will
        have attributes `~Xid.format_id` and `~Xid.bqual` set to `!None` and
        the `~Xid.gtrid` set to the PostgreSQL transaction ID: such Xids are
        still usable for recovery.  Psycopg uses the same algorithm of the
        `PostgreSQL JDBC driver`__ to encode a XA triple in a string, so
        transactions initiated by a program using such driver should be
        unpacked correctly.

        .. __: https://jdbc.postgresql.org/

        Xids returned by `!tpc_recover()` also have extra attributes
        `~Xid.prepared`, `~Xid.owner`, `~Xid.database` populated with the
        values read from the server.

        .. seealso:: the |pg_prepared_xacts|_ system view.

        .. |pg_prepared_xacts| replace:: `pg_prepared_xacts`
        .. _pg_prepared_xacts: https://www.postgresql.org/docs/current/static/view-pg-prepared-xacts.html


The `!AsyncConnection` classes
------------------------------

.. autoclass:: AsyncConnection()

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines. Unless specified otherwise,
    non-blocking methods are shared with the `Connection` class.

    The following methods have the same behaviour of the matching `!Connection`
    methods, but should be called using the `await` keyword.

    .. automethod:: connect

        .. versionchanged:: 3.1

            Automatically resolve domain names asynchronously. In previous
            versions, name resolution blocks, unless the `!hostaddr`
            parameter is specified, or the `~psycopg._dns.resolve_hostaddr_async()`
            function is used.

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

    .. automethod:: pipeline

        .. note::

            It must be called as::

                async with conn.pipeline() as p:
                    ...

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

    .. automethod:: tpc_prepare
    .. automethod:: tpc_commit
    .. automethod:: tpc_rollback
    .. automethod:: tpc_recover


.. autoclass:: AnyIOConnection()

    This is class is similar to `AsyncConnection` but uses anyio_ as an
    asynchronous library instead of `asyncio`.

    To use this class, run ``pip install "psycopg[anyio]"`` to install
    required dependencies.

.. _anyio: https://anyio.readthedocs.io/
