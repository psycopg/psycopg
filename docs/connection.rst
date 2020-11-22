Connection classes
==================

.. currentmodule:: psycopg3

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

        Connection parameters can be passed either as a `conninfo string`__ (a
        ``postgresql://`` url or a list of ``key=value pairs``) or as keywords.
        Keyword parameters override the ones specified in the connection string.

        .. __: https://www.postgresql.org/docs/current/libpq-connect.html
            #LIBPQ-CONNSTRING

        This method is also aliased as `psycopg3.connect()`.

        .. seealso::

            - the list of `the accepted connection parameters`__
            - the `environment varialbes`__ affecting connection

            .. __: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
            .. __: https://www.postgresql.org/docs/current/libpq-envars.html

    .. automethod:: close

        .. note:: you can use :ref:`with connect(): ...<with-statement>` to
            close the connection automatically when the block is exited.

    .. autoattribute:: closed
        :annotation: bool

    .. automethod:: cursor

        .. note:: you can use :ref:`with conn.cursor(): ...<with-statement>`
            to close the cursor automatically when the block is exited.


    .. rubric:: Transaction management methods

    For details see :ref:`transactions`.

    .. automethod:: commit()
    .. automethod:: rollback()
    .. automethod:: transaction(savepoint_name: Optional[str] = None, force_rollback: bool = False) -> Transaction

        .. note:: it must be called as ``with conn.transaction() as tx: ...``

        Inside a transaction block it will not be possible to call `commit()`
        or `rollback()`.

    .. autoattribute:: autocommit
        :annotation: bool

        The property is writable for sync connections, read-only for async
        ones: you should call ``await`` `~AsyncConnection.set_autocommit`\
        :samp:`({value})` instead.

    .. rubric:: Checking and configuring the connection state

    .. autoattribute:: client_encoding
        :annotation: str

        The property is writable for sync connections, read-only for async
        ones: you should call ``await`` `~AsyncConnection.set_client_encoding`\
        :samp:`({value})` instead.

    .. attribute:: info

        TODO

    .. rubric:: Methods you can use to do something cool

    .. automethod:: notifies

        Notifies are recevied after using :sql:`LISTEN` in a connection, when
        any sessions in the database generates a :sql:`NOTIFY` on one of the
        listened channels.

    .. automethod:: add_notify_handler
    .. automethod:: remove_notify_handler

    See :ref:`async-notify` for details.

    .. automethod:: cancel
    .. automethod:: add_notice_handler

        The argument of the callback is a `~psycopg3.errors.Diagnostic` object
        containing all the details about the notice.

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

        .. note:: you can use ``async with`` to close the connection
            automatically when the block is exited, but be careful about
            the async quirkness: see :ref:`with-statement` for details.

    .. automethod:: cursor

        .. note:: you can use ``async with`` to close the cursor
            automatically when the block is exited, but be careful about
            the async quirkness: see :ref:`with-statement` for details.

    .. automethod:: commit
    .. automethod:: rollback

    .. automethod:: transaction(savepoint_name: Optional[str] = None, force_rollback: bool = False) -> AsyncTransaction

        .. note:: it must be called as ``async with conn.transaction() as tx: ...``.

    .. automethod:: notifies
    .. automethod:: set_client_encoding
    .. automethod:: set_autocommit


Connection support objects
--------------------------

.. autoclass:: Notify()
    :members: channel, payload, pid

    The object is usually returned by `Connection.notifies()`.


.. rubric:: Objects involved in :ref:`transactions`

.. autoclass:: Transaction()

    .. autoproperty:: savepoint_name
    .. autoattribute:: connection
        :annotation: Connection

.. autoclass:: AsyncTransaction()

.. autoexception:: Rollback

    It can be used as

    - ``raise Rollback``: roll back the operation that happened in the current
      transaction block and continue the program after the block.

    - ``raise Rollback()``: same effect as above

    - :samp:`raise Rollback({tx})`: roll back any operation that happened in
      the `Transaction` *tx* (returned by a statement such as :samp:`with
      conn.transaction() as {tx}:` and all the blocks nested within. The
      program will continue after the *tx* block.
