The ``Connection`` classes
==========================

.. currentmodule:: psycopg3

The `Connection` and `AsyncConnection` classes are the main wrappers for a
PostgreSQL database session. You can imagine them similar to a :program:`psql`
session.

One of the differences compared to :program:`psql` is that a `Connection`
usually handles a transaction automatically: other sessions will not be able
to see the changes until you have committed them, more or less explicitly.
Take a look to :ref:`transactions` for the details.

.. autoclass:: Connection

    This class implements a DBAPI-compliant interface. It is what you want to
    use if you write a "classic", blocking program (eventually using threads or
    Eventlet/gevent for concurrency. If your program uses `asyncio` you might
    want to use `AsyncConnection` instead.

    Connections behave as context managers: on block exit, the current
    transaction will be committed (or rolled back, in case of exception) and
    the connection will be closed.

    .. rubric:: Methods you will need every day

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

    .. automethod:: cursor
    .. automethod:: commit
    .. automethod:: rollback

    .. autoattribute:: autocommit

        The property is writable for sync connections, read-only for async
        ones: you can call `~AsyncConnection.set_autocommit()` on those.

    For details see :ref:`transactions`.

    .. automethod:: close

    .. rubric:: Checking and configuring the connection state

    .. autoproperty:: closed
    .. autoproperty:: client_encoding

        The property is writable for sync connections, read-only for async
        ones: you can call `~AsyncConnection.set_client_encoding()` on those.

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
    .. automethod:: remove_notice_handler

    TODO: document `Diagnostic`


.. autoclass:: AsyncConnection

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines. Unless specified otherwise,
    non-blocking methods are shared with the `Connection` class.

    The following methods have the same behaviour of the matching `~Connection`
    methods, but have an `async` interface.

    .. automethod:: connect
    .. automethod:: close
    .. automethod:: cursor
    .. automethod:: commit
    .. automethod:: rollback
    .. automethod:: notifies
    .. automethod:: set_client_encoding
    .. automethod:: set_autocommit


.. autoclass:: Notify
    :members: channel, payload, pid
