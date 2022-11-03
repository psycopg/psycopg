.. currentmodule:: psycopg

Other top-level objects
=======================

Connection information
----------------------

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

    .. autoattribute:: pipeline_status

    .. autoattribute:: backend_pid
    .. autoattribute:: vendor

        Normally it is `PostgreSQL`; it may be different if connected to
        a different database.

        .. versionadded:: 3.1

    .. autoattribute:: server_version

        The number is formed by converting the major, minor, and revision
        numbers into two-decimal-digit numbers and appending them together.
        Starting from PostgreSQL 10 the minor version was dropped, so the
        second group of digits is always 00. For example, version 9.3.5 is
        returned as 90305, version 10.2 as 100002.

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

    .. autoattribute:: encoding

        The value returned is always normalized to the Python codec
        `~codecs.CodecInfo.name`::

            conn.execute("SET client_encoding TO LATIN9")
            conn.info.encoding
            'iso8859-15'

        A few PostgreSQL encodings are not available in Python and cannot be
        selected (currently ``EUC_TW``, ``MULE_INTERNAL``). The PostgreSQL
        ``SQL_ASCII`` encoding has the special meaning of "no encoding": see
        :ref:`adapt-string` for details.

        .. seealso::

            The `PostgreSQL supported encodings`__.

            .. __: https://www.postgresql.org/docs/current/multibyte.html


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


Notifications
-------------

.. autoclass:: Notify()

    The object is usually returned by `Connection.notifies()`.

    .. attribute:: channel
        :type: str

        The name of the channel on which the notification was received.

    .. attribute:: payload
        :type: str

        The message attached to the notification.

    .. attribute:: pid
        :type: int

        The PID of the backend process which sent the notification.


Pipeline-related objects
------------------------

See :ref:`pipeline-mode` for details.

.. autoclass:: Pipeline

    This objects is returned by `Connection.pipeline()`.

    .. automethod:: sync
    .. automethod:: is_supported


.. autoclass:: AsyncPipeline

    This objects is returned by `AsyncConnection.pipeline()`.

    .. automethod:: sync


Transaction-related objects
---------------------------

See :ref:`transactions` for details about these objects.

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

    It can be used as:

    - ``raise Rollback``: roll back the operation that happened in the current
      transaction block and continue the program after the block.

    - ``raise Rollback()``: same effect as above

    - :samp:`raise Rollback({tx})`: roll back any operation that happened in
      the `Transaction` `!tx` (returned by a statement such as :samp:`with
      conn.transaction() as {tx}:` and all the blocks nested within. The
      program will continue after the `!tx` block.


Two-Phase Commit related objects
--------------------------------

.. autoclass:: Xid()

    See :ref:`two-phase-commit` for details.

    .. autoattribute:: format_id

        Format Identifier of the two-phase transaction.

    .. autoattribute:: gtrid

        Global Transaction Identifier of the two-phase transaction.

        If the Xid doesn't follow the XA standard, it will be the PostgreSQL
        ID of the transaction (in which case `format_id` and `bqual` will be
        `!None`).

    .. autoattribute:: bqual

        Branch Qualifier of the two-phase transaction.

    .. autoattribute:: prepared

        Timestamp at which the transaction was prepared for commit.

        Only available on transactions recovered by `~Connection.tpc_recover()`.

    .. autoattribute:: owner

        Named of the user that executed the transaction.

        Only available on recovered transactions.

    .. autoattribute:: database

        Named of the database in which the transaction was executed.

        Only available on recovered transactions.
