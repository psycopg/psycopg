.. _psycopg.pq:

`pq` -- libpq wrapper module
============================

.. index::
    single: libpq

.. module:: psycopg.pq

Psycopg is built around the libpq_, the PostgreSQL client library, which
performs most of the network communications and returns query results in C
structures.

.. _libpq: https://www.postgresql.org/docs/current/libpq.html

The low-level functions of the library are exposed by the objects in the
`!psycopg.pq` module.


.. _pq-impl:

``pq`` module implementations
-----------------------------

There are actually several implementations of the module, all offering the
same interface. Current implementations are:

- ``python``: a pure-python implementation, implemented using the `ctypes`
  module. It is less performing than the others, but it doesn't need a C
  compiler to install. It requires the libpq installed in the system.

- ``c``: a C implementation of the libpq wrapper (more precisely, implemented
  in Cython_). It is much better performing than the ``python``
  implementation, however it requires development packages installed on the
  client machine. It can be installed using the ``c`` extra, i.e. running
  ``pip install "psycopg[c]"``.

- ``binary``: a pre-compiled C implementation, bundled with all the required
  libraries. It is the easiest option to deal with, fast to install and it
  should require no development tool or client library, however it may be not
  available for every platform. You can install it using the ``binary`` extra,
  i.e. running ``pip install "psycopg[binary]"``.

.. _Cython: https://cython.org/

The implementation currently used is available in the `~psycopg.pq.__impl__`
module constant.

At import time, Psycopg 3 will try to use the best implementation available
and will fail if none is usable. You can force the use of a specific
implementation by exporting the env var :envvar:`PSYCOPG_IMPL`: importing the
library will fail if the requested implementation is not available::

    $ PSYCOPG_IMPL=c python -c "import psycopg"
    Traceback (most recent call last):
       ...
    ImportError: couldn't import requested psycopg 'c' implementation: No module named 'psycopg_c'

.. envvar:: PSYCOPG_IMPL

    If set, force the use of a specific implementation of the libpq wrapper:
    ``python``, ``c``, or ``binary``.


Module content
--------------

.. autodata:: __impl__

    The choice of implementation is automatic but can be forced setting the
    :envvar:`PSYCOPG_IMPL` env var.


.. autofunction:: version

    .. seealso:: the :pq:`PQlibVersion()` function


.. autodata:: __build_version__


Objects wrapping libpq structures and functions
-----------------------------------------------

The objects expose the libpq functions with minimal changes. Their names are
the libpq names in snake case, without the ``PQ`` prefix: for instance
`PGconn.exec_params()` wraps :pq:`PQexecParams()`. Strings are passed and
returned as `!bytes`, numbers and enum values as `!int`.

Unlike the libpq functions, the methods raise an exception if the libpq
returns an error code, or if the connection is closed. They also raise
`~psycopg.NotSupportedError` if the function requires a libpq version newer
than the one loaded (see `version()`).

.. autoclass:: PGconn()

    .. autoattribute:: pgconn_ptr

    .. rubric:: Connecting to the database

    .. automethod:: connect
    .. automethod:: connect_start
    .. automethod:: connect_poll
    .. automethod:: finish
    .. automethod:: reset
    .. automethod:: reset_start
    .. automethod:: reset_poll
    .. automethod:: ping
    .. autoattribute:: info

    .. rubric:: Connection status

    .. autoattribute:: status
    .. autoattribute:: transaction_status
    .. automethod:: parameter_status
    .. autoattribute:: db
    .. autoattribute:: user
    .. autoattribute:: password
    .. autoattribute:: host
    .. autoattribute:: hostaddr
    .. autoattribute:: port
    .. autoattribute:: tty
    .. autoattribute:: options
    .. autoattribute:: error_message
    .. automethod:: get_error_message
    .. autoattribute:: protocol_version
    .. autoattribute:: full_protocol_version

        .. versionadded:: 3.3

    .. autoattribute:: server_version
    .. autoattribute:: socket
    .. autoattribute:: backend_pid
    .. autoattribute:: needs_password
    .. autoattribute:: used_password
    .. autoattribute:: used_gssapi

        .. versionadded:: 3.3

    .. autoattribute:: ssl_in_use

    .. rubric:: Executing commands

    .. automethod:: exec_
    .. automethod:: exec_params
    .. automethod:: prepare
    .. automethod:: exec_prepared
    .. automethod:: describe_prepared
    .. automethod:: describe_portal
    .. automethod:: close_prepared
    .. automethod:: close_portal
    .. automethod:: make_empty_result

    .. automethod:: encrypt_password

       .. code:: python

           >>> enc = conn.info.encoding
           >>> encrypted = conn.pgconn.encrypt_password(password.encode(enc), rolename.encode(enc))
           b'SCRAM-SHA-256$4096:...

    .. automethod:: change_password

    .. rubric:: Asynchronous command processing

    .. automethod:: send_query
    .. automethod:: send_query_params
    .. automethod:: send_prepare
    .. automethod:: send_query_prepared
    .. automethod:: send_describe_prepared
    .. automethod:: send_describe_portal
    .. automethod:: send_close_prepared
    .. automethod:: send_close_portal
    .. automethod:: get_result
    .. automethod:: consume_input
    .. automethod:: is_busy
    .. autoattribute:: nonblocking
    .. automethod:: flush
    .. automethod:: set_single_row_mode
    .. automethod:: set_chunked_rows_mode

    .. rubric:: Pipeline mode

    .. autoattribute:: pipeline_status
    .. automethod:: enter_pipeline_mode
    .. automethod:: exit_pipeline_mode
    .. automethod:: pipeline_sync
    .. automethod:: send_flush_request

    .. rubric:: Canceling commands

    .. automethod:: cancel_conn
    .. automethod:: get_cancel

    .. rubric:: Notices and notifications

    .. attribute:: notice_handler
        :type: Callable[[PGresult], None] | None

        A function called when the server sends a notice or warning.

        The function receives a `PGresult` with the notice, whose fields can
        be read with `~PGresult.error_field()`.

        .. seealso:: :pq:`PQsetNoticeReceiver()`

    .. attribute:: notify_handler
        :type: Callable[[PGnotify], None] | None

        A function called when a notification is received.

        The `!PGconn` methods don't call it: Psycopg calls it for each
        notification received while it waits for the results of a command.
        Otherwise, use `notifies()` to retrieve the notifications.

    .. automethod:: notifies

    .. rubric:: COPY operations

    .. automethod:: put_copy_data
    .. automethod:: put_copy_end
    .. automethod:: get_copy_data

    .. rubric:: Tracing

    .. automethod:: trace
    .. automethod:: set_trace_flags
    .. automethod:: untrace

    .. code:: python

        >>> conn.pgconn.trace(sys.stderr.fileno())
        >>> conn.pgconn.set_trace_flags(pq.Trace.SUPPRESS_TIMESTAMPS | pq.Trace.REGRESS_MODE)
        >>> conn.execute("select now()")
        F	13	Parse	 "" "BEGIN" 0
        F	14	Bind	 "" "" 0 0 1 0
        F	6	Describe	 P ""
        F	9	Execute	 "" 0
        F	4	Sync
        B	4	ParseComplete
        B	4	BindComplete
        B	4	NoData
        B	10	CommandComplete	 "BEGIN"
        B	5	ReadyForQuery	 T
        F	17	Query	 "select now()"
        B	28	RowDescription	 1 "now" NNNN 0 NNNN 8 -1 0
        B	39	DataRow	 1 29 '2022-09-14 14:12:16.648035+02'
        B	13	CommandComplete	 "SELECT 1"
        B	5	ReadyForQuery	 T
        <psycopg.Cursor [TUPLES_OK] [INTRANS] (database=postgres) at 0x7f18a18ba040>
        >>> conn.pgconn.untrace()


.. autoclass:: PGresult()

    .. autoattribute:: pgresult_ptr
    .. automethod:: clear

    .. rubric:: Result status

    .. autoattribute:: status
    .. autoattribute:: error_message
    .. automethod:: get_error_message
    .. automethod:: error_field

    .. rubric:: Retrieving query results

    .. autoattribute:: ntuples
    .. autoattribute:: nfields
    .. automethod:: fname
    .. automethod:: ftable
    .. automethod:: ftablecol
    .. automethod:: fformat
    .. automethod:: ftype
    .. automethod:: fmod
    .. automethod:: fsize
    .. autoattribute:: binary_tuples
    .. automethod:: get_value
    .. autoattribute:: nparams
    .. automethod:: param_type

    .. rubric:: Other command results

    .. autoattribute:: command_status
    .. autoattribute:: command_tuples
    .. autoattribute:: oid_value

    .. rubric:: Building results

    .. automethod:: set_attributes


.. autoclass:: Conninfo
    :members:

.. autoclass:: Escaping
    :members:

.. autoclass:: PGcancelConn()

    .. automethod:: start
    .. automethod:: blocking
    .. automethod:: poll
    .. autoattribute:: status
    .. autoattribute:: socket
    .. autoattribute:: error_message
    .. automethod:: get_error_message
    .. automethod:: reset
    .. automethod:: finish

.. autoclass:: PGcancel()
    :members:


Data structures
---------------

.. autoclass:: PGnotify()
    :members:

.. autoclass:: ConninfoOption()
    :members:

.. autoclass:: PGresAttDesc()
    :members:


Enumerations
------------

.. autoclass:: ConnStatus
    :members:

    There are other values in this enum, but only `OK` and `BAD` are seen
    after a connection has been established. Other statuses might only be seen
    during the connection phase and are considered internal.
    `ALLOCATED` is only expected to be returned by `PGcancelConn.status`.

    .. seealso:: :pq:`PQstatus()` and :pq:`PQcancelStatus()` return this value.


.. autoclass:: PollingStatus
    :members:

    .. seealso:: :pq:`PQconnectPoll` for a description of these states.


.. autoclass:: TransactionStatus
    :members:

    .. seealso:: :pq:`PQtransactionStatus` for a description of these states.


.. autoclass:: ExecStatus
    :members:

    .. seealso:: :pq:`PQresultStatus` for a description of these states.


.. autoclass:: PipelineStatus
    :members:

    .. seealso:: :pq:`PQpipelineStatus` for a description of these states.


.. autoclass:: Format
    :members:


.. autoclass:: DiagnosticField
    :members:

    .. seealso:: :pq:`PQresultErrorField` for a description of these values,
        or the attributes of `~psycopg.errors.Diagnostic`, which expose them.


.. autoclass:: Ping
    :members:

    .. seealso:: :pq:`PQpingParams` for a description of these values.

.. autoclass:: Trace
    :members:

    .. seealso:: :pq:`PQsetTraceFlags` for a description of these values.
