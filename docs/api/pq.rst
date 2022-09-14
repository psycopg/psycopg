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


Module content
--------------

.. autodata:: __impl__

    The choice of implementation is automatic but can be forced setting the
    :envvar:`PSYCOPG_IMPL` env var.


.. autofunction:: version

    .. seealso:: the :pq:`PQlibVersion()` function


.. autodata:: __build_version__

.. autofunction:: error_message


Objects wrapping libpq structures and functions
-----------------------------------------------

.. admonition:: TODO

    finish documentation

.. autoclass:: PGconn()

    .. autoattribute:: pgconn_ptr
    .. automethod:: get_cancel
    .. autoattribute:: needs_password
    .. autoattribute:: used_password

    .. automethod:: encrypt_password

       .. code:: python

           >>> enc = conn.info.encoding
           >>> encrypted = conn.pgconn.encrypt_password(password.encode(enc), rolename.encode(enc))
           b'SCRAM-SHA-256$4096:...

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


.. autoclass:: Conninfo
.. autoclass:: Escaping

.. autoclass:: PGcancel()
    :members:


Enumerations
------------

.. autoclass:: ConnStatus
    :members:

    There are other values in this enum, but only `OK` and `BAD` are seen
    after a connection has been established. Other statuses might only be seen
    during the connection phase and are considered internal.

    .. seealso:: :pq:`PQstatus()` returns this value.


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

    Available attributes:

    .. attribute::
        SEVERITY
        SEVERITY_NONLOCALIZED
        SQLSTATE
        MESSAGE_PRIMARY
        MESSAGE_DETAIL
        MESSAGE_HINT
        STATEMENT_POSITION
        INTERNAL_POSITION
        INTERNAL_QUERY
        CONTEXT
        SCHEMA_NAME
        TABLE_NAME
        COLUMN_NAME
        DATATYPE_NAME
        CONSTRAINT_NAME
        SOURCE_FILE
        SOURCE_LINE
        SOURCE_FUNCTION

    .. seealso:: :pq:`PQresultErrorField` for a description of these values.


.. autoclass:: Ping
    :members:

    .. seealso:: :pq:`PQpingParams` for a description of these values.

.. autoclass:: Trace
    :members:

    .. seealso:: :pq:`PQsetTraceFlags` for a description of these values.
