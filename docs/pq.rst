.. _psycopg3.pq:

`pq` -- Libpq wrapper module
============================

.. index::
    single: libpq

.. module:: psycopg3.pq

``psycopg3`` is built around the libpq_, the PostgreSQL client library, which
performs most of the network communications and returns query results in C
structures.

.. _libpq: https://www.postgresql.org/docs/current/libpq.html

The low-level functions of the library are exposed by the objects in the
`!psycopg3.pq` module.


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
  ``pip install psycopg3[c]``.

- ``binary``: a pre-compiled C implementation, bundled with all the required
  libraries. It is the easiest option to deal with, fast to install and it
  should require no development tool or client library, however it may be not
  available for every platform. You can install it using the ``binary`` extra,
  i.e. running ``pip install psycopg3[binary]``.

.. _Cython: https://cython.org/

The implementation currently used is available in the `~psycopg3.pq.__impl__`
module constant.

At import, ``psycopg3`` will try to use the best implementation available and
will fail if none is usable. You can force the use of a specific
implementation by exporting the env var :envvar:`PSYCOPG3_IMPL`: importing the
library will fail if the requested implementation is not available.


Module content
--------------

.. autodata:: __impl__
    :annotation: str:

    The choice of implementation is automatic but can be forced setting the
    :envvar:`PSYCOPG3_IMPL` env var.


.. autofunction:: version

    .. admonition:: TODO

        Document pg10 quirk.

    .. seealso:: the :pq:`PQlibVersion()` function


.. autofunction:: error_message


Objects wrapping libpq structures and functions
-----------------------------------------------

.. admonition:: TODO

    finish documentation

.. autoclass:: PGconn()
    :members:

.. autoclass:: PGresult()
.. autoclass:: Conninfo
.. autoclass:: Escaping

.. autoclass:: PGcancel()
    :members:


Enumerations
------------

.. autoclass:: ConnStatus
    :members:

    Other possible status are only seen during the connection phase.

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
