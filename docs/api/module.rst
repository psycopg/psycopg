The `!psycopg` module
=====================

Psycopg implements the `Python Database DB API 2.0 specification`__. As such
it also exposes the `module-level objects`__ required by the specifications.

.. __: https://www.python.org/dev/peps/pep-0249/
.. __: https://www.python.org/dev/peps/pep-0249/#module-interface

.. module:: psycopg

.. autofunction:: connect

   This is an alias of the class method `Connection.connect`: see its
   documentation for details.

   If you need an asynchronous connection use `AsyncConnection.connect`
   instead.

.. data:: capabilities

    An object that can be used to verify that the client library used by
    psycopg implements a certain feature. For instance::

        # Fail at import time if encrypted passwords is not available
        import psycopg
        psycopg.capabilities.has_encrypt_password(check=True)

        # Verify at runtime if a feature can be used
        if psycopg.capabilities.has_hostaddr():
            print(conn.info.hostaddr)
        else:
            print("unknown connection hostadd")

    :type: `Capabilities`

    .. versionadded:: 3.2


.. rubric:: Exceptions

The standard `DBAPI exceptions`__ are exposed both by the `!psycopg` module
and by the `psycopg.errors` module. The latter also exposes more specific
exceptions, mapping to the database error states (see
:ref:`sqlstate-exceptions`).

.. __: https://www.python.org/dev/peps/pep-0249/#exceptions

.. parsed-literal::

    `!Exception`
    \|__ `Warning`
    \|__ `Error`
        \|__ `InterfaceError`
        \|__ `DatabaseError`
            \|__ `DataError`
            \|__ `OperationalError`
            \|__ `IntegrityError`
            \|__ `InternalError`
            \|__ `ProgrammingError`
            \|__ `NotSupportedError`


.. data:: adapters

   The default adapters map establishing how Python and PostgreSQL types are
   converted into each other.

   This map is used as a template when new connections are created, using
   `psycopg.connect()`. Its `~psycopg.adapt.AdaptersMap.types` attribute is a
   `~psycopg.types.TypesRegistry` containing information about every
   PostgreSQL builtin type, useful for adaptation customisation (see
   :ref:`adaptation`)::

       >>> psycopg.adapters.types["int4"]
       <TypeInfo: int4 (oid: 23, array oid: 1007)>

   :type: `~psycopg.adapt.AdaptersMap`
