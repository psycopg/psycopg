The `!psycopg` module
=====================

Psycopg implements the `Python Database DB API 2.0 specification`__. As such
it also exposes the `module-level objects`__ required by the specifications.

.. __: https://www.python.org/dev/peps/pep-0249/
.. __: https://www.python.org/dev/peps/pep-0249/#module-interface


.. currentmodule:: psycopg

.. autofunction:: connect

   This is an alias of the class method `Connection.connect`: see its
   documentation for details.

   If you need an asynchronous connection use `AsyncConnection.connect`
   instead.


.. rubric:: Exceptions

The standard `DBAPI exceptions`__ are exposed both by the `!psycopg` module
and by the `psycopg.errors` module. The latter also exposes more specific
exceptions, mapping to the database error states (see
:ref:`sqlstate-exceptions`.

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

   The global, default adapters map establishing how Python and PostgreSQL
   types are converted into each other. This map is used as template when new
   connections are created, using `psycopg.connect()`.

   :type: `~psycopg.adapt.AdaptersMap`
