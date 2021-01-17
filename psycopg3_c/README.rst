PostgreSQL database adapter for Python - optimisation package
=============================================================

This distribution contains the optional optimization package ``psycopg3_c``.

You shouldn't install this package directly: use instead ::

    pip install psycopg3[c]

Installing this distribution requires the ``libpq-dev`` package and other
packages normally used to build Python C extensions. If you cannot meet these
dependencies, don't worry: you don't need the package: please install the
``psycopg3`` package only.

Please read `the project readme`__ for more details.

.. __: https://github.com/psycopg/psycopg3#readme


Requirements
------------

On Linux, in order to install this package you will need:

- a C compiler,
- the libpq development packages (packages libpq-dev or postgresql-devel,
  according to your distribution)
- the `pg_config` binary in your PATH

If you don't have these prerequisites please don't try to install this
package: please install the `binary version`_.


Binary version
--------------

This library is available pre-compiled and bundled with all the required
client library as ``psycopg3_binary``. In order to use it please install::

    pip install psycopg3[binary]


Copyright (C) 2020-2021 The Psycopg Team
