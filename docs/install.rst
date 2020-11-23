.. _installation:

Installation
============

.. warning::

    `!psycopg3` is still in a development phase: packages haven't been
    released yet on PyPI.

    Please refer to `the README`__ for the current installation state, and
    please know that things may change.

    .. __: https://github.com/psycopg/psycopg3#readme

    The following is how it will be *supposed* to work, once it is released...


Quick install
-------------

The quickest way to start developing with psycopg3 is to run::

    pip install psycopg3[binary]

This will install a self-contained package with all the libraries needed.

The above package should work in most situations. It **will not work** in
some though:

- the ``binary`` package doesn't work on Alpine Linux;
- the ``binary`` package is not advised in production.


Proper installation
-------------------

Proper install means obtaining a performing and maintainable library. The
library will include a performing C module and will be bound to the system
libpq, so that system upgrade of libraries will upgrade the library used by
``psycopg3``.

In order to perform a "proper" installation you need some prerequisites:

- a C compiler,
- Python development headers (e.g. the python3-dev package).
- PostgreSQL client development headers (e.g. the libpq-dev package).
- The :program:`pg_config` program available in the :envvar:`PATH`.

You **must be able** to troubleshoot an extension build, for instance you must
be able to read your compiler's error message. If you are not, please don't
try this and follow the `quick install`_ instead.

If your build prerequisites are in place you can run::

    pip install psycopg3[c]


Pure Python installation
------------------------

If you simply install::

    pip install psycopg3

without ``[c]`` or ``[binary]`` extras you will obtain a pure Python
implementation. This is particularly handy to debug and hack, but it still
requires the system libpq to operate (which will be used dynamically via
`ctypes`).

In order to use the pure Python installation you will need the ``libpq``
installed in the system: for instance on Debian system you will probably
need::

    sudo apt-get install libpq5

If you are not able to fulfill this requirement please follow the `quick
install`_.
