.. _installation:

Installation
============

In short::

    pip install -U pip                  # upgrade pip to at least 20.3
    pip install --pre psycopg[binary]

and you should be :ref:`ready to start <module-usage>`.


.. _binary-install:

Binary installation
-------------------

The quickest way to start developing with Psycopg 3 is to install the binary
packages by running::

    pip install --pre psycopg[binary]

This will install a self-contained package with all the libraries needed.
You will need pip >= 20.3 at least: please run ``pip install -U pip`` to update
it beforehand.

The above package should work in most situations. It **will not work** in
some cases though:

- you need a glibc-based Linux distribution: the ``binary`` package doesn't
  work on Alpine Linux for instance;
- you have a newly released Python or macOS version for which binary
  packages are not ready yet.

In these case you should proceed to a :ref:`local installation
<local-installation>` or a :ref:`pure Python installation
<pure-python-installation>`.

.. seealso::

    Did Psycopg 3 install ok? Great! You can now move on to the :ref:`basic
    module usage <module-usage>` to learn how it works.

    You can come back here if you the above method didn't work and you need a
    way to install Psycopg 3 past the basic one.

    For further information about the differences between the packages see
    :ref:`pq-impl`.


.. _local-installation:

Local installation
------------------

A "Local installation" means obtaining a performing and maintainable library.
The library will include a performing C module and will be bound to the system
libraries (``libpq``, ``libssl``...) so that system upgrade of libraries will
upgrade the libraries used by Psycopg 3 too. This is the preferred way to
install Psycopg for a production site.

In order to perform a local installation you need some prerequisites:

- a C compiler,
- Python development headers (e.g. the python3-dev package).
- PostgreSQL client development headers (e.g. the libpq-dev package).
- The :program:`pg_config` program available in the :envvar:`PATH`.

You **must be able** to troubleshoot an extension build, for instance you must
be able to read your compiler's error message. If you are not, please don't
try this and follow the `binary installation`_ instead.

If your build prerequisites are in place you can run::

    pip install --pre psycopg[c]


.. _pure-python-installation:

Pure Python installation
------------------------

If you simply install::

    pip install --pre psycopg

without ``[c]`` or ``[binary]`` extras you will obtain a pure Python
implementation. This is particularly handy to debug and hack, but it still
requires the system libpq to operate (which will be imported dynamically via
`ctypes`).

In order to use the pure Python installation you will need the ``libpq``
installed in the system: for instance on Debian system you will probably
need::

    sudo apt install libpq5

If you are not able to fulfill this requirement please follow the `binary
installation`_.


.. _pool-installation:

Installing the connection pool
------------------------------

The :ref:`Psycopg connection pools <connection-pools>` are distributed in a
separate package from the `!psycopg` package itself, in order to allow a
different release cycle.

In order to use the pool you must install the ``pool`` extra, using ``pip
install psycopg[pool]``, or install the `psycopg_pool` package separately,
which would allow to specify the release to install more precisely.


Handling dependencies
---------------------

If you need to specify your project dependencies (for instance in a
``requirements.txt`` file, ``setup.py``, ``pyproject.toml`` dependencies...)
you should probably specify one of the following:

- If your project is a library, add a dependency on ``psycopg``. This will
  make sure that your library will have the ``psycopg`` package with the right
  interface and leaves the possibility of choosing a specific implementation
  to the end user of your library.

- If your project if a final application (e.g. a service running on a server)
  you can require a specific implementation, for instance ``psycopg[c]``,
  after you have made sure that the prerequisites are met (e.g. the depending
  libraries and tools are installed in the host machine).

In both cases you can specify which version of Psycopg to use using
`requirement specifiers`__.

.. __: https://pip.pypa.io/en/stable/cli/pip_install/#requirement-specifiers

If you want to make sure that a specific implementation is used you can
specify the :envvar:`PSYCOPG_IMPL` environment variable: importing the library
will fail if the implementation specified is not available. See :ref:`pq-impl`.
