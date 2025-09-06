.. _installation:

Installation
============

In short, if you use a :ref:`supported system<supported-systems>`::

    pip install --upgrade pip           # upgrade pip to at least 20.3
    pip install "psycopg[binary]"       # remove [binary] for PyPy

and you should be :ref:`ready to start <module-usage>`. Read further for
alternative ways to install.

.. note::
   Fun fact: there is no ``psycopg3`` package, only ``psycopg``!


.. _supported-systems:

Supported systems
-----------------

The Psycopg version documented here has *official and tested* support for:

- Python: from version 3.10 to 3.14

  - Python 3.8 and 3.9 supported before Psycopg 3.3
  - Python 3.7 supported before Psycopg 3.2
  - Python 3.6 supported before Psycopg 3.1

- PyPy: from version 3.10 to 3.11

  - PyPy 3.9 supported before Psycopg 3.3
  - **Note:** Only the pure Python version is supported.

- PostgreSQL: from version 10 to 18

  - **Note:** PostgreSQL `currently supported release`__ are actively tested
    in the CI. Out-of-support releases are supported on a best-effort basis.

- OS: Linux, macOS, Windows

.. __: https://www.postgresql.org/support/versioning/

The tests to verify the supported systems run in `Github workflows`__:
anything that is not tested there is not officially supported. This includes:

.. __: https://github.com/psycopg/psycopg/actions

- Unofficial Python distributions such as Conda;
- Alternative PostgreSQL implementation;
- Other platforms such as BSD or Solaris.

If you use an unsupported system, things might work (because, for instance, the
database may use the same wire protocol as PostgreSQL) but we cannot guarantee
the correct working or a smooth ride.


.. _binary-install:

Binary installation
-------------------

The quickest way to start developing with Psycopg 3 is to install the binary
packages by running::

    pip install "psycopg[binary]"

This will install a self-contained package with all the libraries needed.
**You will need pip 20.3 at least**: please run ``pip install --upgrade pip``
to update it beforehand.

.. seealso::

    Did Psycopg 3 install ok? Great! You can now move on to the :ref:`basic
    module usage <module-usage>` to learn how it works.

    Keep on reading if the above method didn't work and you need a different
    way to install Psycopg 3.

    For further information about the differences between the packages see
    :ref:`pq-impl`.

If your platform is not supported, or if the libpq packaged is not suitable,
you should proceed to a :ref:`local installation <local-installation>` or a
:ref:`pure Python installation <pure-python-installation>`.

.. note::

    Binary packages are produced on a best-effort basis; the supported
    platforms depend on the CI runners available to build the
    packages. This means that:

    - binary packages for a new version of Python are made available once
      the runners used for the build support it. You can check the
      `psycopg-binary PyPI files`__ to verify whether your platform is
      supported;

    - the libpq version included in the binary packages depends on the version
      available on the runners. You can use the `psycopg.pq.version()`
      function and `~psycopg.pq.__build_version__` constant to infer the
      features available.

    .. __: https://pypi.org/project/psycopg-binary/#files


.. warning::

    - Starting from Psycopg 3.1.20, ARM64 macOS binary packages (i.e. for
      Apple M1 machines) are no more available for macOS versions before 14.0.
      Please upgrade your OS to at least 14.0 or use a :ref:`local
      <local-installation>` or a :ref:`Python <pure-python-installation>`
      installation.

    - The binary installation is not supported by PyPy.



.. _local-installation:

Local installation
------------------

A "Local installation" results in a performing and maintainable library. The
library will include the speed-up C module and will be linked to the system
libraries (``libpq``, ``libssl``...) so that system upgrade of libraries will
upgrade the libraries used by Psycopg 3 too. This is the preferred way to
install Psycopg for a production site.

In order to perform a local installation you need some prerequisites:

- a C compiler,
- Python development headers (e.g. the ``python3-dev`` package).
- PostgreSQL client development headers (e.g. the ``libpq-dev`` package).
- The :program:`pg_config` program available in the :envvar:`PATH`.

You **must be able** to troubleshoot an extension build, for instance you must
be able to read your compiler's error message. If you are not, please don't
try this and follow the `binary installation`_ instead.

If your build prerequisites are in place you can run::

    pip install "psycopg[c]"

.. warning::

   The local installation is not supported by PyPy.


.. _pure-python-installation:

Pure Python installation
------------------------

If you simply install::

    pip install psycopg

without ``[c]`` or ``[binary]`` extras you will obtain a pure Python
implementation. This is particularly handy to debug and hack, but it still
requires the system libpq to operate (which will be imported dynamically via
`ctypes`).

In order to use the pure Python installation you will need the ``libpq``
installed in the system: for instance on Debian system you will probably
need::

    sudo apt install libpq5

.. note::

    The ``libpq`` is the client library used by :program:`psql`, the
    PostgreSQL command line client, to connect to the database.  On most
    systems, installing :program:`psql` will install the ``libpq`` too as a
    dependency.

If you are not able to fulfill this requirement please follow the `binary
installation`_.


.. _pool-installation:

Installing the connection pool
------------------------------

The :ref:`Psycopg connection pools <connection-pools>` are distributed in a
separate package from the `!psycopg` package itself, in order to allow a
different release cycle.

In order to use the pool you must install the ``pool`` extra, using ``pip
install "psycopg[pool]"``, or install the `psycopg_pool` package separately,
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

- If your project is a final application (e.g. a service running on a server)
  you can require a specific implementation, for instance ``psycopg[c]``,
  after you have made sure that the prerequisites are met (e.g. the depending
  libraries and tools are installed in the host machine).

In both cases you can specify which version of Psycopg to use using
`requirement specifiers`__.

.. __: https://pip.pypa.io/en/stable/reference/requirement-specifiers/

If you want to make sure that a specific implementation is used you can
specify the :envvar:`PSYCOPG_IMPL` environment variable: importing the library
will fail if the implementation specified is not available. See :ref:`pq-impl`.
