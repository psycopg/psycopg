Psycopg 3 -- PostgreSQL database adapter for Python
===================================================

Psycopg 3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

Quick version::

    pip install --upgrade pip               # upgrade pip to at least 20.3
    pip install "psycopg[binary,pool]"      # install binary dependencies

For further information about installation please check `the documentation`__.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html


.. _Hacking:

Hacking
-------

In order to work on the Psycopg source code, you must have the
``libpq`` PostgreSQL client library installed on the system. For instance, on
Debian systems, you can obtain it by running::

    sudo apt install libpq5

On macOS, run::

    brew install libpq

On Windows you can use EnterpriseDB's `installers`__ to obtain ``libpq``
which is included in the Command Line Tools.

.. __: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

You can then clone this repository to develop Psycopg::

    git clone https://github.com/psycopg/psycopg.git
    cd psycopg

Please note that the repository contains the source code of several Python
packages: that's why you don't see a ``setup.py`` here. The packages may have
different requirements:

- The ``psycopg`` directory contains the pure python implementation of
  ``psycopg``. The package has only a runtime dependency on the ``libpq``, the
  PostgreSQL client library, which should be installed in your system.

- The ``psycopg_c`` directory contains an optimization module written in
  C/Cython. In order to build it you will need a few development tools: please
  look at `Local installation`__ in the docs for the details.

- The ``psycopg_pool`` directory contains the `connection pools`__
  implementations. This is kept as a separate package to allow a different
  release cycle.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html#local-installation
.. __: https://www.psycopg.org/psycopg3/docs/advanced/pool.html

You can create a local virtualenv and install the packages `in
development mode`__, together with their development and testing
requirements::

    python -m venv .venv
    source .venv/bin/activate
    pip install -e "./psycopg[dev,test]"    # for the base Python package
    pip install -e ./psycopg_pool           # for the connection pool
    pip install ./psycopg_c                 # for the C speedup module

.. __: https://pip.pypa.io/en/stable/topics/local-project-installs/#editable-installs

Please add ``--config-settings editable_mode=strict`` to the ``pip install
-e`` above if you experience `editable mode broken`__.

.. __: https://github.com/pypa/setuptools/issues/3557

Now hack away! You can run the tests using::

    psql -c 'create database psycopg_test'
    export PSYCOPG_TEST_DSN="dbname=psycopg_test"
    pytest


Cross-compiling
---------------

To use cross-platform zipapps created with `shiv`__ that include Psycopg
as a dependency you must also have ``libpq`` installed. See
`the section above <Hacking_>`_ for install instructions.

.. __: https://github.com/linkedin/shiv
