Psycopg 3 -- PostgreSQL database adapter for Python
===================================================

Psycopg 3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

Quick version::

    pip install --upgrade pip               # upgrade pip to at least 20.3
    pip install psycopg[binary,pool]        # install binary dependencies

For further information about installation please check `the documentation`__.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html


Hacking
-------

In order to work on the Psycopg source code you need to have the ``libpq``
PostgreSQL client library installed in the system. For instance, on Debian
systems, you can obtain it by running::

    sudo apt install libpq5

After which you can clone this repository::

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

  .. __: https://www.psycopg.org/psycopg3/docs/basic/install.html#local-installation

- The ``psycopg_pool`` directory contains the `connection pools`__
  implementations. This is kept as a separate package to allow a different
  release cycle.

  .. __: https://www.psycopg.org/psycopg3/docs/advanced/pool.html

You can create a local virtualenv and install there the packages `in
development mode`__, together with their development and testing
requirements::

    python -m venv .venv
    source .venv/bin/activate
    pip install -e ./psycopg[dev,test]      # for the base Python package
    pip install -e ./psycopg_c              # for the C extension module
    pip install -e ./psycopg_pool           # for the connection pool

.. __: https://pip.pypa.io/en/stable/reference/pip_install/#install-editable

Now hack away! You can use tox to validate the code::

    pip install tox
    tox -p4

and to run the tests::

    psql -c 'create database psycopg_test'
    export PSYCOPG_TEST_DSN="dbname=psycopg_test"
    tox -c psycopg -s
    tox -c psycopg_c -s

Please look at the commands definitions in the ``tox.ini`` files if you want
to run some of them interactively: the dependency should be already in your
virtualenv. Feel free to adapt these recipes if you follow a different
development pattern.
