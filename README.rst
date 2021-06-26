Psycopg 3 -- PostgreSQL database adapter for Python
==================================================

Psycopg 3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

The library is still in a development stage and is not available on PyPI in
the form of packages yet. You can install it directly `from the GitHub
project`__::

    $ pip install git+https://github.com/psycopg/psycopg.git#subdirectory=psycopg
    $ python3
    >>> import psycopg

.. __: https://github.com/psycopg/psycopg

You are required to have the ``libpq``, the PostgreSQL client library, already
installed in the system before using ``psycopg``. On Debian system you can
obtain it by running::

    sudo apt-get install libpq5

Please check your system's documentation for information about installing the
``libpq`` on your platform.


Hacking
-------

In order to work on the Psycopg source code you should clone this repository::

    git clone https://github.com/psycopg/psycopg.git
    cd psycopg

Please note that the repository contains the source code of several Python
packages: that's why you don't see a ``setup.py`` here. The packages may have
different requirements:

- The ``psycopg`` directory contains the pure python implementation of
  ``psycopg``. The package has only a runtime dependency on the ``libpq``,
  the PostgreSQL client library, which should have been already installed in
  your system.

- The ``psycopg_c`` directory contains an optimization module written in
  C/Cython. In order to build it you will need a few development tools: please
  look at `Local installation`__ in the docs for the details.

  .. __: https://www.psycopg.org/psycopg/docs/install.html#local-installation

You can create a local virtualenv and install there the packages `in
development mode`__, together with their development and testing
requirements::

    python -m venv .venv
    source .venv/bin/activate
    pip install -e ./psycopg[dev,test]     # for the base Python package
    pip install -e ./psycopg_c             # for the C extension module

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
